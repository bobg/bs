package sqlite3

import (
	"context"
	"database/sql"
	stderrs "errors"
	"time"

	"github.com/bobg/sqlutil"
	_ "github.com/mattn/go-sqlite3" // register the sqlite3 type for sql.Open
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Store{}

// Store is a Sqlite-based blob store.
type Store struct {
	db *sql.DB
}

// Schema is the SQL that New executes.
// It creates the `blobs` and `anchors` tables if they do not exist.
// (If they do exist, they must have the columns, constraints, and indexing described here.)
const Schema = `
CREATE TABLE IF NOT EXISTS blobs (
  ref BLOB PRIMARY KEY NOT NULL,
  data BLOB NOT NULL,
  typ BLOB
);

CREATE TABLE IF NOT EXISTS anchors (
  name TEXT NOT NULL,
  ref BLOB NOT NULL,
  at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS anchor_idx ON anchors (name, at);
`

// New produces a new Store using `db` for storage.
// It expects to create tables `blobs` and `anchors`,
// or for those tables already to exist with the correct schema.
// (See variable Schema.)
func New(ctx context.Context, db *sql.DB) (*Store, error) {
	_, err := db.ExecContext(ctx, Schema)
	return &Store{db: db}, err
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, bs.Ref, error) {
	const q = `SELECT data, typ FROM blobs WHERE ref = $1`

	var (
		b   bs.Blob
		typ bs.Ref
	)
	err := s.db.QueryRowContext(ctx, q, ref).Scan(&b, &typ)
	if stderrs.Is(err, sql.ErrNoRows) {
		return nil, bs.Ref{}, bs.ErrNotFound
	}
	return b, typ, errors.Wrapf(err, "getting %s", ref)
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	ref := b.Ref()

	args := []interface{}{ref, b}

	var q string
	if typ == nil {
		q = `INSERT INTO blobs (ref, data) VALUES ($1, $2) ON CONFLICT DO NOTHING`
	} else {
		q = `INSERT INTO blobs (ref, data, typ) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`
		args = append(args, *typ)
	}

	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return bs.Ref{}, false, errors.Wrapf(err, "inserting %s", ref)
	}

	aff, err := res.RowsAffected()
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "counting affected rows")
	}

	if aff > 0 {
		err = anchor.Check(b, typ, func(name string, ref bs.Ref, at time.Time) error {
			const q = `INSERT INTO anchors (name, ref, at) VALUES ($1, $2, $3)`
			_, err = s.db.ExecContext(ctx, q, name, ref, at.UTC().Format(time.RFC3339Nano))
			return err
		})
		if err != nil {
			return ref, true, errors.Wrap(err, "adding anchor")
		}
	}

	return ref, aff > 0, nil
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(r, typ bs.Ref) error) error {
	const q = `SELECT ref, typ FROM blobs WHERE ref > $1 ORDER BY ref`
	rows, err := s.db.QueryContext(ctx, q, start)
	if err != nil {
		return errors.Wrap(err, "querying starting position")
	}
	defer rows.Close()

	for rows.Next() {
		var ref, typ bs.Ref

		err := rows.Scan(&ref, &typ)
		if err != nil {
			return errors.Wrap(err, "scanning query result")
		}

		err = f(ref, typ)
		if err != nil {
			return err
		}
	}
	return errors.Wrap(rows.Err(), "iterating over result rows")
}

// GetAnchor implements anchor.Store.GetAnchor.
func (s *Store) GetAnchor(ctx context.Context, name string, at time.Time) (bs.Ref, error) {
	const q = `SELECT ref FROM anchors WHERE name = $1 AND at <= $2 ORDER BY at DESC LIMIT 1`

	var result bs.Ref
	err := s.db.QueryRowContext(ctx, q, name, at.UTC().Format(time.RFC3339Nano)).Scan(&result)
	if stderrs.Is(err, sql.ErrNoRows) {
		return bs.Ref{}, bs.ErrNotFound
	}
	return result, errors.Wrapf(err, "querying anchor %s", name)
}

func (s *Store) ListAnchors(ctx context.Context, start string, f func(string, bs.Ref, time.Time) error) error {
	const q = `SELECT name, ref, at FROM anchors WHERE name > $1 ORDER BY name, at`
	return sqlutil.ForQueryRows(ctx, s.db, q, start, func(name string, ref bs.Ref, atstr string) error {
		at, err := time.Parse(time.RFC3339Nano, atstr)
		if err != nil {
			return errors.Wrapf(err, "parsing time %s", atstr)
		}
		return f(name, ref, at)
	})
}

func init() {
	store.Register("sqlite3", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
		conn, ok := conf["conn"].(string)
		if !ok {
			return nil, errors.New(`missing "conn" parameter`)
		}
		db, err := sql.Open("sqlite3", conn)
		if err != nil {
			return nil, errors.Wrap(err, "opening db")
		}
		return New(ctx, db)
	})
}
