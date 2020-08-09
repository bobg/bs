package sqlite3

import (
	"context"
	"database/sql"
	stderrs "errors"

	_ "github.com/mattn/go-sqlite3" // register the sqlite3 type for sql.Open
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

var _ bs.Store = &Store{}

// Store is a Sqlite-based blob store.
type Store struct {
	db *sql.DB
}

// Schema is the SQL that New executes.
// It creates the `blobs` table if it does not exist.
// (If it does exist, it must have the columns, constraints, and indexing described here.)
const Schema = `
CREATE TABLE IF NOT EXISTS blobs (
  ref BLOB PRIMARY KEY NOT NULL,
  data BLOB NOT NULL,
  typ BLOB
);

CREATE INDEX IF NOT EXISTS typ_idx ON blobs (typ);
`

// New produces a new Store using `db` for storage.
// It expects to create the table `blobs`,
// or for that table already to exist with the correct schema.
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
	const q = `INSERT INTO blobs (ref, data, typ) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`

	var typRef bs.Ref
	if typ != nil {
		typRef = *typ
	}

	ref := b.Ref()
	res, err := s.db.ExecContext(ctx, q, ref, b, typRef)
	if err != nil {
		return bs.Ref{}, false, errors.Wrapf(err, "inserting %s", ref)
	}

	aff, err := res.RowsAffected()
	return ref, aff > 0, errors.Wrap(err, "counting affected rows")
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
