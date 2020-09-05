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
  data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS types (
  blob_ref BLOB NOT NULL,
  type_ref BLOB NOT NULL,
  PRIMARY KEY (blob_ref, type_ref)
);

CREATE INDEX IF NOT EXISTS type_idx ON types (blob_ref);

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
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, []bs.Ref, error) {
	const q = `SELECT data FROM blobs WHERE ref = $1`

	var b bs.Blob
	err := s.db.QueryRowContext(ctx, q, ref).Scan(&b)
	if stderrs.Is(err, sql.ErrNoRows) {
		return nil, nil, bs.ErrNotFound
	}

	const q2 = `SELECT type_ref FROM types WHERE blob_ref = $1`

	var types []bs.Ref
	err = sqlutil.ForQueryRows(ctx, s.db, q2, ref, func(t bs.Ref) {
		types = append(types, t)
	})

	return b, types, errors.Wrap(err, "querying types")
}

// GetAnchor implements anchor.Store.GetAnchor.
func (s *Store) GetAnchor(ctx context.Context, name string, at time.Time) (bs.Ref, error) {
	const q = `SELECT ref FROM anchors WHERE name = $1 AND at <= $2 ORDER BY at DESC LIMIT 1`

	var result bs.Ref
	err := s.db.QueryRowContext(ctx, q, name, at.UTC().Format(time.RFC3339Nano)).Scan(&result)
	if stderrs.Is(err, sql.ErrNoRows) {
		return bs.Ref{}, bs.ErrNotFound
	}
	return result, errors.Wrapf(err, "getting anchor %s", name)
}

// ListAnchors implements anchor.Getter.
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

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	const q = `INSERT INTO blobs (ref, data) VALUES ($1, $2) ON CONFLICT DO NOTHING`

	ref := b.Ref()
	res, err := s.db.ExecContext(ctx, q, ref, b)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "inserting blob")
	}

	aff, err := res.RowsAffected()
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "counting affected rows")
	}

	added := aff > 0

	if typ != nil {
		const q2 = `INSERT INTO types (blob_ref, type_ref) VALUES ($1, $2) ON CONFLICT DO NOTHING`

		res, err = s.db.ExecContext(ctx, q2, ref, *typ)
		if err != nil {
			return bs.Ref{}, false, errors.Wrap(err, "adding type info")
		}

		aff, err = res.RowsAffected()
		if err != nil {
			return bs.Ref{}, false, errors.Wrap(err, "counting affected rows")
		}
		if aff > 0 {
			err = anchor.Check(b, typ, func(name string, ref bs.Ref, at time.Time) error {
				const q = `INSERT INTO anchors (name, ref, at) VALUES ($1, $2, $3)`
				_, err := s.db.ExecContext(ctx, q, name, ref, at.UTC().Format(time.RFC3339Nano))
				return err
			})
			if err != nil {
				return bs.Ref{}, false, errors.Wrap(err, "inserting anchor")
			}
		}
	}

	return ref, added, nil
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref, []bs.Ref) error) error {
	const q = `SELECT blobs.ref, types.type_ref
		FROM blobs LEFT JOIN types ON blobs.ref = types.blob_ref
		WHERE blobs.ref > $1
		ORDER BY blobs.ref`

	var (
		lastRef *bs.Ref
		types   []bs.Ref
	)
	err := sqlutil.ForQueryRows(ctx, s.db, q, start, func(ref, typ bs.Ref) error {
		if lastRef != nil {
			if ref != *lastRef {
				err := f(*lastRef, types)
				if err != nil {
					return err
				}
				lastRef = &ref
				types = nil
			}
		} else {
			lastRef = &ref
		}
		if typ != (bs.Ref{}) {
			types = append(types, typ)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if lastRef != nil {
		err = f(*lastRef, types)
		if err != nil {
			return err
		}
	}
	return nil
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
