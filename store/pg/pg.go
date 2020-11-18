// Package pg implements a blob store in a Postgresql relational database schema.
package pg

import (
	"context"
	"database/sql"
	"time"

	"github.com/bobg/sqlutil"
	_ "github.com/lib/pq" // register the postgres type for sql.Open
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Store{}

// Store is a Postgresql-based blob store.
type Store struct {
	db *sql.DB
}

// Schema is the SQL that New executes.
// It creates the `blobs` and `anchors` tables if they do not exist.
// (If they do exist, they must have the columns, constraints, and indexing described here.)
const Schema = `
CREATE TABLE IF NOT EXISTS blobs (
  ref BYTEA PRIMARY KEY NOT NULL,
  data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS anchors (
  name TEXT NOT NULL,
  ref BYTEA NOT NULL,
  at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS ON anchors (name, at);
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
	if errors.Is(err, sql.ErrNoRows) {
		return nil, bs.ErrNotFound
	}

	return b, errors.Wrap(err, "querying types")
}

// GetAnchor implements anchor.Store.GetAnchor.
func (s *Store) GetAnchor(ctx context.Context, name string, at time.Time) (bs.Ref, error) {
	const q = `SELECT ref FROM anchors WHERE name = $1 AND at <= $2 ORDER BY at DESC LIMIT 1`

	var result bs.Ref
	err := s.db.QueryRowContext(ctx, q, name, at).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return bs.Ref{}, bs.ErrNotFound
	}
	return result, errors.Wrapf(err, "getting anchor %s", name)
}

// ListAnchors implements anchor.Getter.
func (s *Store) ListAnchors(ctx context.Context, start string, f func(string, bs.Ref, time.Time) error) error {
	const q = `SELECT name, ref, at FROM anchors WHERE name > $1 ORDER BY name, at`
	return sqlutil.ForQueryRows(ctx, s.db, q, start, func(name string, ref bs.Ref, at time.Time) error {
		return f(name, ref, at)
	})
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
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

	return ref, added, nil
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	const q = `SELECT blobs.ref FROM blobs WHERE blobs.ref > $1 ORDER BY blobs.ref`

	var lastRef *bs.Ref
	return sqlutil.ForQueryRows(ctx, s.db, q, start, f)
}

func init() {
	store.Register("pg", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
		conn, ok := conf["conn"].(string)
		if !ok {
			return nil, errors.New(`missing "conn" parameter`)
		}
		db, err := sql.Open("postgres", conn)
		if err != nil {
			return nil, errors.Wrap(err, "opening db")
		}
		return New(ctx, db)
	})
}
