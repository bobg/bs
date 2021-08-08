// Package pg implements a blob store in a Postgresql relational database schema.
package pg

import (
	"context"
	"database/sql"

	"github.com/bobg/sqlutil"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/schema"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = (*Store)(nil)

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

CREATE TABLE IF NOT EXISTS anchor_map_ref (
  ref BYTEA NOT NULL,
  singleton BOOLEAN NOT NULL UNIQUE DEFAULT TRUE CHECK (singleton)
);
`

// New produces a new Store using `db` for storage.
func New(ctx context.Context, db *sql.DB) (*Store, error) {
	_, err := db.ExecContext(ctx, Schema)
	return &Store{db: db}, err
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	const q = `SELECT data FROM blobs WHERE ref = $1`

	var b bs.Blob
	err := s.db.QueryRowContext(ctx, q, ref).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, bs.ErrNotFound
	}
	return b, errors.Wrapf(err, "querying db for ref %s", ref)
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
	err := sqlutil.ForQueryRows(ctx, s.db, q, start, func(ref bs.Ref) error {
		if lastRef == nil {
			lastRef = &ref
		} else {
			if ref != *lastRef {
				err := f(*lastRef)
				if err != nil {
					return err
				}
				lastRef = &ref
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if lastRef != nil {
		err = f(*lastRef)
		if err != nil {
			return err
		}
	}
	return nil
}

// AnchorMapRef implements anchor.Getter.
func (s *Store) AnchorMapRef(ctx context.Context) (bs.Ref, error) {
	const q = `SELECT ref FROM anchor_map_ref`

	var ref bs.Ref
	err := s.db.QueryRowContext(ctx, q).Scan(&ref)
	if errors.Is(err, sql.ErrNoRows) {
		return bs.Ref{}, anchor.ErrNoAnchorMap
	}
	return ref, err
}

// UpdateAnchorMap implements anchor.Store.
func (s *Store) UpdateAnchorMap(ctx context.Context, f anchor.UpdateFunc) error {
	var (
		m        *schema.Map
		wasNoMap bool
	)

	oldRef, err := s.AnchorMapRef(ctx)
	if errors.Is(err, anchor.ErrNoAnchorMap) {
		m = schema.NewMap()
		wasNoMap = true
	} else {
		if err != nil {
			return errors.Wrap(err, "getting anchor map ref")
		}
		m, err = schema.LoadMap(ctx, s, oldRef)
		if err != nil {
			return errors.Wrap(err, "loading anchor map")
		}
	}

	newRef, err := f(oldRef, m)
	if err != nil {
		return err
	}

	if wasNoMap {
		const q = `INSERT INTO anchor_map_ref (ref) VALUES ($1)`
		_, err = s.db.ExecContext(ctx, q, newRef)
		var perr *pq.Error
		if errors.As(err, &perr) {
			switch perr.Code {
			case "23505", "23514":
				return anchor.ErrUpdateConflict
			}
		}
		return err
	}

	const q = `UPDATE anchor_map_ref SET ref = $1 WHERE ref = $2`
	res, err := s.db.ExecContext(ctx, q, newRef, oldRef)
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff == 0 {
		return anchor.ErrUpdateConflict
	}
	return nil
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
