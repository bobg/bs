// Package sqlite3 implements a blob store in a Sqlite3 relational database schema.
package sqlite3

import (
	"context"
	"database/sql"

	"github.com/bobg/sqlutil"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = (*Store)(nil)

// Store is a Sqlite-based blob store.
type Store struct {
	db *sql.DB
}

// Schema is the SQL that New executes.
const Schema = `
CREATE TABLE IF NOT EXISTS blobs (
  ref BLOB PRIMARY KEY NOT NULL,
  data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS anchor_map_ref (
  ref BLOB NOT NULL,
  singleton INT NOT NULL UNIQUE DEFAULT 1 CHECK (singleton = 1)
);
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
func (s *Store) Get(ctx context.Context, ref bs.Ref) ([]byte, error) {
	const q = `SELECT data FROM blobs WHERE ref = $1`

	var b []byte
	err := s.db.QueryRowContext(ctx, q, ref).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, bs.ErrNotFound
	}
	return b, errors.Wrapf(err, "querying db for ref %s", ref)
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	const q = `INSERT INTO blobs (ref, data) VALUES ($1, $2) ON CONFLICT DO NOTHING`

	ref := bs.RefOf(b.Bytes())
	res, err := s.db.ExecContext(ctx, q, ref, b.Bytes())
	if err != nil {
		return bs.Zero, false, errors.Wrap(err, "inserting blob")
	}

	aff, err := res.RowsAffected()
	if err != nil {
		return bs.Zero, false, errors.Wrap(err, "counting affected rows")
	}

	added := aff > 0

	return ref, added, nil
}

func (s *Store) PutType(ctx context.Context, ref bs.Ref, typ []byte) error {
	return anchor.PutType(ctx, s, ref, typ)
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
		return bs.Zero, anchor.ErrNoAnchorMap
	}
	return ref, err
}

// UpdateAnchorMap implements anchor.Store.
func (s *Store) UpdateAnchorMap(ctx context.Context, f anchor.UpdateFunc) error {
	oldRef, err := s.AnchorMapRef(ctx)
	if errors.Is(err, anchor.ErrNoAnchorMap) {
		oldRef = bs.Zero
	} else if err != nil {
		return errors.Wrap(err, "getting anchor map ref")
	}

	newRef, err := f(oldRef)
	if err != nil {
		return err
	}

	if oldRef.IsZero() {
		const q = `INSERT INTO anchor_map_ref (ref) VALUES ($1)`
		_, err = s.db.ExecContext(ctx, q, newRef)
		var e sqlite3.Error
		if errors.As(err, &e) && e.Code == sqlite3.ErrConstraint {
			return anchor.ErrUpdateConflict
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
