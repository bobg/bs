package pg

import (
	"context"
	"database/sql"
	stderrs "errors"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

var _ bs.Store = &Store{}

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
  anchor TEXT NOT NULL,
  at TIMESTAMP WITH TIME ZONE NOT NULL,
  ref BYTEA NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ON anchors (anchor, at);
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
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	const q = `SELECT data FROM blobs WHERE ref = $1`

	var result bs.Blob // xxx Scan/Value methods?
	err := s.db.QueryRowContext(ctx, q, ref).Scan(&result)
	if stderrs.Is(err, sql.ErrNoRows) {
		return nil, bs.ErrNotFound
	}
	return result, err
}

// GetMulti gets multiple blobs in one call.
// TODO: refactor; this matches the implementation in gcs.
func (s *Store) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	result := make(bs.GetMultiResult)
	for _, ref := range refs {
		var (
			ref = ref
			ch  = make(chan struct{})
			b   []byte
			err error
		)
		go func() {
			b, err = s.Get(ctx, ref)
			close(ch)
		}()
		result[ref] = func(ctx context.Context) (bs.Blob, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ch:
				return b, err
			}
		}
	}
	return result, nil
}

// GetAnchor gets the latest blob ref for a given anchor as of a given time.
func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, at time.Time) (bs.Ref, error) {
	const q = `SELECT ref FROM anchors WHERE anchor = $1 AND at <= $2 ORDER BY at DESC LIMIT 1`

	var result bs.Ref // xxx Scan/Value methods?
	err := s.db.QueryRowContext(ctx, q, a, at).Scan(&result)
	if stderrs.Is(err, sql.ErrNoRows) {
		return bs.Ref{}, bs.ErrNotFound
	}
	return result, err
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	const q = `INSERT INTO blobs (ref, blob) VALUES ($1, $2) ON CONFLICT DO NOTHING`

	ref := b.Ref()
	res, err := s.db.ExecContext(ctx, q, ref, b)
	if err != nil {
		return bs.Ref{}, false, err
	}

	aff, err := res.RowsAffected()
	return ref, aff > 0, err
}

// PutMulti adds multiple blobs to the store in one call.
// TODO: refactor; this matches the implementation in gcs.
func (s *Store) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	result := make(bs.PutMultiResult, len(blobs))
	for i, b := range blobs {
		var (
			i     = i
			b     = b
			ch    = make(chan struct{})
			ref   bs.Ref
			added bool
			err   error
		)
		go func() {
			ref, added, err = s.Put(ctx, b)
			close(ch)
		}()
		result[i] = func(_ context.Context) (bs.Ref, bool, error) {
			select {
			case <-ctx.Done():
				return bs.Ref{}, false, ctx.Err()
			case <-ch:
				return ref, added, err
			}
		}
	}
	return result, nil
}

// PutAnchor adds a new ref for a given anchor as of a given time.
func (s *Store) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, at time.Time) error {
	const q = `INSERT INTO anchors (anchor, at, ref) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`

	_, err := s.db.ExecContext(ctx, q, a, at, ref)
	return err
}

// ListRefs produces all blob refs in the store, in lexical order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, ch chan<- bs.Ref) error {
	defer close(ch)

	const q = `SELECT ref FROM blobs WHERE ref > $1 ORDER BY ref`
	rows, err := s.db.QueryContext(ctx, q, start)
	if err != nil {
		return errors.Wrap(err, "querying starting position")
	}
	defer rows.Close()

	for rows.Next() {
		var ref bs.Ref
		err := rows.Scan(&ref)
		if err != nil {
			return errors.Wrap(err, "scanning query result")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case ch <- ref:
			// do nothing
		}
	}
	return errors.Wrap(rows.Err(), "iterating over result rows")
}

// ListAnchors lists all anchors in the store, in lexical order.
func (s *Store) ListAnchors(ctx context.Context, start bs.Anchor, ch chan<- bs.Anchor) error {
	defer close(ch)

	const q = `SELECT DISTINCT(anchor) FROM anchors WHERE anchor > $1 ORDER BY anchor`
	rows, err := s.db.QueryContext(ctx, q, start)
	if err != nil {
		return errors.Wrap(err, "querying starting position")
	}
	defer rows.Close()

	for rows.Next() {
		var anchor bs.Anchor
		err := rows.Scan(&anchor)
		if err != nil {
			return errors.Wrap(err, "scanning query result")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case ch <- anchor:
			// do nothing
		}
	}
	return errors.Wrap(rows.Err(), "iterating over result rows")
}

// ListAnchorRefs lists all blob refs for a given anchor,
// together with their timestamps,
// in chronological order.
func (s *Store) ListAnchorRefs(ctx context.Context, a bs.Anchor, ch chan<- bs.TimeRef) error {
	defer close(ch)

	const q = `SELECT at, ref FROM anchors WHERE anchor = $1 ORDER BY at`
	rows, err := s.db.QueryContext(ctx, q, a)
	if err != nil {
		return errors.Wrapf(err, "querying anchor %s", a)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			t   time.Time
			ref bs.Ref
		)
		err = rows.Scan(&t, &ref)
		if err != nil {
			return errors.Wrap(err, "scanning query result")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case ch <- bs.TimeRef{T: t, R: ref}:
			// do nothing
		}
	}
	return errors.Wrap(rows.Err(), "iterating over result rows")
}

func init() {
	store.Register("pg", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
		conn, ok := conf["conn"].(string)
		if !ok {
			// xxx
		}
		db, err := sql.Open("postgres", conn)
		if err != nil {
			// xxx
		}
		return New(ctx, db)
	})
}
