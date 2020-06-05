package pg

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/bobg/bs"
)

var _ bs.Store = &Store{}

type Store struct {
	db *sql.DB
}

const schema = `
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

func New(ctx context.Context, db *sql.DB) (*Store, error) {
	_, err := db.ExecContext(ctx, schema)
	return &Store{db: db}, err
}

func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	const q = `SELECT data FROM blobs WHERE ref = $1`

	var result bs.Blob // xxx Scan/Value methods?
	err := s.db.QueryRowContext(ctx, q, ref).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, bs.ErrNotFound
	}
	return result, err
}

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
		result[ref] = func(_ context.Context) (bs.Blob, error) {
			<-ch
			return b, err
		}
	}
	return result, nil
}

func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, at time.Time) (bs.Ref, error) {
	const q = `SELECT ref FROM anchors WHERE anchor = $1 AND at <= $2 ORDER BY at DESC LIMIT 1`

	var result bs.Ref // xxx Scan/Value methods?
	err := s.db.QueryRowContext(ctx, q, a, at).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return bs.Zero, bs.ErrNotFound
	}
	return result, err
}

func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	const q = `INSERT INTO blobs (ref, blob) VALUES ($1, $2) ON CONFLICT DO NOTHING`

	ref := b.Ref()
	res, err := s.db.ExecContext(ctx, q, ref, b)
	if err != nil {
		return bs.Zero, false, err
	}

	aff, err := res.RowsAffected()
	return ref, aff > 0, err
}

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
			<-ch
			return ref, added, err
		}
	}
	return result, nil
}

func (s *Store) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, at time.Time) error {
	const q = `INSERT INTO anchors (anchor, at, ref) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`

	_, err := s.db.ExecContext(ctx, q, a, at, ref)
	return err
}
