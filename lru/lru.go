package lru

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/bobg/bs"
)

var _ bs.Store = &Store{}

type Store struct {
	c *lru.Cache // Ref->Blob
	s bs.Store
}

func New(s bs.Store, size int) (*Store, error) {
	c, err := lru.New(size)
	return &Store{s: s, c: c}, err
}

func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	if got, ok := s.c.Get(ref); ok {
		return got.(bs.Blob), nil
	}
	got, err := s.s.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	s.c.Add(ref, got)
	return got, nil
}

func (s *Store) GetMulti(ctx context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	m := make(bs.GetMultiResult)

	var misses []bs.Ref
	for _, ref := range refs {
		ref := ref
		if got, ok := s.c.Get(ref); ok {
			m[ref] = func(_ context.Context) (bs.Blob, error) { return got.(bs.Blob), nil }
		} else {
			misses = append(misses, ref)
		}
	}

	if len(misses) > 0 {
		m2, err := s.s.GetMulti(ctx, misses)
		if err != nil {
			return nil, err
		}
		for ref, fn := range m2 {
			ref, fn := ref, fn
			m[ref] = func(ctx context.Context) (bs.Blob, error) {
				b, err := fn(ctx)
				if err != nil {
					return nil, err
				}
				s.c.Add(ref, b)
				return b, nil
			}
		}
	}

	return m, nil
}

func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, at time.Time) (bs.Ref, error) {
	return s.s.GetAnchor(ctx, a, at)
}

func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	ref, added, err := s.s.Put(ctx, b)
	if err != nil {
		return ref, added, err
	}
	s.c.Add(ref, b)
	return ref, added, nil
}

func (s *Store) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	return bs.PutMulti(ctx, s, blobs)
}

func (s *Store) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, at time.Time) error {
	return s.s.PutAnchor(ctx, ref, a, at)
}
