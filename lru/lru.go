package lru

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/bobg/bs"
)

var _ bs.Store = &Store{}

// Store implements a memory-based least-recently-used cache for a blob store.
// At present it caches only blobs, not anchors.
// Writes pass through to the underlying blob store.
type Store struct {
	c *lru.Cache // Ref->Blob
	s bs.Store
}

// New produces a new Store backed by `s` and caching up to `size` blobs.
func New(s bs.Store, size int) (*Store, error) {
	c, err := lru.New(size)
	return &Store{s: s, c: c}, err
}

// Get gets the blob with hash `ref`.
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

// GetMulti gets multiple blobs in one call.
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

// GetAnchor gets the latest blob ref for a given anchor as of a given time.
func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, at time.Time) (bs.Ref, error) {
	return s.s.GetAnchor(ctx, a, at)
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	ref, added, err := s.s.Put(ctx, b)
	if err != nil {
		return ref, added, err
	}
	s.c.Add(ref, b)
	return ref, added, nil
}

// PutMulti adds multiple blobs to the store in one call.
func (s *Store) PutMulti(ctx context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	return bs.PutMulti(ctx, s, blobs)
}

// PutAnchor adds a new ref for a given anchor as of a given time.
func (s *Store) PutAnchor(ctx context.Context, ref bs.Ref, a bs.Anchor, at time.Time) error {
	return s.s.PutAnchor(ctx, ref, a, at)
}

// ListRefs produces all blob refs in the store, in lexical order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref) (<-chan bs.Ref, func() error, error) {
	return s.s.ListRefs(ctx, start)
}

// ListAnchors lists all anchors in the store, in lexical order.
func (s *Store) ListAnchors(ctx context.Context, start bs.Anchor) (<-chan bs.Anchor, func() error, error) {
	return s.s.ListAnchors(ctx, start)
}

// ListAnchorRefs lists all blob refs for a given anchor,
// together with their timestamps,
// in chronological order.
func (s *Store) ListAnchorRefs(ctx context.Context, anchor bs.Anchor) (<-chan bs.TimeRef, func() error, error) {
	return s.s.ListAnchorRefs(ctx, anchor)
}
