// Package lru implements a blob store that acts as a least-recently-used cache for a nested blob store.
package lru

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
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
func (s *Store) GetAnchor(ctx context.Context, a bs.Anchor, at time.Time) (bs.Ref, time.Time, error) {
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
func (s *Store) PutAnchor(ctx context.Context, a bs.Anchor, at time.Time, ref bs.Ref) error {
	return s.s.PutAnchor(ctx, a, at, ref)
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	return s.s.ListRefs(ctx, start, f)
}

// ListAnchors lists all anchors in the store, in lexicographic order.
func (s *Store) ListAnchors(ctx context.Context, start bs.Anchor, f func(bs.Anchor, time.Time, bs.Ref) error) error {
	return s.s.ListAnchors(ctx, start, f)
}

func init() {
	store.Register("lru", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
		size, ok := conf["size"].(int)
		if !ok {
			return nil, errors.New(`missing "size" parameter`)
		}
		nested, ok := conf["nested"].(map[string]interface{})
		if !ok {
			return nil, errors.New(`missing "nested" parameter`)
		}
		nestedType, ok := nested["type"].(string)
		if !ok {
			return nil, errors.New(`"nested" parameter missing "type"`)
		}
		nestedStore, err := store.Create(ctx, nestedType, nested)
		if err != nil {
			return nil, errors.Wrap(err, "creating nested store")
		}
		return New(nestedStore, size)
	})
}
