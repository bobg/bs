// Package lru implements a blob store that acts as a least-recently-used cache for a nested blob store.
package lru

import (
	"context"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

var _ bs.Store = &Store{}

// Store implements a memory-based least-recently-used cache for a blob store.
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
	blob, err := s.s.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	s.c.Add(ref, blob)
	return blob, nil
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

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(r bs.Ref) error) error {
	return s.s.ListRefs(ctx, start, f)
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
