// Package mem implements an in-memory blob store.
package mem

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

var _ bs.Store = &Store{}

// Store is a memory-based implementation of a blob store.
type Store struct {
	mu      sync.Mutex
	blobs   map[bs.Ref]bs.Blob
	anchors map[bs.Anchor][]bs.TimeRef
}

// New produces a new Store.
func New() *Store {
	return &Store{
		blobs:   make(map[bs.Ref]bs.Blob),
		anchors: make(map[bs.Anchor][]bs.TimeRef),
	}
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.Blob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.get(ref)
}

// Caller must obtain a lock.
func (s *Store) get(ref bs.Ref) (bs.Blob, error) {
	if b, ok := s.blobs[ref]; ok {
		return b, nil
	}
	return nil, bs.ErrNotFound
}

// GetMulti gets multiple blobs in one call.
func (s *Store) GetMulti(_ context.Context, refs []bs.Ref) (bs.GetMultiResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(bs.GetMultiResult)
	for _, ref := range refs {
		ref := ref
		result[ref] = func(_ context.Context) (bs.Blob, error) {
			s.mu.Lock()
			defer s.mu.Unlock()

			return s.get(ref)
		}
	}
	return result, nil
}

// GetAnchor gets the latest blob ref for a given anchor as of a given time.
func (s *Store) GetAnchor(_ context.Context, a bs.Anchor, at time.Time) (bs.Ref, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return bs.FindAnchor(s.anchors[a], at)
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(_ context.Context, b bs.Blob) (bs.Ref, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ref, added := s.put(b)
	return ref, added, nil
}

// Caller must obtain a lock.
func (s *Store) put(b bs.Blob) (bs.Ref, bool) {
	var added bool

	r := b.Ref()
	if _, ok := s.blobs[r]; !ok {
		s.blobs[r] = b
		added = true
	}

	return r, added
}

// PutMulti adds multiple blobs to the store in one call.
func (s *Store) PutMulti(_ context.Context, blobs []bs.Blob) (bs.PutMultiResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(bs.PutMultiResult, 0, len(blobs))

	for _, b := range blobs {
		ref, added := s.put(b)
		result = append(result, func(_ context.Context) (bs.Ref, bool, error) {
			return ref, added, nil
		})
	}

	return result, nil
}

// PutAnchor adds a new ref for a given anchor as of a given time.
func (s *Store) PutAnchor(_ context.Context, ref bs.Ref, a bs.Anchor, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.anchors[a] = append(s.anchors[a], bs.TimeRef{T: at, R: ref})
	sort.Slice(s.anchors[a], func(i, j int) bool {
		return s.anchors[a][i].T.Before(s.anchors[a][j].T)
	})

	return nil
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	s.mu.Lock()
	refs := make([]bs.Ref, 0, len(s.blobs))
	for ref := range s.blobs {
		refs = append(refs, ref)
	}
	s.mu.Unlock()

	sort.Slice(refs, func(i, j int) bool { return refs[i].Less(refs[j]) })
	index := sort.Search(len(refs), func(n int) bool {
		return start.Less(refs[n])
	})

	for i := index; i < len(refs); i++ {
		err := f(refs[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// ListAnchors lists all anchors in the store, in lexicographic order.
func (s *Store) ListAnchors(ctx context.Context, start bs.Anchor, f func(bs.Anchor, bs.TimeRef) error) error {
	s.mu.Lock()
	anchors := make([]bs.Anchor, 0, len(s.anchors))
	for anchor := range s.anchors {
		anchors = append(anchors, anchor)
	}
	s.mu.Unlock()

	sort.Slice(anchors, func(i, j int) bool { return anchors[i] < anchors[j] })
	index := sort.Search(len(anchors), func(n int) bool {
		return anchors[n] > start
	})

	for i := index; i < len(anchors); i++ {
		a := anchors[i]
		s.mu.Lock()
		trs := make([]bs.TimeRef, len(s.anchors[a]))
		copy(trs, s.anchors[a])
		s.mu.Unlock()
		for _, tr := range trs {
			err := f(a, tr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func init() {
	store.Register("mem", func(context.Context, map[string]interface{}) (bs.Store, error) {
		return New(), nil
	})
}
