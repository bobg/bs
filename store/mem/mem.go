// Package mem implements an in-memory blob store.
package mem

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Store{}

// Store is a memory-based implementation of a blob store.
type (
	Store struct {
		mu      sync.Mutex
		blobs   map[bs.Ref]bs.Blob
		anchors map[string][]timeref
	}

	timeref struct {
		r bs.Ref
		t time.Time
	}
)

// New produces a new Store.
func New() *Store {
	return &Store{
		blobs:   make(map[bs.Ref]bs.Blob),
		anchors: make(map[string][]timeref),
	}
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.Blob, []bs.Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.blobs[ref]; ok {
		return p, nil
	}
	return nil, bs.ErrNotFound
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	s.mu.Lock()
	var refs []bs.Ref
	for ref := range s.blobs {
		if ref.Less(start) || ref == start {
			continue
		}
		refs = append(refs, ref)
	}
	s.mu.Unlock()

	sort.Slice(refs, func(i, j int) bool { return refs[i].Less(refs[j]) })

	for _, ref := range refs {
		err := f(ref)
		if err != nil {
			return err
		}
	}
	return nil
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(_ context.Context, b bs.Blob) (bs.Ref, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		ref   = b.Ref()
		added bool
	)
	if _, ok := s.blobs[ref]; !ok {
		s.blobs[ref] = b
		added = true
	}

	return ref, added, nil
}

// GetAnchor implements anchor.Store.GetAnchor.
func (s *Store) GetAnchor(_ context.Context, name string, at time.Time) (bs.Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	anchors := s.anchors[name]
	if len(anchors) == 0 {
		return bs.Ref{}, bs.ErrNotFound
	}
	index := sort.Search(len(anchors), func(n int) bool {
		return !anchors[n].t.Before(at)
	})
	if index < len(anchors) && anchors[index].t.Equal(at) {
		return anchors[index].r, nil
	}
	if index == 0 {
		return bs.Ref{}, bs.ErrNotFound
	}
	return anchors[index-1].r, nil
}

// ListAnchors implements anchor.Store.ListAnchors.
func (s *Store) ListAnchors(ctx context.Context, start string, f func(string, bs.Ref, time.Time) error) error {
	var names []string
	s.mu.Lock()
	for name := range s.anchors {
		if name > start {
			names = append(names, name)
		}
	}
	s.mu.Unlock()

	sort.StringSlice(names).Sort()

	for _, name := range names {
		s.mu.Lock()
		timerefs := s.anchors[name]
		s.mu.Unlock()

		for _, tr := range timerefs {
			if err := ctx.Err(); err != nil {
				return err
			}
			err := f(name, tr.r, tr.t)
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
