// Package mem implements an in-memory blob store.
package mem

import (
	"context"
	"sort"
	"sync"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

var _ bs.Store = &Store{}

// Store is a memory-based implementation of a blob store.
type (
	Store struct {
		mu     sync.Mutex
		tblobs map[bs.Ref]tblob
	}

	tblob struct {
		blob bs.Blob
		typ  bs.Ref
	}
)

// New produces a new Store.
func New() *Store {
	return &Store{tblobs: make(map[bs.Ref]tblob)}
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.Blob, bs.Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.tblobs[ref]; ok {
		return p.blob, p.typ, nil
	}
	return bs.Blob{}, bs.Ref{}, bs.ErrNotFound
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(r, typ bs.Ref) error) error {
	type tref struct {
		r, typ bs.Ref
	}

	s.mu.Lock()
	var trefs []tref
	for ref, rt := range s.tblobs {
		if ref.Less(start) || ref == start {
			continue
		}
		trefs = append(trefs, tref{r: ref, typ: rt.typ})
	}
	s.mu.Unlock()

	sort.Slice(trefs, func(i, j int) bool { return trefs[i].r.Less(trefs[j].r) })

	for _, tr := range trefs {
		err := f(tr.r, tr.typ)
		if err != nil {
			return err
		}
	}
	return nil
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(_ context.Context, b bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ref := b.Ref()
	if _, ok := s.tblobs[ref]; !ok {
		tb := tblob{blob: b}
		if typ != nil {
			tb.typ = *typ
		}
		s.tblobs[ref] = tb
		return ref, true, nil
	}
	return ref, false, nil
}

func init() {
	store.Register("mem", func(context.Context, map[string]interface{}) (bs.Store, error) {
		return New(), nil
	})
}
