// Package mem implements an in-memory blob store.
package mem

import (
	"context"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

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
		tblobs  map[bs.Ref]tblob
		anchors map[string][]timeref
	}

	tblob struct {
		blob bs.Blob
		typ  bs.Ref
	}

	timeref struct {
		r bs.Ref
		t time.Time
	}
)

// New produces a new Store.
func New() *Store {
	return &Store{
		tblobs:  make(map[bs.Ref]tblob),
		anchors: make(map[string][]timeref),
	}
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

		if typ != nil && *typ == anchor.TypeRef() {
			var a anchor.Anchor
			err := proto.Unmarshal(b, &a)
			if err != nil {
				return bs.Ref{}, false, errors.Wrap(err, "unmarshaling Anchor protobuf")
			}

			at := a.At.AsTime()
			tr := timeref{r: bs.RefFromBytes(a.Ref), t: at}

			anchors := s.anchors[a.Name]
			anchors = append(anchors, tr)
			sort.Slice(anchors, func(i, j int) bool {
				return anchors[i].t.Before(anchors[j].t)
			})
			s.anchors[a.Name] = anchors
		}

		return ref, true, nil
	}
	return ref, false, nil
}

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

func init() {
	store.Register("mem", func(context.Context, map[string]interface{}) (bs.Store, error) {
		return New(), nil
	})
}
