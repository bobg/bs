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
type Store struct {
	mu     sync.Mutex
	tblobs map[bs.Ref]bs.TBlob
}

// New produces a new Store.
func New() *Store {
	return &Store{tblobs: make(map[bs.Ref]bs.TBlob)}
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.TBlob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.tblobs[ref]; ok {
		return b, nil
	}
	return nil, bs.ErrNotFound
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(r, typ bs.Ref) error) error {
	type pair struct {
		r, typ bs.Ref
	}

	s.mu.Lock()
	var pairs []pair
	for ref, tb := range s.tblobs {
		if ref.Less(start) || ref == start {
			continue
		}
		pairs = append(pairs, pair{r: ref, typ: tb.Type})
	}
	s.mu.Unlock()

	sort.Slice(pairs, func(i, j int) bool { return pairs[i].r.Less(pairs[j].r) })

	for _, p := range pairs {
		err := f(p.r, p.typ)
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

	ref := b.Ref()
	added := s.put(ref, b)
	return ref, added, nil
}

// Caller must obtain a lock.
func (s *Store) put(ref bs.Ref, b bs.Blob) bool {
	if _, ok := s.tblobs[r]; !ok {
		s.tblobs[ref] = bs.TBlob{Blob: b}
		return true
	}
	return false
}

func (s *Store) PutProto(_ context.Context, m proto.Message) (bs.Ref, bool, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "marshaling protobuf")
	}
	ref := b.Ref()
	added := s.put(ref, b)

	typ, err := bs.Type(m)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "marshaling protobuf descriptor")
	}
	typRef := typ.Ref()
	if ref != typRef { // prevent infinite regress when m is a descriptor proto
		s.put(typRef, typ)
	}

	return ref, added, nil
}

func init() {
	store.Register("mem", func(context.Context, map[string]interface{}) (bs.Store, error) {
		return New(), nil
	})
}
