// Package mem implements an in-memory blob store.
package mem

import (
	"context"
	"iter"
	"maps"
	"slices"
	"sort"
	"sync"

	"github.com/bobg/seqs"

	"github.com/bobg/bs/v2"
	"github.com/bobg/bs/v2/anchor"
	"github.com/bobg/bs/v2/store"
)

var _ anchor.Store = (*Store)(nil)

// Store is a memory-based implementation of a blob store.
type Store struct {
	mu           sync.Mutex
	blobs        map[bs.Ref]bs.Blob
	anchorMapRef bs.Ref
}

// New produces a new Store.
func New() *Store {
	return &Store{blobs: make(map[bs.Ref]bs.Blob)}
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.Blob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.blobs[ref]; ok {
		return p, nil
	}
	return nil, bs.ErrNotFound
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref) (iter.Seq[bs.Ref], *error) {
	s.mu.Lock()
	refs := slices.Collect(seqs.Filter(maps.Keys(s.blobs), func(ref bs.Ref) bool {
		return start.Less(ref)
	}))
	s.mu.Unlock()

	sort.Slice(refs, func(i, j int) bool { return refs[i].Less(refs[j]) })

	f := func(yield func(bs.Ref) bool) {
		for _, ref := range refs {
			if !yield(ref) {
				return
			}
		}
	}

	var err error
	return f, &err
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

// Delete implements bs.DeleterStore.
func (s *Store) Delete(_ context.Context, ref bs.Ref) error {
	s.mu.Lock()
	delete(s.blobs, ref)
	s.mu.Unlock()
	return nil
}

// AnchorMapRef implements anchor.Getter.
func (s *Store) AnchorMapRef(_ context.Context) (bs.Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	if s.anchorMapRef.IsZero() {
		err = anchor.ErrNoAnchorMap
	}
	return s.anchorMapRef, err
}

// UpdateAnchorMap implements anchor.Store.
// It uses optimistic locking and can return anchor.ErrUpdateConflict.
func (s *Store) UpdateAnchorMap(ctx context.Context, f anchor.UpdateFunc) error {
	s.mu.Lock()
	oldRef := s.anchorMapRef
	s.mu.Unlock()

	newRef, err := f(oldRef)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.anchorMapRef != oldRef {
		return anchor.ErrUpdateConflict
	}
	s.anchorMapRef = newRef
	return nil
}

func init() {
	store.Register("mem", func(context.Context, map[string]interface{}) (bs.Store, error) {
		return New(), nil
	})
}
