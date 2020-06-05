package mem

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/bobg/bs"
)

var _ bs.Store = &Store{}

type Store struct {
	mu      sync.Mutex
	blobs   map[bs.Ref]bs.Blob
	anchors map[bs.Anchor][]bs.TimeRef
}

func New() *Store {
	return &Store{
		blobs: make(map[bs.Ref]bs.Blob),
	}
}

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

func (s *Store) GetAnchor(_ context.Context, a bs.Anchor, at time.Time) (bs.Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return bs.FindAnchor(s.anchors[a], at)
}

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

func (s *Store) PutAnchor(_ context.Context, ref bs.Ref, a bs.Anchor, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.anchors[a] = append(s.anchors[a], bs.TimeRef{T: at, R: ref})
	sort.Slice(s.anchors[a], func(i, j int) bool {
		return s.anchors[a][i].T.Before(s.anchors[a][j].T)
	})

	return nil
}
