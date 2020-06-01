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
	anchors map[bs.Anchor][]timeRefPair
}

type timeRefPair struct {
	t time.Time
	r bs.Ref
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

func (s *Store) GetAnchored(_ context.Context, a bs.Anchor, at time.Time) (bs.Ref, bs.Blob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pairs := s.anchors[a]
	if len(pairs) == 0 {
		return bs.Zero, nil, bs.ErrNotFound
	}

	index := sort.Search(len(pairs), func(n int) bool {
		return !pairs[n].t.Before(at)
	})
	if index == len(pairs) {
		return bs.Zero, nil, bs.ErrNotFound
	}
	if pairs[index].t.Equal(at) {
		ref := pairs[index].r
		return ref, s.blobs[ref], nil
	}
	if index == 0 {
		return bs.Zero, nil, bs.ErrNotFound
	}
	index--
	ref := pairs[index].r
	return ref, s.blobs[ref], nil
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

func (s *Store) PutAnchored(_ context.Context, b bs.Blob, a bs.Anchor, at time.Time) (bs.Ref, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ref, added := s.put(b)

	s.anchors[a] = append(s.anchors[a], timeRefPair{t: at, r: ref})
	sort.Slice(s.anchors[a], func(i, j int) bool {
		return s.anchors[a][i].t.Before(s.anchors[a][j].t)
	})

	return ref, added, nil
}
