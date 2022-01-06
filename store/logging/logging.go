// Package logging implements a store that delegates everything to a nested store,
// logging operations as they happen.
package logging

import (
	"context"
	"fmt"
	"log"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = (*Store)(nil)

// Store is the type of a logging wrapper around a given bs.Store.
// The nested store may optionally be an anchor.Store too.
type Store struct {
	s bs.Store
}

// New produces a new Store that embeds a given bs.Store,
// which is optionally also an anchor.Store.
func New(s bs.Store) *Store {
	return &Store{s: s}
}

// Get implements bs.Getter.Get.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	b, err := s.s.Get(ctx, ref)
	if err != nil {
		log.Printf("ERROR Get %s: %s", ref, err)
	} else {
		log.Printf("Get %s", ref)
	}
	return b, err
}

// ListRefs implements bs.Getter.ListRefs.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	log.Printf("ListRefs, start=%s", start)
	return s.s.ListRefs(ctx, start, func(ref bs.Ref) error {
		err := f(ref)
		if err != nil {
			log.Printf("  ERROR in ListRefs: %s: %s", ref, err)
		} else {
			log.Printf("  ListRefs: %s", ref)
		}
		return err
	})
}

// Put implements bs.Store.Put.
func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	ref, added, err := s.s.Put(ctx, b)
	if err != nil {
		log.Printf("ERROR in Put: %s", err)
	} else {
		log.Printf("Put %s, added=%v", ref, added)
	}
	return ref, added, err
}

func (s *Store) PutType(ctx context.Context, ref bs.Ref, typ []byte) error {
	// TODO: some logging here
	return anchor.PutType(ctx, s, ref, typ)
}

func (s *Store) GetTypes(ctx context.Context, ref bs.Ref) ([][]byte, error) {
	return anchor.GetTypes(ctx, s, ref)
}

// AnchorMapRef implements anchor.Getter.
// If the nested store is not an anchor.Store,
// this returns anchor.ErrNotAnchorStore.
func (s *Store) AnchorMapRef(ctx context.Context) (bs.Ref, error) {
	if a, ok := s.s.(anchor.Getter); ok {
		return a.AnchorMapRef(ctx)
	}
	return bs.Zero, anchor.ErrNotAnchorStore
}

// UpdateAnchorMap implements anchor.Store.
// If the nested store is not an anchor.Store,
// this returns anchor.ErrNotAnchorStore.
func (s *Store) UpdateAnchorMap(ctx context.Context, f anchor.UpdateFunc) error {
	if a, ok := s.s.(anchor.Store); ok {
		return a.UpdateAnchorMap(ctx, f)
	}
	return anchor.ErrNotAnchorStore
}

func init() {
	store.Register("logging", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
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
		if a, ok := nestedStore.(bs.Store); ok {
			return New(a), nil
		}
		return nil, fmt.Errorf("nested store is a %T and not a bs.Store", nestedStore)
	})
}
