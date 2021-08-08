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
	"github.com/bobg/bs/schema"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Store{}

type Store struct {
	s anchor.Store
}

func New(s anchor.Store) *Store {
	return &Store{s: s}
}

func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	b, err := s.s.Get(ctx, ref)
	if err != nil {
		log.Printf("ERROR Get %s: %s", ref, err)
	} else {
		log.Printf("Get %s", ref)
	}
	return b, err
}

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

func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	ref, added, err := s.s.Put(ctx, b)
	if err != nil {
		log.Printf("ERROR in Put: %s", err)
	} else {
		log.Printf("Put %s, added=%v", ref, added)
	}
	return ref, added, err
}

func (s *Store) AnchorMapRef(ctx context.Context) (bs.Ref, error) {
	return s.s.AnchorMapRef(ctx)
}

func (s *Store) UpdateAnchorMap(ctx context.Context, f func(bs.Ref, *schema.Map) (bs.Ref, error)) error {
	return s.s.UpdateAnchorMap(ctx, f)
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
		if a, ok := nestedStore.(anchor.Store); ok {
			return New(a), nil
		}
		return nil, fmt.Errorf("nested store is a %T and not an anchor.Store", nestedStore)
	})
}
