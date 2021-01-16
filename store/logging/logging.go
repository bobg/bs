// Package logging implements a store that delegates everything to a nested store,
// logging operations as they happen.
package logging

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
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

func (s *Store) GetAnchor(ctx context.Context, name string, at time.Time) (bs.Ref, error) {
	ref, err := s.s.GetAnchor(ctx, name, at)
	if err != nil {
		log.Printf("ERROR in GetAnchor(%s, %s): %s", name, at, err)
	} else {
		log.Printf("GetAnchor(%s, %s): %s", name, at, ref)
	}
	return ref, err
}

func (s *Store) ListAnchors(ctx context.Context, start string, f func(string, bs.Ref, time.Time) error) error {
	log.Printf("ListAnchors, start=%s", start)
	return s.s.ListAnchors(ctx, start, func(name string, ref bs.Ref, at time.Time) error {
		err := f(name, ref, at)
		if err != nil {
			log.Printf("  ERROR in ListAnchors at (%s, %s, %s): %s", name, at, ref, err)
		} else {
			log.Printf("  ListAnchors: (%s, %s, %s)", name, at, ref)
		}
		return err
	})
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
