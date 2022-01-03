// Package transform implements a blob store that can transform blobs into and out of a nested store.
package transform

import (
	"bytes"
	"compress/lzw"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/schema"
	"github.com/bobg/bs/store"
)

var _ bs.Store = &Store{}

// Store is a blob store wrapped a nested anchor.Store and a Transformer.
// Blobs are transformed according to the Transformer on their way in and out of the nested store.
type Store struct {
	s anchor.Store
	x Transformer
	a string // anchor name at which the ref map lives in the nested store

	mu sync.Mutex  // protects m
	m  *schema.Map // maps untransformed-blob refs to transformed-blob refs
}

// Transformer tells how to transform a blob on its way into and out of a Store.
// Out should be the inverse of In.
type Transformer interface {
	// In transforms a blob on its way into the store.
	In(context.Context, bs.Blob) (bs.Blob, error)

	// Out transforms a blob on its way out of the store.
	Out(context.Context, []byte) ([]byte, error)
}

// New produces a new Store.
// It uses s as backing storage,
// containing transformed blobs and the mapping from blob refs to transformed-blob refs.
// Blobs are transformed into and out of s using the Transformer x.
// The ref mapping is a schema.Map in s whose root ref is given by the anchor name a.
func New(ctx context.Context, s anchor.Store, x Transformer, a string) (*Store, error) {
	var m *schema.Map

	ref, err := anchor.Get(ctx, s, a, time.Now())
	if errors.Is(err, bs.ErrNotFound) {
		m = schema.NewMap()
	} else {
		if err != nil {
			return nil, err
		}
		m, err = schema.LoadMap(ctx, s, ref)
		if err != nil {
			return nil, err
		}
	}

	return &Store{s: s, x: x, a: a, m: m}, nil
}

// Get implements bs.Getter.Get.
func (s *Store) Get(ctx context.Context, ref bs.Ref) ([]byte, error) {
	cref, err := func() (bs.Ref, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		got, ok, err := s.m.Lookup(ctx, s.s, ref[:])
		if err != nil {
			return bs.Zero, err
		}
		if !ok {
			return bs.Zero, bs.ErrNotFound
		}
		return bs.RefFromBytes(got), nil
	}()
	if err != nil {
		return nil, errors.Wrap(err, "getting transformed-blob ref")
	}

	blob, err := s.s.Get(ctx, cref)
	if err != nil {
		return nil, errors.Wrap(err, "getting transformed blob")
	}

	if ref != cref {
		blob, err = s.x.Out(ctx, blob)
		if err != nil {
			return nil, errors.Wrap(err, "untransforming blob")
		}
	}

	return blob, nil
}

// Put implements bs.Store.Put.
func (s *Store) Put(ctx context.Context, blob bs.Blob) (bs.Ref, bool, error) {
	ref := bs.RefOf(blob.Bytes())
	cblob, err := s.x.In(ctx, blob)
	if err != nil {
		return bs.Zero, false, errors.Wrap(err, "transforming blob")
	}

	cref := bs.RefOf(cblob.Bytes())

	_, added, err := s.s.Put(ctx, cblob)
	if err != nil {
		return bs.Zero, false, errors.Wrap(err, "storing transformed blob")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	got, ok, err := s.m.Lookup(ctx, s.s, ref[:])
	if err != nil {
		return bs.Zero, false, errors.Wrap(err, "consulting ref map")
	}
	if ok && bytes.Equal(got, cref[:]) {
		// No need to update the map, ref already points to cref.
		return ref, false, nil
	}
	mref, _, err := s.m.Set(ctx, s.s, ref[:], cref[:])
	if err != nil {
		return bs.Zero, false, errors.Wrap(err, "updating ref map")
	}

	err = anchor.Put(ctx, s.s, s.a, mref, time.Now())
	return ref, added, errors.Wrap(err, "updating ref map anchor")
}

// ListRefs implements bs.Getter.ListRefs.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	return s.s.ListRefs(ctx, start, func(ref bs.Ref) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		got, ok, err := s.m.Lookup(ctx, s.s, ref[:])
		if err != nil {
			return errors.Wrap(err, "consulting ref map")
		}
		if !ok {
			return nil // xxx ?
		}
		return f(bs.RefFromBytes(got))
	})
}

func init() {
	store.Register("transform", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
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
		s, ok := nestedStore.(anchor.Store)
		if !ok {
			return nil, fmt.Errorf(`nested "%s" store is not an anchor.Store`, nestedType)
		}
		anchor, ok := conf["anchor"].(string)
		if !ok {
			return nil, errors.New(`missing "anchor" parameter`)
		}
		transformer, ok := conf["transformer"].(string)
		if !ok {
			return nil, errors.New(`missing "transformer" parameter`)
		}
		switch transformer {
		case "lzw":
			order := lzw.LSB
			if o, ok := conf["order"].(int); ok && lzw.Order(o) == lzw.MSB {
				order = lzw.MSB
			}
			return New(ctx, s, LZW{Order: order}, anchor)

		case "flate":
			level := -1
			if l, ok := conf["level"].(int); ok {
				level = l
			}
			return New(ctx, s, Flate{Level: level}, anchor)

		default:
			return nil, fmt.Errorf(`unknown transformer "%s"`, transformer)
		}
	})
}
