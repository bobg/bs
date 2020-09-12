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
	"google.golang.org/protobuf/proto"

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
	a string // anchor name at which the ref map lives in the nested stored

	mu sync.Mutex  // protects m
	m  *schema.Map // maps untransformed-blob refs to serialized Pairs
}

// Transformer tells how to transform a blob on its way into and out of a Store.
// Out should be the inverse of In.
type Transformer interface {
	// In transforms a blob on its way into the store.
	In(context.Context, []byte) ([]byte, error)

	// Out transforms a blob on its way out of the store.
	Out(context.Context, []byte) ([]byte, error)
}

func New(ctx context.Context, s anchor.Store, x Transformer, a string) (*Store, error) {
	var m *schema.Map

	ref, err := s.GetAnchor(ctx, a, time.Now())
	if errors.Is(err, bs.ErrNotFound) {
		m = schema.NewMap()
	} else if err != nil {
		return nil, err
	} else {
		m, err = schema.LoadMap(ctx, s, ref)
		if err != nil {
			return nil, err
		}
	}

	return &Store{s: s, x: x, a: a, m: m}, nil
}

func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, []bs.Ref, error) {
	cref, types, err := func() (bs.Ref, []bs.Ref, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		got, ok, err := s.m.Lookup(ctx, s.s, ref[:])
		if err != nil {
			return bs.Ref{}, nil, err
		}
		if !ok {
			return bs.Ref{}, nil, bs.ErrNotFound
		}
		var pair Pair
		err = proto.Unmarshal(got, &pair)
		if err != nil {
			return bs.Ref{}, nil, errors.Wrap(err, "unmarshaling pair")
		}
		var types []bs.Ref
		for _, t := range pair.Types {
			types = append(types, bs.RefFromBytes(t))
		}
		return bs.RefFromBytes(pair.TransformedRef), types, nil
	}()
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting transformed-blob ref")
	}

	blob, _, err := s.s.Get(ctx, cref)
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting transformed blob")
	}

	if ref != cref {
		blob, err = s.x.Out(ctx, blob)
		if err != nil {
			return nil, nil, errors.Wrap(err, "untransforming blob")
		}
	}

	return blob, types, nil
}

func (s *Store) Put(ctx context.Context, blob bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	ref := blob.Ref()
	cblob, err := s.x.In(ctx, blob)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "transforming blob")
	}

	cref := bs.Blob(cblob).Ref()

	_, added, err := s.s.Put(ctx, cblob, nil)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "storing transformed blob")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	got, ok, err := s.m.Lookup(ctx, s.s, ref[:])
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "consulting ref map")
	}

	var pair Pair
	if ok {
		err = proto.Unmarshal(got, &pair)
		if err != nil {
			return bs.Ref{}, false, errors.Wrap(err, "unmarshaling pair")
		}
	} else {
		pair.TransformedRef = cref[:]
	}
	if typ != nil {
		var found bool
		for _, t := range pair.Types {
			if bytes.Equal(t, (*typ)[:]) {
				found = true
				break
			}
		}
		if !found {
			pair.Types = append(pair.Types, (*typ)[:])
		}
	}
	pairBytes, err := proto.Marshal(&pair)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "marshaling pair")
	}

	mref, _, err := s.m.Set(ctx, s.s, ref[:], pairBytes)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "updating ref map")
	}

	_, _, err = anchor.Put(ctx, s.s, s.a, mref, time.Now())
	return ref, added, errors.Wrap(err, "updating ref map anchor")
}

func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref, []bs.Ref) error) error {
	return s.s.ListRefs(ctx, start, func(ref bs.Ref, types []bs.Ref) error {
		if len(types) > 0 {
			return f(ref, types)
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		got, ok, err := s.m.Lookup(ctx, s.s, ref[:])
		if err != nil {
			return errors.Wrap(err, "consulting ref map")
		}
		if !ok {
			return nil // xxx ?
		}
		var pair Pair
		err = proto.Unmarshal(got, &pair)
		if err != nil {
			return errors.Wrap(err, "unmarshaling pair")
		}

		for _, t := range pair.Types {
			types = append(types, bs.RefFromBytes(t))
		}
		return f(ref, types)
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
