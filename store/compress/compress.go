// Package compress implements a blob store that compresses and uncompresses blobs
// on their way into and out of a nested store.
package compress

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

type Store struct {
	s anchor.Store
	c Compressor
	a string // anchor name at which the ref map lives in the nested stored

	mu sync.Mutex  // protects m
	m  *schema.Map // maps uncompressed-blob refs to serialized Pairs
}

type Compressor interface {
	Compress([]byte) []byte
	Uncompress([]byte) ([]byte, error)
}

func New(ctx context.Context, s anchor.Store, c Compressor, a string) (*Store, error) {
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

	return &Store{s: s, c: c, a: a, m: m}, nil
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
		return bs.RefFromBytes(pair.CompressedRef), types, nil
	}()
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting compressed-blob ref")
	}

	blob, _, err := s.s.Get(ctx, cref)
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting compressed blob")
	}

	if ref != cref {
		blob, err = s.c.Uncompress(blob)
		if err != nil {
			return nil, nil, errors.Wrap(err, "uncompressing blob")
		}
	}

	return blob, types, nil
}

func (s *Store) Put(ctx context.Context, blob bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	ref := blob.Ref()
	cblob := bs.Blob(s.c.Compress(blob))

	cref := ref
	if len(cblob) < len(blob) {
		cref = cblob.Ref()
	} else {
		cblob = blob
	}

	_, added, err := s.s.Put(ctx, cblob, nil)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "storing compressed blob")
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
		pair.CompressedRef = cref[:]
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
	store.Register("compress", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
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
		compressor, ok := conf["compressor"].(string)
		if !ok {
			return nil, errors.New(`missing "compressor" parameter`)
		}
		switch compressor {
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
			return nil, fmt.Errorf(`unknown compressor "%s"`, compressor)
		}
	})
}
