// Package anchor defines anchor.Store,
// an extension to bs.Store that indexes "anchors,"
// which are constant lookup names for changing blobs.
package anchor

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
	"github.com/bobg/bs/schema"
)

var (
	ErrNoAnchorMap    = errors.New("no anchor map")
	ErrUpdateConflict = errors.New("update conflict")
	ErrNotAnchorStore = errors.New("not anchor store")
)

// An anchor map is a schema.Map where keys are anchor names and payloads are serialized schema.Lists
// (n.b. not refs to schema.Lists).
// The members of each schema.List are serialized Anchor protos
// (n.b. not refs to Anchor protos).

type Getter interface {
	bs.Getter

	AnchorMapRef(context.Context) (bs.Ref, error)
}

type Store interface {
	Getter
	bs.Store

	UpdateAnchorMap(context.Context, func(*schema.Map) (bs.Ref, error)) error
}

func Get(ctx context.Context, g Getter, name string, at time.Time) (bs.Ref, error) {
	ref, err := g.AnchorMapRef(ctx)
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "getting anchor map ref")
	}

	m, err := schema.LoadMap(ctx, g, ref)
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "loading anchor map")
	}

	listBytes, found, err := m.Lookup(ctx, g, []byte(name))
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "looking up anchor")
	}
	if !found {
		// xxx
	}

	var list schema.List
	err = proto.Unmarshal(listBytes, &list)
	if err != nil {
		// xxx
	}

	for i := len(list.Members) - 1; i >= 0; i-- {
		var item Anchor
		err = proto.Unmarshal(list.Members[i], &item)
		if err != nil {
			// xxx
		}
		itemTime := item.At.AsTime()
		if !itemTime.After(at) {
			return bs.RefFromBytes(item.Ref), nil
		}
	}

	return bs.Ref{}, bs.ErrNotFound
}

func Put(ctx context.Context, s Store, name string, ref bs.Ref, at time.Time) error {
	return s.UpdateAnchorMap(ctx, func(m *schema.Map) (bs.Ref, error) {
		listBytes, found, err := m.Lookup(ctx, s, []byte(name))
		if err != nil {
			// xxx
		}

		var list schema.List
		if found {
			err = proto.Unmarshal(listBytes, &list)
			if err != nil {
				// xxx
			}
		}

		newAnchor := &Anchor{
			Ref: ref[:],
			At:  timestamppb.New(at),
		}
		newAnchorBytes, err := proto.Marshal(newAnchor)
		if err != nil {
			// xxx
		}

		list.Members = append(list.Members, newAnchorBytes)
		sort.Slice(list.Members, func(i, j int) bool { // TODO: skip this if newAnchor.At > latestAnchor.At
			var a, b Anchor

			proto.Unmarshal(list.Members[i], &a)
			proto.Unmarshal(list.Members[j], &b)

			var (
				t1 = a.At.AsTime()
				t2 = b.At.AsTime()
			)

			return t1.Before(t2)
		})

		listBytes, err = proto.Marshal(&list)
		if err != nil {
			// xxx
		}

		newMapRef, _, err := m.Set(ctx, s, []byte(name), listBytes)
		return newMapRef, err
	})
}

func Each(ctx context.Context, g Getter, f func(string, bs.Ref, time.Time) error) error {
	ref, err := g.AnchorMapRef(ctx)
	if errors.Is(err, ErrNoAnchorMap) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "getting anchor map")
	}
	m, err := schema.LoadMap(ctx, g, ref)
	if err != nil {
		return errors.Wrap(err, "loading anchor map")
	}
	return m.Each(ctx, g, func(pair *schema.MapPair) error {
		key := string(pair.Key)

		var list schema.List
		err = proto.Unmarshal(pair.Payload, &list)
		if err != nil {
			return errors.Wrap(err, "unmarshaling anchor list")
		}
		for _, anchorBytes := range list.Members {
			var anchor Anchor
			err = proto.Unmarshal(anchorBytes, &anchor)
			if err != nil {
				return errors.Wrap(err, "unmarshaling anchor")
			}
			err = f(key, bs.RefFromBytes(anchor.Ref), anchor.At.AsTime())
			if err != nil {
				return err
			}
		}
		return nil
	})
}
