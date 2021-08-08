// Package anchor defines anchor.Store,
// an extension to bs.Store that indexes "anchors,"
// which are constant lookup names for changing blobs.
package anchor

import (
	"bytes"
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

	// xxx require the ref passed to the callback to be the zero ref when there is no anchor map yet?
	UpdateAnchorMap(context.Context, func(bs.Ref, *schema.Map) (bs.Ref, error)) error
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
		return bs.Ref{}, bs.ErrNotFound
	}

	var list schema.List
	err = proto.Unmarshal(listBytes, &list)
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "unmarshaling anchor list")
	}

	for i := len(list.Members) - 1; i >= 0; i-- {
		var item Anchor
		err = proto.Unmarshal(list.Members[i], &item)
		if err != nil {
			return bs.Ref{}, errors.Wrap(err, "unmarshaling anchor")
		}
		itemTime := item.At.AsTime()
		if !itemTime.After(at) {
			return bs.RefFromBytes(item.Ref), nil
		}
	}

	return bs.Ref{}, bs.ErrNotFound
}

func Put(ctx context.Context, s Store, name string, ref bs.Ref, at time.Time) error {
	return s.UpdateAnchorMap(ctx, func(mref bs.Ref, m *schema.Map) (bs.Ref, error) {
		listBytes, found, err := m.Lookup(ctx, s, []byte(name))
		if err != nil {
			return mref, errors.Wrap(err, "looking up anchor")
		}

		var list schema.List
		if found {
			err = proto.Unmarshal(listBytes, &list)
			if err != nil {
				return mref, errors.Wrap(err, "unmarshaling anchor list")
			}
		}

		newAnchor := &Anchor{
			Ref: ref[:],
			At:  timestamppb.New(at),
		}

		var doSort bool

		if len(list.Members) > 0 {
			var latest Anchor
			err = proto.Unmarshal(list.Members[len(list.Members)-1], &latest)
			if err != nil {
				return mref, errors.Wrap(err, "unmarshaling previous anchor")
			}

			if latest.At.AsTime().Before(at) {
				// latest and newAnchor are in chronological order
				if bytes.Equal(latest.Ref, ref[:]) {
					// Don't add a new anchor if it's for the same ref but later.
					return mref, nil
				}
			} else {
				doSort = true
			}
		}

		newAnchorBytes, err := proto.Marshal(newAnchor)
		if err != nil {
			return mref, errors.Wrap(err, "marshaling new anchor")
		}
		list.Members = append(list.Members, newAnchorBytes)

		if doSort {
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

			if len(list.Members) > 1 {
				// Go through and make sure two adjacent anchors aren't for the same ref.
				// (The earlier one wins.)
				var a Anchor
				err = proto.Unmarshal(list.Members[0], &a)
				if err != nil {
					return mref, errors.Wrap(err, "unmarshaling anchor during deduplication")
				}
				for i := 1; i < len(list.Members); { // n.b. no i++
					var b Anchor
					err = proto.Unmarshal(list.Members[i], &b)
					if err != nil {
						return mref, errors.Wrap(err, "unmarshaling anchor during deduplication")
					}
					if bytes.Equal(a.Ref, b.Ref) {
						// Splice out list.Members[i]
						copy(list.Members[i:], list.Members[i+1:])
						list.Members[len(list.Members)-1] = nil
						list.Members = list.Members[:len(list.Members)-1]
					} else {
						a.Ref, a.At = b.Ref, b.At
						i++
					}
				}
			}
		}

		listBytes, err = proto.Marshal(&list)
		if err != nil {
			return mref, errors.Wrap(err, "marshaling anchor list")
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

// Expire expires anchors older than oldest.
// However, it never shortens an anchor's history to fewer than min items.
func Expire(ctx context.Context, s Store, oldest time.Time, min int) error {
	return s.UpdateAnchorMap(ctx, func(mref bs.Ref, m *schema.Map) (bs.Ref, error) {
		// Get a second copy of the map to mutate during the call to m.Each.
		m2, err := schema.LoadMap(ctx, s, mref) // xxx check mref is not the zero ref
		if err != nil {
			return mref, errors.Wrap(err, "loading second copy of anchor map")
		}
		m2ref := mref
		err = m.Each(ctx, s, func(pair *schema.MapPair) error {
			var list schema.List
			err := proto.Unmarshal(pair.Payload, &list)
			if err != nil {
				return errors.Wrap(err, "unmarshaling list")
			}

			var doUpdate bool
			for len(list.Members) > min {
				var a Anchor
				err = proto.Unmarshal(list.Members[0], &a)
				if err != nil {
					return errors.Wrap(err, "unmarshaling anchor")
				}
				if !a.At.AsTime().Before(oldest) {
					break
				}
				doUpdate = true
				list.Members = list.Members[1:]
			}

			if doUpdate {
				listBytes, err := proto.Marshal(&list)
				if err != nil {
					return errors.Wrap(err, "marshaling list")
				}
				m2ref, _, err = m2.Set(ctx, s, pair.Key, listBytes)
				if err != nil {
					return errors.Wrap(err, "updating second copy of anchor map")
				}
			}

			return nil
		})

		return m2ref, err
	})
}

// Sync copies every store's anchors to every other store.
// Strategy: each store does a one-way copy to its immediate neighbor to the right (mod N).
// Then each store does a one-way copy to its second neighbor to the right (mod N).
// This repeats len(stores)-1 times, at which point all stores have all anchors.
func Sync(ctx context.Context, stores []Store) error {
	for delta := 1; delta < len(stores); delta++ {
		maps := make([]*schema.Map, len(stores))
		for i, store := range stores {
			ref, err := store.AnchorMapRef(ctx)
			if errors.Is(err, ErrNoAnchorMap) {
				maps[i] = schema.NewMap()
			} else {
				if err != nil {
					return errors.Wrap(err, "getting anchor map ref")
				}
				maps[i], err = schema.LoadMap(ctx, store, ref)
				if err != nil {
					return errors.Wrap(err, "loading anchor map")
				}
			}
		}
		for i, src := range stores {
			neighbor := stores[(i+delta)%len(stores)]
			err := maps[i].Each(ctx, src, func(pair *schema.MapPair) error {
				key := string(pair.Key)
				var list schema.List
				err := proto.Unmarshal(pair.Payload, &list)
				if err != nil {
					return errors.Wrap(err, "unmarshaling anchor list")
				}
				for _, anchorBytes := range list.Members {
					var a Anchor
					err = proto.Unmarshal(anchorBytes, &a)
					if err != nil {
						return errors.Wrap(err, "unmarshaling anchor")
					}
					err = Put(ctx, neighbor, key, bs.RefFromBytes(a.Ref), a.At.AsTime())
					if err != nil {
						return errors.Wrap(err, "storing anchor")
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}
