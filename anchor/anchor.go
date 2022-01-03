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
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
	"github.com/bobg/bs/schema"
)

var (
	// ErrNoAnchorMap is the error produced by AnchorMapRef when no anchor map yet exists.
	ErrNoAnchorMap = errors.New("no anchor map")

	// ErrUpdateConflict is the error produced by UpdateAnchorMap when "optimistic locking" fails.
	ErrUpdateConflict = errors.New("update conflict")

	// ErrNotAnchorStore is an error that implementations should use
	// to indicate that a bs.Store is being used as an anchor.Store but isn't one.
	ErrNotAnchorStore = errors.New("not anchor store")
)

// An anchor map is a schema.Map where keys are anchor names and payloads are serialized schema.Lists
// (n.b. not refs to schema.Lists).
// The members of each schema.List are serialized Anchor protos
// (n.b. not refs to Anchor protos).

// Getter is a bs.Getter that can additionally get anchors.
// See Store.
type Getter interface {
	bs.Getter

	// AnchorMapRef produces the ref of the Getter's anchor map.
	// If no anchor map yet exists, this must return ErrNoAnchorMap.
	AnchorMapRef(context.Context) (bs.Ref, error)
}

// Store is a bs.Store that can additionally store anchors.
// Anchors are in a schema.Map that lives in the store.
// The store tracks the anchor map's changing ref.
type Store interface {
	Getter
	bs.Store

	// UpdateAnchorMap is used to update the anchor map in the Store.
	// Implementations must call the given UpdateFunc with the ref of the current anchor map.
	// If no anchor map yet exists, this must be the zero Ref.
	// The callback will presumably perform updates on the map, returning its new ref.
	// The implementation should store this as the new anchor map ref.
	// However, concurrent callers may make conflicting updates to the anchor map.
	// Therefore implementations are encouraged to use "optimistic locking":
	// after the callback returns, check that the anchor map still lives at the original ref and,
	// if it does, perform the update,
	// and if it doesn't, then return ErrUpdateConflict
	// (because some other caller has updated the map in the meantime).
	UpdateAnchorMap(context.Context, UpdateFunc) error
}

// UpdateFunc is the type of the callback passed to UpdateAnchorMap.
type UpdateFunc = func(bs.Ref) (bs.Ref, error)

// Get gets the latest ref for the anchor with the given name whose timestamp is not later than the given time.
// If no such anchor is found, this returns bs.ErrNotFound.
func Get(ctx context.Context, g Getter, name string, at time.Time) (bs.Ref, error) {
	ref, err := g.AnchorMapRef(ctx)
	if errors.Is(err, ErrNoAnchorMap) {
		return bs.Zero, bs.ErrNotFound
	}
	if err != nil {
		return bs.Zero, errors.Wrap(err, "getting anchor map ref")
	}

	m, err := schema.LoadMap(ctx, g, ref)
	if err != nil {
		return bs.Zero, errors.Wrap(err, "loading anchor map")
	}

	listBytes, found, err := m.Lookup(ctx, g, []byte(name))
	if err != nil {
		return bs.Zero, errors.Wrap(err, "looking up anchor")
	}
	if !found {
		return bs.Zero, bs.ErrNotFound
	}

	var list schema.List
	err = proto.Unmarshal(listBytes, &list)
	if err != nil {
		return bs.Zero, errors.Wrap(err, "unmarshaling anchor list")
	}

	for i := len(list.Members) - 1; i >= 0; i-- {
		var item Anchor
		err = proto.Unmarshal(list.Members[i], &item)
		if err != nil {
			return bs.Zero, errors.Wrap(err, "unmarshaling anchor")
		}
		itemTime := item.At.AsTime()
		if !itemTime.After(at) {
			return bs.RefFromBytes(item.Ref), nil
		}
	}

	return bs.Zero, bs.ErrNotFound
}

// Put stores a new anchor with the given name, ref, and timestamp.
// If an anchor for the given name already exists with the same ref at the same time,
// this silently does nothing.
// TODO: accept "oldest" and "limit" options here (as in Expire)?
func Put(ctx context.Context, s Store, name string, ref bs.Ref, at time.Time) error {
	return s.UpdateAnchorMap(ctx, func(mref bs.Ref) (bs.Ref, error) {
		var (
			m   *schema.Map
			err error
		)
		if mref.IsZero() {
			m = schema.NewMap()
		} else {
			m, err = schema.LoadMap(ctx, s, mref)
			if err != nil {
				return bs.Zero, errors.Wrap(err, "loading anchor map")
			}
		}

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

// Each iterates through all anchors in g in an indeterminate order,
// calling a callback for each one.
// If the callback returns an error,
// Each exits early with that error.
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
	return s.UpdateAnchorMap(ctx, func(mref bs.Ref) (bs.Ref, error) {
		if mref.IsZero() {
			return mref, nil
		}

		m, err := schema.LoadMap(ctx, s, mref)
		if err != nil {
			return bs.Zero, errors.Wrap(err, "loading anchor map")
		}

		// Get a second copy of the map to mutate during the call to m.Each.
		m2, err := schema.LoadMap(ctx, s, mref)
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

const protoTypesAnchorName = "__github.com/bobg/bs/anchor__reserved__proto_types__"

// PutProto does what bs.PutProto does,
// but additionally stores type info about the stored protobuf
// in a special entry in the anchor map.
//
// The entry,
// which is at the anchor key protoTypesAnchorName,
// is a schema.Map mapping blob refs to schema.Sets full of types.
// Each type is the ref of a protobuf "descriptor."
func PutProto(ctx context.Context, s Store, m proto.Message) (bs.Ref, bool, error) {
	now := time.Now()

	ref, added, err := bs.PutProto(ctx, s, m)
	if err != nil || !added {
		return ref, added, err
	}

	protoDescriptor := m.ProtoReflect().Descriptor()
	protoType := protodesc.ToDescriptorProto(protoDescriptor)
	protoTypeRef, _, err := bs.PutProto(ctx, s, protoType)
	if err != nil {
		return ref, added, errors.Wrap(err, "computing protobuf type")
	}

	var typesMap *schema.Map

	typesMapRef, err := Get(ctx, s, protoTypesAnchorName, now)
	if errors.Is(err, bs.ErrNotFound) {
		typesMap = schema.NewMap()
	} else if err != nil {
		return ref, added, errors.Wrap(err, "getting proto types map ref")
	} else {
		typesMap, err = schema.LoadMap(ctx, s, typesMapRef)
		if err != nil {
			return ref, added, errors.Wrap(err, "loading proto types map")
		}
	}

	// TODO: use optimistic locking here in case of concurrent updates to the same typeset.

	var typeSet *schema.Set
	typeSetRefBytes, found, err := typesMap.Lookup(ctx, s, ref[:])
	if err != nil {
		return ref, added, errors.Wrapf(err, "loading type set for proto ref %s", ref)
	}
	if found {
		typeSet, err = schema.LoadSet(ctx, s, bs.RefFromBytes(typeSetRefBytes))
		if err != nil {
			return ref, added, errors.Wrapf(err, "loading type set for proto ref %s", ref)
		}
	} else {
		typeSet = schema.NewSet()
	}

	typeSetRef, updated, err := typeSet.Add(ctx, s, protoTypeRef)
	if err != nil {
		return ref, added, errors.Wrapf(err, "adding type for proto ref %s", ref)
	}
	if !updated {
		return ref, added, nil
	}

	typesMapRef, outcome, err := typesMap.Set(ctx, s, ref[:], typeSetRef[:])
	if err != nil {
		return ref, added, errors.Wrapf(err, "updating type set for proto ref %s", ref)
	}
	if outcome == schema.ONone {
		return ref, added, nil
	}

	err = Put(ctx, s, protoTypesAnchorName, typesMapRef, now)
	return ref, added, errors.Wrap(err, "updating proto types map anchor")
}
