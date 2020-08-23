// Package anchor defines anchor.Store,
// an extension to bs.Store that indexes "anchors,"
// which are constant lookup names for changing blobs.
package anchor

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
)

// Getter is a read-only anchor store.
type Getter interface {
	bs.Getter

	// GetAnchor returns the latest ref associated with the given anchor
	// not later than the given time.
	GetAnchor(context.Context, string, time.Time) (bs.Ref, error)

	// ListAnchors calls a callback for all existing anchors at all existing timestamps,
	// sorted by name first and by timestamp second,
	// beginning with the first anchor after the given name string.
	// If the callback returns an error,
	// ListAnchors exits with that error.
	ListAnchors(context.Context, string, func(string, bs.Ref, time.Time) error) error
}

// Store is a blob store that can also store and retrieve anchors.
//
// Implementation note: the Put method (from bs.Store)
// must detect the case where the type ref is non-nil and equal to TypeRef().
// In that case, the implementation must unmarshal the blob as an Anchor
// in order to record it.
// If Put is called on an Anchor-typed blob that is already present in the store,
// it is free to assume that that anchor already has been recorded.
type Store interface {
	bs.Store
	Getter
}

// Put adds an Anchor constructed from name, ref, and at to the given Store.
func Put(ctx context.Context, s bs.Store, name string, ref bs.Ref, at time.Time) (bs.Ref, bool, error) {
	return bs.PutProto(ctx, s, &Anchor{Name: name, Ref: ref[:], At: timestamppb.New(at)})
}

// Check checks whether typ indicates that b is an Anchor.
// If it is, it interprets b as an Anchor and calls f with the Anchor's fields.
func Check(b bs.Blob, typ *bs.Ref, f func(name string, ref bs.Ref, at time.Time) error) error {
	if typ == nil || *typ != TypeRef() {
		return nil
	}
	var a Anchor
	err := proto.Unmarshal(b, &a)
	if err != nil {
		return errors.Wrap(err, "unmarshaling Anchor protobuf")
	}
	return f(a.Name, bs.RefFromBytes(a.Ref), a.At.AsTime())
}

var typeRef *bs.Ref

// TypeRef returns the type ref of an Anchor.
func TypeRef() bs.Ref {
	if typeRef == nil {
		tr, err := bs.TypeRef(&Anchor{})
		if err != nil {
			panic(err)
		}
		typeRef = &tr
	}
	return *typeRef
}

// Init initializes a Store by populating it with the type blob of an Anchor.
// It also calls bs.Init.
func Init(ctx context.Context, s Store) error {
	err := bs.Init(ctx, s)
	if err != nil {
		return errors.Wrap(err, "calling bs.Init")
	}
	t := bs.Type(&Anchor{})
	if err != nil {
		return errors.Wrap(err, "computing type of Anchor")
	}
	_, _, err = bs.PutProto(ctx, s, t)
	return errors.Wrap(err, "storing Anchor type blob")
}
