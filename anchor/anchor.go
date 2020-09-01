// Package anchor defines anchor.Store,
// an extension to bs.Store that indexes "anchors,"
// which are constant lookup names for changing blobs.
package anchor

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
	"github.com/bobg/bs/proto"
	"github.com/bobg/bs/typed"
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
type Store interface {
	Getter
	typed.Store
}

// Put adds an Anchor constructed from name, ref, and at to the given Store.
func Put(ctx context.Context, s bs.Store, name string, ref bs.Ref, at time.Time) (bs.Ref, bool, error) {
	return proto.Put(ctx, s, &Anchor{Name: name, Ref: ref[:], At: timestamppb.New(at)})
}

var typeRef *bs.Ref

// TypeRef returns the type ref of an Anchor.
func TypeRef() bs.Ref {
	if typeRef == nil {
		tr, err := proto.TypeRef(&Anchor{})
		if err != nil {
			panic(err)
		}
		typeRef = &tr
	}
	return *typeRef
}

// Init initializes a Store by populating it with the type blob of an Anchor.
// It also calls proto.Init.
func Init(ctx context.Context, s Store) error {
	err := proto.Init(ctx, s)
	if err != nil {
		return errors.Wrap(err, "calling bs.Init")
	}
	t := proto.Type(&Anchor{})
	if err != nil {
		return errors.Wrap(err, "computing type of Anchor")
	}
	_, _, err = proto.Put(ctx, s, t)
	return errors.Wrap(err, "storing Anchor type blob")
}
