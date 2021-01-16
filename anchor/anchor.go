// Package anchor defines anchor.Store,
// an extension to bs.Store that indexes "anchors,"
// which are constant lookup names for changing blobs.
package anchor

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
)

// Getter is a read-only anchor store.
type Getter interface {
	bs.Getter

	// GetAnchor returns the latest ref associated with the given anchor name
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
// must detect the case the blob is an Anchor
// in order to record it.
// The function Check can assist with that.
type Store interface {
	bs.Store
	Getter
}

// Put adds an Anchor constructed from name, ref, and at to the given Store.
func Put(ctx context.Context, s bs.Store, name string, ref bs.Ref, at time.Time) (bs.Ref, bool, error) {
	return bs.PutProto(ctx, s, &Anchor{Name: name, Ref: ref[:], At: timestamppb.New(at)})
}

// Check checks whether b can be interpreted as an Anchor.
// If it can, it calls f with the Anchor's fields.
func Check(b bs.Blob, f func(name string, ref bs.Ref, at time.Time) error) error {
	var a Anchor
	err := proto.Unmarshal(b, &a)
	if err != nil {
		return nil
	}
	if a.Name == "" || len(a.Ref) != len(bs.Ref{}) {
		return nil
	}
	return f(a.Name, bs.RefFromBytes(a.Ref), a.At.AsTime())
}
