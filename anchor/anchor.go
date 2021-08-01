// Package anchor defines anchor.Store,
// an extension to bs.Store that indexes "anchors,"
// which are constant lookup names for changing blobs.
package anchor

import (
	"context"
	"time"

	"github.com/bobg/bs"
)

// Getter is a read-only blob and anchor store.
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
// must detect the case where the type ref is non-nil and equal to TypeRef().
// In that case, the implementation must unmarshal the blob as an Anchor
// in order to record it.
// The function Check can assist with that.
// In a call to Put,
// if both the blob and its type were already present,
// it is not necessary to check that the type is TypeRef().
// The implementation can assume the anchor has already been recorded.
type Store interface {
	bs.Store
	Getter

	PutAnchor(context.Context, string, bs.Ref, time.Time) error
}
