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
type Store interface {
	bs.Store
	Getter

	PutAnchor(context.Context, string, bs.Ref, time.Time) error
}
