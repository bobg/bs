package bs

import (
	"context"
	"errors"
	"time"
)

// Getter is a read-only Store (qv).
type Getter interface {
	// Get gets a blob by its ref.
	Get(context.Context, Ref) (Blob, error)

	// GetMulti returns multiple blobs in a single call.
	GetMulti(context.Context, []Ref) (GetMultiResult, error)

	// GetAnchor gets the latest ref with the given anchor
	// not later than the given timestamp.
	GetAnchor(context.Context, Anchor, time.Time) (Ref, time.Time, error)

	// ListRefs calls a function for each blob ref in the store in lexicographic order,
	// beginning with the first ref _after_ the specified one.
	//
	// The calls reflect at least the set of refs known at the moment ListRefs was called.
	// It is unspecified whether later changes,
	// that happen concurrently with ListRefs,
	// are reflected.
	//
	// If the callback function returns an error,
	// ListRefs exits with that error.
	ListRefs(context.Context, Ref, func(Ref) error) error

	// ListAnchors calls a function for each anchor/timestamp/ref in the store,
	// beginning with the first anchor _after_ the specified one.
	// These triples are passed to the callback in ascending order,
	// sorted lexicographically by anchor name,
	// and by time within a given anchor name.
	//
	// The calls reflect at least the set of anchors known at the moment ListAnchors was called.
	// It is unspecified whether later changes,
	// that happen concurrently with ListAnchors,
	// are reflected.
	//
	// If the callback function returns an error,
	// ListAnchors exits with that error.
	ListAnchors(context.Context, Anchor, func(Anchor, time.Time, Ref) error) error
}

// Store is a blob store.
// It stores byte sequences - "blobs" - of arbitrary length.
// Each blob can be retrieved using its "ref" as a lookup key.
// A ref is simply the SHA2-256 hash of the blob's content.
type Store interface {
	Getter

	// Put adds a blob to the store if it was not already present.
	// It returns the blob's ref and a boolean that is true iff the blob had to be added.
	Put(context.Context, Blob) (ref Ref, added bool, err error)

	// PutMulti adds multiple blobs to the store.
	PutMulti(context.Context, []Blob) (PutMultiResult, error)

	// PutAnchor associates an anchor and a timestamp with a ref.
	PutAnchor(context.Context, Anchor, time.Time, Ref) error
}

type (
	// GetMultiResult is the type of value returned by Getter.GetMulti.
	GetMultiResult map[Ref]func(context.Context) (Blob, error)

	// PutMultiResult is the type of value returned by Store.PutMulti.
	PutMultiResult []func(context.Context) (Ref, bool, error)
)

// ErrNotFound is the error returned
// when a Getter tries to access a non-existent ref or anchor.
var ErrNotFound = errors.New("not found")
