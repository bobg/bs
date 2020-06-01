package bs

import (
	"context"
	"errors"
	"time"
)

type Getter interface {
	// Get gets a blob by its ref.
	Get(context.Context, Ref) (Blob, error)

	// GetMulti returns multiple blobs in a single call.
	GetMulti(context.Context, []Ref) (GetMultiResult, error)

	// GetAnchored gets the latest ref and blob with the given anchor
	// not later than the given timestamp.
	GetAnchored(context.Context, Anchor, time.Time) (Ref, Blob, error)
}

type Store interface {
	Getter

	// Put adds a blob to the store if it was not already present.
	// It returns the blob's ref and a boolean that is true iff the blob had to be added.
	Put(context.Context, Blob) (ref Ref, added bool, err error)

	// PutMulti adds multiple blobs to the store.
	PutMulti(context.Context, []Blob) (PutMultiResult, error)

	// PutAnchored stores a blob like Put,
	// but also indexes the blob by the given "anchor" and timestamp.
	PutAnchored(context.Context, Blob, Anchor, time.Time) (Ref, bool, error)
}

type (
	// GetMultiResult is the type of value returned by Getter.GetMulti.
	GetMultiResult map[Ref]func(context.Context) (Blob, error)

	// PutMultiResult is the type of value returned by Store.PutMulti.
	PutMultiResult []func(context.Context) (Ref, bool, error)
)

// ErrNotFound is the error returned
// when Get and GetMulti try to access a non-existent ref.
var ErrNotFound = errors.New("not found")
