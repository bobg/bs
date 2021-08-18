package bs

import (
	"context"
	"errors"
)

// Getter is a read-only Store (qv).
type Getter interface {
	// Get gets a blob by its ref.
	Get(context.Context, Ref) (Blob, error)

	// ListRefs calls a function for each blob ref in the store in lexicographic order,
	// beginning with the first ref _after_ the specified one.
	//
	// The calls reflect at least the set of refs
	// known at the moment ListRefs was called.
	// It is unspecified whether later changes,
	// that happen concurrently with ListRefs,
	// are reflected.
	//
	// If the callback function returns an error,
	// ListRefs exits with that error.
	ListRefs(context.Context, Ref, func(r Ref) error) error
}

// MultiGetter is an interface that Getters may optionally implement to make the GetMulti function efficient.
type MultiGetter interface {
	GetMulti(context.Context, []Ref) (map[Ref]Blob, error)
}

// Store is a blob store.
// It stores byte sequences - "blobs" - of arbitrary length.
// Each blob can be retrieved using its "ref" as a lookup key.
// A ref is simply the SHA2-256 hash of the blob's content.
type Store interface {
	Getter

	// Put adds b to the store if it was not already present.
	// It returns b's ref and a boolean that is true iff the blob had to be added.
	Put(ctx context.Context, b Blob) (ref Ref, added bool, err error)
}

// MultiPutter is an interface that Stores may optionally implement to make the PutMulti function efficient.
type MultiPutter interface {
	PutMulti(context.Context, []Blob) (map[Ref]bool, error)
}

// DeleterStore is the type of a Store that can also delete blobs.
type DeleterStore interface {
	Store

	// Delete deletes the blob identified by the given ref from the store.
	// Implementations may choose to return nil or ErrNotFound in the case where the ref does not exist.
	//
	// TODO: What if the store is also an anchor.Store and this deletes the target of an anchor?
	// Probably should delete that anchor, too.
	Delete(context.Context, Ref) error
}

// ErrNotFound is the error returned
// when a Getter tries to access a non-existent ref.
var ErrNotFound = errors.New("not found")
