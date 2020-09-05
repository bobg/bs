package bs

import (
	"context"
	"errors"
)

// Getter is a read-only Store (qv).
type Getter interface {
	// Get gets a blob by its ref.
	// Get also returns the set of types that have been associated with this blob by Put,
	// in an unspecified order but without any duplicates.
	Get(context.Context, Ref) (b Blob, types []Ref, err error)

	// ListRefs calls a function for each blob ref in the store in lexicographic order,
	// beginning with the first ref _after_ the specified one.
	// The function gets the blob's ref plus its type ref,
	// if it has one.
	// (If it doesn't, then typ is the zero ref.)
	//
	// The calls reflect at least the set of refs known at the moment ListRefs was called.
	// It is unspecified whether later changes,
	// that happen concurrently with ListRefs,
	// are reflected.
	//
	// If the callback function returns an error,
	// ListRefs exits with that error.
	ListRefs(context.Context, Ref, func(r Ref, types []Ref) error) error
}

// Store is a blob store.
// It stores byte sequences - "blobs" - of arbitrary length.
// Each blob can be retrieved using its "ref" as a lookup key.
// A ref is simply the SHA2-256 hash of the blob's content.
type Store interface {
	Getter

	// Put adds b to the store if it was not already present.
	// It returns b's ref and a boolean that is true iff the blob had to be added.
	//
	// If typ is non-nil,
	// it is associated with b as a type annotation.
	// It should be the ref of a type-describing blob,
	// such as a serialized protobuf descriptor.
	// (See PutProto.)
	// A single blob may have multiple type annotations.
	//
	// All types added for a given blob are returned by Get.
	Put(ctx context.Context, b Blob, typ *Ref) (ref Ref, added bool, err error)
}

// ErrNotFound is the error returned
// when a Getter tries to access a non-existent ref.
var ErrNotFound = errors.New("not found")

// Init initializes a store by populating it with the metatype TypeTypeBlob,
// which has itself as its type.
func Init(ctx context.Context, s Store) error {
	_, _, err := s.Put(ctx, TypeTypeBlob, &TypeTypeRef)
	return err
}
