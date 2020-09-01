package typed

import (
	"context"

	"github.com/bobg/bs"
)

// Getter is a bs.Getter that can retrieve type information for a blob.
type Getter interface {
	bs.Getter

	// GetTyped returns the blob for the given ref (like Get does)
	// and also the list of types,
	// if any,
	// associated with the blob by earlier calls to Store.PutType.
	// The list of types is complete but its order is not specified.
	GetTyped(context.Context, bs.Ref) (bs.Blob, []bs.Ref, error)
}

// Store is a bs.Store that can store type annotations on blobs.
type Store interface {
	bs.Store
	Getter
	PutType(ctx context.Context, ref, typ bs.Ref) error
}
