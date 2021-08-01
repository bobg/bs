package gc

import (
	"context"

	"github.com/bobg/bs"
)

// Store is a blob store that can delete blobs.
type Store interface {
	bs.Getter

	// Delete removes the blob with the given ref from the store.
	// It is a no-op,
	// not an error,
	// to call this on a non-existent ref.
	Delete(context.Context, bs.Ref) error
}

// Run runs a garbage collection on s,
// with k the set of refs to keep.
func Run(ctx context.Context, s Store, k Keep) error {
	return s.ListRefs(ctx, bs.Ref{}, func(ref bs.Ref) error {
		found, err := k.Contains(ctx, ref)
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		return s.Delete(ctx, ref)
	})
}
