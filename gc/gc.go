package gc

import (
	"context"

	"github.com/bobg/bs"
)

type Store interface {
	bs.Getter
	Delete(context.Context, bs.Ref) error
}

// Run runs a garbage collection on s,
// with k the set of refs to keep.
func Run(ctx context.Context, s Store, k Keep) error {
	return s.ListRefs(ctx, bs.Ref{}, func(ref, _ bs.Ref) error {
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
