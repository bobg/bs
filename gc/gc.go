// Package gc implements garbage collection for blob stores.
package gc

import (
	"context"
	"iter"

	"github.com/bobg/errors"
	"github.com/bobg/seqs"

	"github.com/bobg/bs/v2"
)

// Keep is a set of refs to protect from garbage collection.
type Keep interface {
	Add(context.Context, bs.Ref) error
	Contains(context.Context, bs.Ref) (bool, error)
}

// ProtectPair is the type of an item returned by a ProtectFunc.
// It is a ref to add to a Keep,
// plus an optional ProtectFunc for traversing the ref and finding additional refs to protect.
type ProtectPair struct {
	Ref bs.Ref
	F   ProtectFunc
}

// ProtectFunc is the type of the callback passed to Protect.
// It is responsible for traversing a given ref to other refs that must also be protected.
// It should not do this recursively:
// that's handled within Protect itself.
// (However, it safely may, as long as it does its own loop detection.
// [Assuming loops are even a thing in content-addressable storage systems, which they probably aren't.])
// It produces the refs it finds, along with their own ProtectFuncs.
type ProtectFunc = func(context.Context, bs.Getter, bs.Ref) ([]ProtectPair, error)

// Protect adds a given Ref, and all Refs reachable from it, to a Keep.
// An optional callback is responsible for traversing the given Ref
// and reporting its child Refs (if any), plus their own optional traversal functions.
func Protect(ctx context.Context, g bs.Getter, k Keep, ref bs.Ref, traverse ProtectFunc) error {
	ok, err := k.Contains(ctx, ref)
	if err != nil {
		return errors.Wrapf(err, "checking for %s", ref)
	}
	if ok {
		return nil
	}
	err = k.Add(ctx, ref)
	if err != nil {
		return errors.Wrapf(err, "adding %s", ref)
	}
	if traverse == nil {
		return nil
	}
	pairs, err := traverse(ctx, g, ref)
	if err != nil {
		return errors.Wrapf(err, "traversing %s", ref)
	}
	for _, pair := range pairs {
		err = Protect(ctx, g, k, pair.Ref, pair.F)
		if err != nil {
			return err
		}
	}
	return nil
}

// Run runs a garbage collection on store,
// deleting all refs not protected by k.
// See Protect.
func Run(ctx context.Context, store bs.DeleterStore, k Keep) error {
	var lastRef bs.Ref
	for {
		newLastRef := lastRef

		// We need a new ListRefs call after each Delete
		// because bs.Store does not guarantee that deletions don't affect the iteration in ListRefs.
		refs, errptr := store.ListRefs(ctx, lastRef)
		for ref := range refs {
			ok, err := k.Contains(ctx, ref)
			if err != nil {
				return errors.Wrapf(err, "checking ref %s", ref)
			}
			if ok {
				continue
			}
			if err := store.Delete(ctx, ref); err != nil {
				return errors.Wrapf(err, "deleting ref %s", ref)
			}
			newLastRef = ref
			break
		}
		if err := *errptr; err != nil {
			return errors.Wrap(err, "iterating over refs")
		}
		if newLastRef == lastRef {
			return nil
		}
	}
}

var _ bs.DeleterStore = (*Store)(nil)

// Store is a DeleterStore that delegates calls to a nested DeleterStore
// and can count refs and deletions during a call to Run.
type Store struct {
	S               bs.DeleterStore
	Refs, Deletions int
}

// Get implements bs.Getter.Get.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	return s.S.Get(ctx, ref)
}

// Put implements bs.Store.Put.
func (s *Store) Put(ctx context.Context, blob bs.Blob) (bs.Ref, bool, error) {
	return s.S.Put(ctx, blob)
}

// ListRefs implements bs.Getter.ListRefs.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref) (iter.Seq[bs.Ref], *error) {
	return seqs.Err(func(yield func(bs.Ref) bool) error {
		refs, errptr := s.S.ListRefs(ctx, start)
		for ref := range refs {
			s.Refs++
			if !yield(ref) {
				break
			}
		}
		return *errptr
	})
}

// Delete implements bs.DeleterStore.
func (s *Store) Delete(ctx context.Context, ref bs.Ref) error {
	s.Deletions++
	return s.S.Delete(ctx, ref)
}
