package testutil

import (
	"context"
	"sort"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"

	"github.com/bobg/bs"
)

// AllRefs writes a random set of random blobs to an empty store
// and makes sure that the right set of refs comes back in a call to ListRefs.
func AllRefs(ctx context.Context, t *testing.T, storeFactory func() bs.Store) {
	if err := quick.Check(allRefsHelper(ctx, t, storeFactory), nil); err != nil {
		t.Error(err)
	}
}

func allRefsHelper(ctx context.Context, t *testing.T, storeFactory func() bs.Store) func([]bs.Bytes) bool {
	return func(blobs []bs.Bytes) bool {
		var (
			store = storeFactory()
			want  []bs.Ref
		)
		for _, blob := range blobs {
			ref, added, err := store.Put(ctx, blob)
			if err != nil {
				t.Fatal(err)
			}
			if added {
				want = append(want, ref)
			}
		}
		var got []bs.Ref
		err := store.ListRefs(ctx, bs.Zero, func(r bs.Ref) error {
			got = append(got, r)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		sort.Slice(want, func(i, j int) bool { return want[i].Less(want[j]) })
		sort.Slice(got, func(i, j int) bool { return got[i].Less(got[j]) })

		if diff := cmp.Diff(want, got); diff != "" {
			t.Logf("mismatch (-want +got):\n%s", diff)
			return false
		}
		return true
	}
}
