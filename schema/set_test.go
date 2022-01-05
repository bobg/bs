package schema_test

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bobg/bs"
	. "github.com/bobg/bs/schema"
	"github.com/bobg/bs/store/mem"
)

func TestSet(t *testing.T) {
	f, err := os.Open("../testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var (
		ctx   = context.Background()
		sc    = bufio.NewScanner(f)
		store = mem.New()
		s     = NewSet()
		refs  = make(map[bs.Ref]struct{})
		sref  bs.Ref
	)

	for sc.Scan() {
		blob := bs.Blob(sc.Text())
		ref, added, err := store.Put(ctx, blob)
		if err != nil {
			t.Fatal(err)
		}
		if !added {
			continue
		}

		refs[ref] = struct{}{}
		if ref[0]&1 == 0 {
			continue
		}

		sref, added, err = s.Add(ctx, store, ref)
		if err != nil {
			t.Fatal(err)
		}
		if !added {
			t.Fatalf("expected set add of %s to be new", ref)
		}
	}
	if err = sc.Err(); err != nil {
		t.Fatal(err)
	}

	// Check that every eligible ref in refs is in the set,
	// and every ineligible ref is not.
	for ref := range refs {
		ok, err := s.Check(ctx, store, ref)
		if err != nil {
			t.Fatal(err)
		}
		if ok != (ref[0]&1 == 1) {
			t.Errorf("got Check(%s) == %v, want %v", ref, ok, !ok)
		}
	}

	// Check that every ref in the set is an eligible one in refs.
	err = s.Each(ctx, store, func(ref bs.Ref) error {
		if ref[0]&1 == 0 {
			return fmt.Errorf("ineligible ref %s in set", ref)
		}
		if _, ok := refs[ref]; !ok {
			return fmt.Errorf("did not find ref %s in set", ref)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Now delete refs and re-add them.
	// We should always get back the same shape tree.
	for i := int32(1); i < s.Node.Size && i < 128; i++ {
		var deleted []bs.Ref
		for ref := range refs {
			if ref[0]&1 == 0 {
				// This ref is not in the set.
				continue
			}

			_, removed, err := s.Remove(ctx, store, ref)
			if err != nil {
				t.Fatal(err)
			}
			if !removed {
				t.Fatalf("expected to remove %s", ref)
			}

			deleted = append(deleted, ref)
			if int32(len(deleted)) >= i {
				break
			}
		}
		// Re-add in a (probably) different order.
		sort.Slice(deleted, func(i, j int) bool { return deleted[i].Less(deleted[j]) })
		var newSref bs.Ref
		for _, ref := range deleted {
			var added bool
			newSref, added, err = s.Add(ctx, store, ref)
			if err != nil {
				t.Fatal(err)
			}
			if !added {
				t.Fatalf("expected to add %s", ref)
			}
		}
		if sref != newSref {
			t.Fatalf("after adding back %d deleted refs, set root ref %s differs from original %s", len(deleted), newSref, sref)
		}
	}
}

func TestSetFromRefs(t *testing.T) {
	ctx := context.Background()
	seed := time.Now().Unix()
	t.Logf("using seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	for i := 1; i <= 512; i++ {
		t.Run(fmt.Sprintf("%d_members", i), func(t *testing.T) {
			inp := make([]bs.Ref, 0, i)
			for j := 0; j < i; j++ {
				var ref bs.Ref
				_, err := rng.Read(ref[:])
				if err != nil {
					t.Fatal(err)
				}
				inp = append(inp, ref)
			}
			store := mem.New()
			m1 := NewSet()
			var (
				m1ref bs.Ref
				err   error
			)
			for _, ref := range inp {
				m1ref, _, err = m1.Add(ctx, store, bs.RefFromBytes(ref[:]))
				if err != nil {
					t.Fatal(err)
				}
			}

			_, m2ref, err := SetFromRefs(ctx, store, inp)
			if err != nil {
				t.Fatal(err)
			}

			if m1ref != m2ref {
				t.Error("mismatched refs")
			}
		})
	}
}

func dumpSet(ctx context.Context, s *Set, g bs.Getter, depth int) error {
	indent := strings.Repeat("  ", depth)
	fmt.Printf("%sSize: %d, Depth: %d\n", indent, s.Node.Size, s.Node.Depth)
	if s.Node.Left != nil {
		fmt.Printf("%sLeft (size %d)\n", indent, s.Node.Left.Size)
		var sub Set
		err := bs.GetProto(ctx, g, bs.RefFromBytes(s.Node.Left.Ref), &sub)
		if err != nil {
			return err
		}
		err = dumpSet(ctx, &sub, g, depth+1)
		if err != nil {
			return err
		}

		fmt.Printf("%sRight (size %d)\n", indent, s.Node.Right.Size)
		err = bs.GetProto(ctx, g, bs.RefFromBytes(s.Node.Right.Ref), &sub)
		if err != nil {
			return err
		}
		return dumpSet(ctx, &sub, g, depth+1)
	}

	for _, m := range s.Members {
		fmt.Printf("%s  %x\n", indent, m)
	}

	return nil
}
