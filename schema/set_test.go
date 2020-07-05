package schema

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bobg/bs"
	"github.com/bobg/bs/mem"
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
		s     = new(Set)
		refs  = make(map[bs.Ref]struct{})
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
			t.Logf("not adding %s", ref)
			continue
		}

		t.Logf("adding %s", ref)
		_, added, err = s.Add(ctx, store, ref)
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

	err = s.dump(ctx, store, 0)
	if err != nil {
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
	ch := make(chan bs.Ref)
	go func() {
		defer close(ch)

		for ref := range ch {
			if ref[0]&1 == 0 {
				t.Errorf("ineligible ref %s in set", ref)
				continue
			}
			if _, ok := refs[ref]; !ok {
				t.Errorf("did not find ref %s in set", ref)
			}
		}
	}()

	err = s.Each(ctx, store, ch)
	if err != nil {
		t.Fatal(err)
	}
}

func (s *Set) dump(ctx context.Context, g bs.Getter, depth int) error {
	indent := strings.Repeat("  ", depth)
	fmt.Printf("%sSize: %d\n", indent, s.Size)
	if s.Left != nil {
		fmt.Printf("%sLeft (size %d, min %x)\n", indent, s.Left.Size, s.Left.Min)
		var sub Set
		err := bs.GetProto(ctx, g, bs.RefFromBytes(s.Left.Ref), &sub)
		if err != nil {
			return err
		}
		err = sub.dump(ctx, g, depth+1)
		if err != nil {
			return err
		}

		fmt.Printf("%sRight (size %d, min %x)\n", indent, s.Right.Size, s.Right.Min)
		err = bs.GetProto(ctx, g, bs.RefFromBytes(s.Right.Ref), &sub)
		if err != nil {
			return err
		}
		return sub.dump(ctx, g, depth+1)
	}

	for _, m := range s.Members {
		fmt.Printf("%s  %x\n", indent, m)
	}

	return nil
}
