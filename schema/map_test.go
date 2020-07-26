package schema

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/bobg/bs"
	"github.com/bobg/bs/mem"
)

func TestMap(t *testing.T) {
	f, err := os.Open("../testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var (
		ctx   = context.Background()
		sc    = bufio.NewScanner(f)
		store = mem.New()
		m     = &Map{Node: new(TreeNode)}
		mref  bs.Ref
		lines []string
		refs  []bs.Ref
	)

	var linenum int
	for sc.Scan() {
		text := sc.Text()
		lines = append(lines, text)

		blob := bs.Blob(sc.Text())
		ref, _, err := store.Put(ctx, blob)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)

		if linenum%2 == 0 {
			linenumstr := strconv.Itoa(linenum)

			var outcome Outcome
			mref, outcome, err = m.Set(ctx, store, []byte(linenumstr), ref)
			if err != nil {
				t.Fatal(err)
			}
			if outcome != OAdded {
				t.Fatalf("expected to add %d, outcome is %v instead", linenum, outcome)
			}
		}

		linenum++
	}
	if err = sc.Err(); err != nil {
		t.Fatal(err)
	}

	// Check that every eligible ref in refs is in the map,
	// and every ineligible ref is not.
	for i, ref := range refs {
		if i%2 != 0 {
			continue
		}
		linenumstr := strconv.Itoa(i)
		got, ok, err := m.Lookup(ctx, store, []byte(linenumstr))
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("key %d not found in map", i)
		}
		if got != refs[i] {
			t.Fatalf("got ref %s at key %d, want %s", got, i, ref)
		}
	}

	// Check that every ref in the map is an eligible one in refs.
	ch := make(chan MapPair)
	go func() {
		defer close(ch)

		for pair := range ch {
			linenum, err := strconv.Atoi(string(pair.Key))
			if err != nil {
				t.Errorf("key %s does not parse", string(pair.Key))
				continue
			}
			if linenum%2 != 0 {
				t.Errorf("key %d is not even", linenum)
				continue
			}
			if !bytes.Equal(pair.Ref, refs[linenum][:]) {
				t.Errorf("got ref %x for key %d, want %s", pair.Ref, linenum, refs[linenum])
			}
		}
	}()

	err = m.Each(ctx, store, ch)
	if err != nil {
		t.Fatal(err)
	}

	// Now delete entries and re-add them.
	// We should always get back the same shape tree.
	for i := int32(1); i < m.Node.Size && i < 128; i++ {
		var deleted []MapPair

		for linenum := 0; linenum < int(i*2); linenum += 2 {
			linenumstr := strconv.Itoa(linenum)
			_, removed, err := m.Remove(ctx, store, []byte(linenumstr))
			if err != nil {
				t.Fatal(err)
			}
			if !removed {
				t.Fatalf("expected to remove %d", linenum)
			}
			deleted = append(deleted, MapPair{
				Key: []byte(linenumstr),
				Ref: refs[linenum][:],
			})
		}

		// Re-add in a (probably) different order.
		var newMref bs.Ref
		sort.Slice(deleted, func(i, j int) bool { return bytes.Compare(deleted[i].Ref, deleted[j].Ref) < 0 })
		for _, pair := range deleted {
			newMref, _, err = m.Set(ctx, store, pair.Key, bs.RefFromBytes(pair.Ref))
			if err != nil {
				t.Fatal(err)
			}
		}

		if mref != newMref {
			t.Fatalf("after adding back %d deleted pairs, map root ref %s differs from original %s", len(deleted), newMref, mref)
		}
	}

	// Updating a key to the same value should not change the map.
	newMref, outcome, err := m.Set(ctx, store, []byte("2"), refs[2])
	if err != nil {
		t.Fatal(err)
	}
	if outcome != ONone {
		t.Fatalf("got outcome %v, want ONone", outcome)
	}
	if newMref != mref {
		t.Fatalf("no change to map but new ref %s != %s", newMref, mref)
	}

	// Updating a key to a new value should.
	newMref, outcome, err = m.Set(ctx, store, []byte("2"), refs[3])
	if err != nil {
		t.Fatal(err)
	}
	if outcome != OUpdated {
		t.Fatalf("got outcome %v, want OUpdated", outcome)
	}
	if newMref == mref {
		t.Fatal("map changed but ref is unchanged")
	}

	// Restoring the original value for the changed key should reproduce the old map.
	newMref, outcome, err = m.Set(ctx, store, []byte("2"), refs[2])
	if err != nil {
		t.Fatal(err)
	}
	if outcome != OUpdated {
		t.Fatalf("got outcome %v, want OUpdated", outcome)
	}
	if newMref != mref {
		t.Fatalf("map changed back but new ref %s != %s", newMref, mref)
	}
}

func (m *Map) dump(ctx context.Context, g bs.Getter, depth int) error {
	indent := strings.Repeat("  ", depth)
	fmt.Printf("%sSize: %d, Depth: %d\n", indent, m.Node.Size, m.Node.Depth)
	if m.Node.Left != nil {
		fmt.Printf("%sLeft (size %d)\n", indent, m.Node.Left.Size)
		var sub Map
		err := bs.GetProto(ctx, g, bs.RefFromBytes(m.Node.Left.Ref), &sub)
		if err != nil {
			return err
		}
		err = sub.dump(ctx, g, depth+1)
		if err != nil {
			return err
		}

		fmt.Printf("%sRight (size %d)\n", indent, m.Node.Right.Size)
		err = bs.GetProto(ctx, g, bs.RefFromBytes(m.Node.Right.Ref), &sub)
		if err != nil {
			return err
		}
		return sub.dump(ctx, g, depth+1)
	}

	for _, pair := range m.Members {
		fmt.Printf("%s  %x: %x\n", indent, pair.Key, pair.Ref)
	}

	return nil
}
