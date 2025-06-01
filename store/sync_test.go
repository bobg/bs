package store_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/bobg/bs/v2"
	. "github.com/bobg/bs/v2/store"
	"github.com/bobg/bs/v2/store/mem"
)

func TestSync(t *testing.T) {
	const text = `abc def ghi jkl mno pqr stu`

	var (
		ctx    = context.Background()
		words  = strings.Fields(text)
		stores = make([]bs.Store, 0, len(words))
	)
	for i := range words {
		s := mem.New()
		stores = append(stores, s)
		for j, word := range words {
			if i == j {
				continue
			}

			_, _, err := s.Put(ctx, bs.Blob(word))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	err := Sync(ctx, stores)
	if err != nil {
		t.Fatal(err)
	}

	refsSeq, errptr := stores[0].ListRefs(ctx, bs.Zero)
	refs := slices.Collect(refsSeq)
	if err := *errptr; err != nil {
		t.Fatal(err)
	}

	for i := 1; i < len(stores); i++ {
		s := stores[i]
		refs2Seq, errptr := s.ListRefs(ctx, bs.Zero)
		refs2 := slices.Collect(refs2Seq)
		if err := *errptr; err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(refs2, refs); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	}
}
