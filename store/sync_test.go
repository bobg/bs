package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/bobg/bs"
	. "github.com/bobg/bs/store"
	"github.com/bobg/bs/store/mem"
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

	var refs []bs.Ref
	err = stores[0].ListRefs(ctx, bs.Ref{}, func(ref bs.Ref) error {
		refs = append(refs, ref)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i < len(stores); i++ {
		s := stores[i]
		var refs2 []bs.Ref
		err = s.ListRefs(ctx, bs.Ref{}, func(ref bs.Ref) error {
			refs2 = append(refs2, ref)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(refs2, refs); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	}
}
