package gc_test

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/bobg/bs"
	. "github.com/bobg/bs/gc"
	"github.com/bobg/bs/split"
	"github.com/bobg/bs/store/mem"
)

type memKeep struct {
	m map[bs.Ref]struct{}
}

func (k *memKeep) Add(_ context.Context, ref bs.Ref) error {
	k.m[ref] = struct{}{}
	return nil
}

func (k *memKeep) Contains(_ context.Context, ref bs.Ref) (bool, error) {
	_, ok := k.m[ref]
	return ok, nil
}

// type recordingStore struct {
// 	m       *mem.Store
// 	written map[bs.Ref]struct{}
// }

// func (r *recordingStore) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
// 	return r.m.Get(ctx, ref)
// }

// func (r *recordingStore) Put(ctx context.Context, blob bs.Blob) (bs.Ref, bool, error) {
// 	ref, added, err := r.m.Put(ctx, blob)
// 	if err != nil {
// 		return ref, added, err
// 	}
// 	r.written[ref] = struct{}{}
// 	return ref, added, nil
// }

// func (r *recordingStore) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
// 	return r.m.ListRefs(ctx, start, f)
// }

// func (r *recordingStore) Delete(ctx context.Context, ref bs.Ref) error {
// 	return r.m.Delete(ctx, ref)
// }

func TestGC(t *testing.T) {
	store := mem.New()

	f, err := os.Open("../testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()

	w := split.NewWriter(ctx, store)
	_, err = io.Copy(w, f)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	k := &memKeep{m: make(map[bs.Ref]struct{})}
	err = Protect(ctx, store, k, w.Root, split.Protect)
	if err != nil {
		t.Fatal(err)
	}

	var want []bs.Ref
	err = store.ListRefs(ctx, bs.Ref{}, func(ref bs.Ref) error {
		want = append(want, ref)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	f, err = os.Open("../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	w = split.NewWriter(ctx, store)
	_, err = io.Copy(w, f)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = Run(ctx, store, k)
	if err != nil {
		t.Fatal(err)
	}

	var got []bs.Ref
	err = store.ListRefs(ctx, bs.Ref{}, func(ref bs.Ref) error {
		got = append(got, ref)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}
