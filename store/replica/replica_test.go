package replica

import (
	"context"
	"io/ioutil"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store/mem"
	"github.com/bobg/bs/testutil"
)

func TestReplicaSets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		m1 = mem.New()
		m2 = mem.New()
		s  = New(ctx, []bs.Store{m1, m2}, nil, 1)
	)

	ref1, _, err := m1.Put(ctx, bs.Bytes("foo"))
	if err != nil {
		t.Fatal(err)
	}
	ref2, _, err := m2.Put(ctx, bs.Bytes("bar"))
	if err != nil {
		t.Fatal(err)
	}
	ref3, _, err := s.Put(ctx, bs.Bytes("baz"))
	if err != nil {
		t.Fatal(err)
	}

	checkReplica(ctx, t, "m1", m1, ref1, ref3)
	checkReplica(ctx, t, "m2", m2, ref2, ref3)
	checkReplica(ctx, t, "replica", s, ref1, ref2, ref3)
}

func checkReplica(ctx context.Context, t *testing.T, name string, s bs.Store, want ...bs.Ref) {
	t.Run(name, func(t *testing.T) {
		var got []bs.Ref
		err := s.ListRefs(ctx, bs.Zero, func(r bs.Ref) error {
			got = append(got, r)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(want, func(i, j int) bool { return want[i].Less(want[j]) })
		sort.Slice(got, func(i, j int) bool { return got[i].Less(got[j]) })
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestAllRefs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.AllRefs(ctx, t, func() bs.Store {
		var (
			m1 = mem.New()
			m2 = mem.New()
		)
		return New(ctx, []bs.Store{m1, m2}, nil, 1)
	})
}

func TestReadWrite(t *testing.T) {
	data, err := ioutil.ReadFile("../../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		m1 = mem.New()
		m2 = mem.New()
		s  = New(ctx, []bs.Store{m1, m2}, nil, 1)
	)

	testutil.ReadWrite(ctx, t, s, data)
}
