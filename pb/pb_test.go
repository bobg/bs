package pb

import (
	"bytes"
	"context"
	"os"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
	"github.com/bobg/bs/store/mem"
)

func TestTypedBlob(t *testing.T) {
	s := mem.New()

	f, err := os.Open("../testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	ref, err := split.Write(ctx, s, f, nil)
	if err != nil {
		t.Fatal(err)
	}

	var n split.Node
	err = bs.GetProto(ctx, s, ref, &n)
	if err != nil {
		t.Fatal(err)
	}

	nref, _, err := Put(ctx, s, &n)
	if err != nil {
		t.Fatal(err)
	}

	got, err := Get(ctx, s, nref)
	if err != nil {
		t.Fatal(err)
	}

	gotBytes, err := proto.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}

	wantBytes, err := proto.Marshal(&n)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(gotBytes, wantBytes) {
		t.Error("mismatch")
	}
}
