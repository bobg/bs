package rpc

import (
	context "context"
	"io/ioutil"
	"net"
	"testing"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/bobg/bs/store/mem"
	"github.com/bobg/bs/testutil"
)

func TestRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcSrv := grpc.NewServer()
	srv := NewServer(mem.New())
	RegisterStoreServer(grpcSrv, srv)
	defer grpcSrv.GracefulStop()

	l := bufconn.Listen(4096)

	go grpcSrv.Serve(l)

	options := []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return l.Dial()
		}),
		grpc.WithInsecure(),
	}

	cc, err := grpc.DialContext(ctx, "bufnet", options...)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	c := NewClient(cc)

	t.Run("anchors", func(t *testing.T) {
		testutil.Anchors(ctx, t, c, true)
	})
	t.Run("readwrite", func(t *testing.T) {
		data, err := ioutil.ReadFile("../../testdata/yubnub.opus")
		if err != nil {
			t.Fatal(err)
		}
		testutil.ReadWrite(ctx, t, c, data)
	})
}
