package main

import (
	"context"
	"fmt"
	"net"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/bobg/bs/store/rpc"
)

func (c maincmd) serve(ctx context.Context, addr string, _ []string) error {
	gs := grpc.NewServer()
	rs := rpc.NewServer(c.s)
	rpc.RegisterStoreServer(gs, rs)
	defer gs.GracefulStop()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrapf(err, "listening on %s", addr)
	}
	defer lis.Close()

	fmt.Printf("Listening on %s\n", lis.Addr())

	return gs.Serve(lis)
}
