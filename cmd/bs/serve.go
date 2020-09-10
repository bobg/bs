package main

import (
	"context"
	"log"
	"net"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/bobg/bs/store/rpc"
)

func (c maincmd) serve(ctx context.Context, addr string, args []string) error {
	gs := grpc.NewServer()
	rs := rpc.NewServer(c.s)
	rpc.RegisterStoreServer(gs, rs)
	defer gs.GracefulStop()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrapf(err, "listening on %s", addr)
	}
	defer lis.Close()

	log.Printf("listening on %s", lis.Addr())

	return gs.Serve(lis)
}
