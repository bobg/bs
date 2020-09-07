package main

import (
	"context"
	"flag"
	"log"
	"net"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/bobg/bs/store/rpc"
)

func (c maincmd) serve(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var (
		addr = fs.String("addr", ":2969", "server listen address")
	)
	err := fs.Parse(args)
	if err != nil {
		return errors.Wrap(err, "parsing args")
	}

	gs := grpc.NewServer()
	rs := rpc.NewServer(c.s)
	rpc.RegisterStoreServer(gs, rs)
	defer gs.GracefulStop()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return errors.Wrapf(err, "listening on %s", *addr)
	}
	defer lis.Close()

	log.Printf("listening on %s", lis.Addr())

	return gs.Serve(lis)
}
