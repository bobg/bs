package main

import (
	"context"

	"github.com/bobg/errors"
	"github.com/bobg/subcmd/v2"
	"google.golang.org/grpc"

	"github.com/bobg/bs/v2/store/rpc"
)

func (c maincmd) client(ctx context.Context, addr string, insecure bool, args []string) error {
	var opts []grpc.DialOption
	if insecure {
		opts = append(opts, grpc.WithInsecure())
	}

	cc, err := grpc.Dial(addr, opts...)
	if err != nil {
		return errors.Wrapf(err, "connecting to %s", addr)
	}
	defer cc.Close()

	cl := rpc.NewClient(cc)

	return subcmd.Run(ctx, maincmd{s: cl}, args)
}
