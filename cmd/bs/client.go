package main

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/bobg/subcmd"

	"github.com/bobg/bs/store/rpc"
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
