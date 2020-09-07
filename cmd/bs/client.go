package main

import (
	"context"
	"flag"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/bobg/subcmd"

	"github.com/bobg/bs/store/rpc"
)

func (c maincmd) client(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var (
		addr     = fs.String("addr", ":2969", "server address")
		insecure = fs.Bool("insecure", false, "connect insecurely")
	)
	err := fs.Parse(args)
	if err != nil {
		return errors.Wrap(err, "parsing args")
	}

	var opts []grpc.DialOption
	if *insecure {
		opts = append(opts, grpc.WithInsecure())
	}

	cc, err := grpc.Dial(*addr, opts...)
	if err != nil {
		return errors.Wrapf(err, "connecting to %s", *addr)
	}
	defer cc.Close()

	cl := rpc.NewClient(cc)
	return subcmd.Run(ctx, maincmd{s: cl}, flag.Args())
}
