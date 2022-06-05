//go:build mage
// +build mage

package main

import (
	"context"

	"github.com/bobg/mghash"
	"github.com/bobg/mghash/sqlite"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

var Default = Build

func Build() error {
	mg.Deps(Generate)
	return sh.Run(mg.GoCmd(), "build", "./...")
}

func Test() error {
	mg.Deps(Generate)
	args := []string{"test"}
	if mg.Verbose() {
		args = append(args, "-v")
	}
	args = append(args, "./...")
	return sh.Run(mg.GoCmd(), args...)
}

func Generate(ctx context.Context) error {
	db, err := sqlite.Open(ctx, "hashdb.sqlite")
	if err != nil {
		return errors.Wrap(err, "opening hashdb.sqlite")
	}
	defer db.Close()

	anchor := mghash.JRule{
		Sources: []string{"anchor/anchor.proto"},
		Targets: []string{"anchor/anchor.pb.go"},
		Command: []string{"protoc", "-Ianchor", "--go_out=anchor", "anchor/anchor.proto"},
	}
	fs := mghash.JRule{
		Sources: []string{"fs/fs.proto"},
		Targets: []string{"fs/fs.pb.go"},
		Command: []string{"protoc", "-Ifs", "--go_out=fs", "fs/fs.proto"},
	}
	schema := mghash.JRule{
		Sources: []string{"schema/schema.proto"},
		Targets: []string{"schema/schema.pb.go"},
		Command: []string{"protoc", "-Ischema", "--go_out=schema", "schema/schema.proto"},
	}
	split := mghash.JRule{
		Sources: []string{"split/split.proto"},
		Targets: []string{"split/split.pb.go"},
		Command: []string{"protoc", "-Isplit", "--go_out=split", "split/split.proto"},
	}
	rpc := mghash.JRule{
		Sources: []string{"store/rpc/rpc.proto"},
		Targets: []string{"store/rpc/rpc.pb.go", "store/rpc/rpc_grpc.pb.go"},
		Command: []string{"protoc", "-Istore/rpc", "--go_out=store/rpc", "--go-grpc_out=store/rpc", "store/rpc/rpc.proto"},
	}

	mg.CtxDeps(
		ctx,
		&mghash.Fn{DB: db, Rule: anchor},
		&mghash.Fn{DB: db, Rule: fs},
		&mghash.Fn{DB: db, Rule: schema},
		&mghash.Fn{DB: db, Rule: split},
		&mghash.Fn{DB: db, Rule: rpc},
	)

	return nil

}
