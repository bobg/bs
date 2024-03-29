// Command bs is a general purpose CLI interface to blob stores.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/bobg/subcmd/v2"
	"github.com/pkg/errors"

	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
	_ "github.com/bobg/bs/store/file"
	_ "github.com/bobg/bs/store/gcs"
	_ "github.com/bobg/bs/store/logging"
	_ "github.com/bobg/bs/store/lru"
	_ "github.com/bobg/bs/store/mem"
	_ "github.com/bobg/bs/store/pg"
	_ "github.com/bobg/bs/store/rpc"
	_ "github.com/bobg/bs/store/sqlite3"
	_ "github.com/bobg/bs/store/transform"
)

type maincmd struct {
	s anchor.Store
}

func main() {
	var (
		config  = flag.String("config", "bsconf.json", "path to config file")
		cpuprof = flag.String("cpuprof", "", "cpu profile file")
		memprof = flag.String("memprof", "", "mem profile file")
	)
	flag.Parse()

	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			fatalf("Could not create CPU profile: %s", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fatalf("Could not start CPU profile: %s", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprof != "" {
		f, err := os.Create(*memprof)
		if err != nil {
			fatalf("Could not create memory profile: %s", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			fatalf("Could not write memory profile: %s", err)
		}
	}

	if *config == "" {
		fatal("Config value not set")
	}

	ctx := context.Background()

	s, err := store.FromConfigFile(ctx, *config)
	if err != nil {
		fatal(err)
	}

	ss, ok := s.(anchor.Store)
	if !ok {
		fatal("Not an anchor store")
	}

	err = subcmd.Run(ctx, maincmd{s: ss}, flag.Args())

	var usage subcmd.Usage
	if errors.As(err, &usage) {
		fatal(usage.Long())
	} else if err != nil {
		fatal(err)
	}
}

func (c maincmd) Subcmds() map[string]subcmd.Subcmd {
	return subcmd.Commands(
		"client", c.client, "run another subcommand with a client against the given bs server", subcmd.Params(
			"-addr", subcmd.String, ":2969", "server address",
			"-insecure", subcmd.Bool, false, "connect insecurely",
		),
		"get", c.get, "get a blob or tree", subcmd.Params(
			"-anchor", subcmd.String, "", "anchor of blob or tree to get",
			"-ref", subcmd.String, "", "ref of blob or tree to get",
			"-split", subcmd.Bool, false, "get a split tree instead of a single blob",
			"-at", subcmd.String, "", "timestamp for anchor (default: now)",
		),
		"get-anchor", c.getAnchor, "get the ref of an anchor", subcmd.Params(
			"-at", subcmd.String, "", "timestamp for anchor (default: now)",
			"anchor", subcmd.String, "", "anchor name",
		),
		"list-anchors", c.listAnchors, "list anchors", nil,
		"list-refs", c.listRefs, "list refs", subcmd.Params(
			"-start", subcmd.String, "", "start after this ref",
		),
		"ls", c.ls, "list directory contents", subcmd.Params(
			"-anchor", subcmd.String, "", "anchor of dir to get",
			"-ref", subcmd.String, "", "ref of dir",
			"-at", subcmd.String, "", "timestamp for anchor (default: now)",
		),
		"addtodir", c.addToDir, "add to directory", subcmd.Params(
			"-anchor", subcmd.String, "", "anchor for dir; may be existing dir to add to",
			"-at", subcmd.String, "", "timestamp for anchor (default: now)",
			"-ref", subcmd.String, "", "ref of dir to add to",
			"path", subcmd.String, "", "path to add",
		),
		"put", c.put, "store a blob or hashsplit tree", subcmd.Params(
			"-anchor", subcmd.String, "", "anchor to assign to added ref",
			"-split", subcmd.Bool, false, "get a split tree instead of a single blob",
			"-at", subcmd.String, "", "timestamp for anchor (default: now)",
			"-bits", subcmd.Uint, 14, "with -split, the number of bits to split on (to control chunk size)",
			"-fanout", subcmd.Uint, 4, "with -split, divisor for each node's level, to control fan-out (higher numbers = more children per node)",
		),
		"serve", c.serve, "run a bs server", subcmd.Params(
			"-addr", subcmd.String, ":2969", "server listen address",
		),
		"sync", c.sync, "synchronize blob stores", nil,
		"tree", c.tree, "show hashsplit tree structure", subcmd.Params(
			"-anchor", subcmd.String, "", "anchor of blob or tree to get",
			"-ref", subcmd.String, "", "ref of blob or tree to get",
			"-at", subcmd.String, "", "timestamp for anchor (default: now)",
		),
	)
}

var layouts = []string{
	time.RFC3339Nano, time.RFC3339, time.ANSIC, time.UnixDate,
}

func parsetime(s string) (time.Time, error) {
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil { // sic
			return t, nil
		}
	}
	return time.Time{}, errors.New("could not parse time")
}

func fatal(arg ...interface{}) {
	fatalf(strings.Repeat("%s ", len(arg)), arg...)
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}
