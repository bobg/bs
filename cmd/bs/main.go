// Command bs is a general purpose CLI interface to blob stores.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/bobg/subcmd"
	"github.com/pkg/errors"

	"github.com/bobg/bs/anchor"
	_ "github.com/bobg/bs/store/compress"
	_ "github.com/bobg/bs/store/file"
	_ "github.com/bobg/bs/store/gcs"
	_ "github.com/bobg/bs/store/lru"
	_ "github.com/bobg/bs/store/mem"
	_ "github.com/bobg/bs/store/pg"
	_ "github.com/bobg/bs/store/rpc"
	_ "github.com/bobg/bs/store/sqlite3"
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
			log.Fatalf("could not create CPU profile: %s", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("could not start CPU profile: %s", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprof != "" {
		f, err := os.Create(*memprof)
		if err != nil {
			log.Fatalf("could not create memory profile: %s", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("could not write memory profile: %s", err)
		}
	}

	if *config == "" {
		log.Fatal("Config value not set")
	}

	ctx := context.Background()

	s, err := storeFromConfig(ctx, *config)
	if err != nil {
		log.Fatal(err)
	}

	ss, ok := s.(anchor.Store)
	if !ok {
		log.Fatal("not an anchor store")
	}

	err = subcmd.Run(ctx, maincmd{s: ss}, flag.Args())
	if err != nil {
		log.Fatal(err)
	}
}

func (c maincmd) Subcmds() map[string]subcmd.Subcmd {
	return subcmd.Commands(
		"client", c.client, subcmd.Params(
			"addr", subcmd.String, ":2969", "server address",
			"insecure", subcmd.Bool, false, "connect insecurely",
		),
		"get", c.get, subcmd.Params(
			"anchor", subcmd.String, "", "anchor of blob or tree to get",
			"ref", subcmd.String, "", "ref of blob or tree to get",
			"split", subcmd.Bool, false, "get a split tree instead of a single blob",
			"at", subcmd.String, "", "timestamp for anchor (default: now)",
		),
		"get-anchor", c.getAnchor, subcmd.Params(
			"at", subcmd.String, "", "timestamp for anchor (default: now)",
		),
		"list-anchors", c.listAnchors, subcmd.Params(
			"start", subcmd.String, "", "start after this anchor name",
		),
		"list-refs", c.listRefs, subcmd.Params(
			"start", subcmd.String, "", "start after this ref",
		),
		"ls", c.ls, subcmd.Params(
			"anchor", subcmd.String, "", "anchor of dir to get",
			"ref", subcmd.String, "", "ref of dir",
			"at", subcmd.String, "", "timestamp for anchor (default: now)",
		),
		"addtodir", c.addToDir, subcmd.Params(
			"anchor", subcmd.String, "", "anchor for dir; may be existing dir to add to",
			"at", subcmd.String, "", "timestamp for anchor (default: now)",
			"ref", subcmd.String, "", "ref of dir to add to",
		),
		"put", c.put, subcmd.Params(
			"anchor", subcmd.String, "", "anchor to assign to added ref",
			"split", subcmd.Bool, false, "get a split tree instead of a single blob",
			"at", subcmd.String, "", "timestamp for anchor (default: now)",
			"bits", subcmd.Uint, 0, "with -split, the number of bits to split on (to control chunk size)",
		),
		"serve", c.serve, subcmd.Params(
			"addr", subcmd.String, ":2969", "server listen address",
		),
		"sync", c.sync, nil,
		"tree", c.tree, subcmd.Params(
			"anchor", subcmd.String, "", "anchor of blob or tree to get",
			"ref", subcmd.String, "", "ref of blob or tree to get",
			"at", subcmd.String, "", "timestamp for anchor (default: now)",
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
