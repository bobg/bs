// Command bs is a general purpose CLI interface to blob stores.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/bobg/subcmd"
	"github.com/pkg/errors"

	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
	_ "github.com/bobg/bs/store/file"
	_ "github.com/bobg/bs/store/gcs"
	_ "github.com/bobg/bs/store/lru"
	_ "github.com/bobg/bs/store/mem"
	_ "github.com/bobg/bs/store/pg"
)

type maincmd struct {
	s anchor.Store
}

func main() {
	config := flag.String("config", "bsconf.json", "path to config file")
	flag.Parse()

	if *config == "" {
		log.Fatal("Config value not set")
	}

	var conf map[string]interface{}
	f, err := os.Open(*config)
	if err != nil {
		log.Fatalf("Opening config file %s: %s", *config, err)
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&conf)
	if err != nil {
		log.Fatalf("Decoding config file %s: %s", *config, err)
	}

	typ, ok := conf["type"].(string)
	if !ok {
		log.Fatalf("Config file %s missing `type` parameter", *config)
	}

	ctx := context.Background()

	s, err := store.Create(ctx, typ, conf)
	if err != nil {
		log.Fatalf("Creating %s-type store: %s", typ, err)
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
	return map[string]subcmd.Subcmd{
		"get":        c.get,
		"get-anchor": c.getAnchor,
		"list-refs":  c.listRefs,
		"ls":         c.ls,
		"ingest":     c.ingest,
		"put":        c.put,
		"tree":       c.tree,
	}
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
