package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/bobg/subcmd"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
	"github.com/bobg/bs/store"
	_ "github.com/bobg/bs/store/bt"
	_ "github.com/bobg/bs/store/file"
	_ "github.com/bobg/bs/store/lru"
	_ "github.com/bobg/bs/store/mem"
	_ "github.com/bobg/bs/store/pg"
)

type maincmd struct {
	s bs.Store
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

	err = subcmd.Run(ctx, maincmd{s: s}, flag.Args())
	if err != nil {
		log.Fatal(err)
	}
}

func (c maincmd) Subcmds() map[string]subcmd.Subcmd {
	return map[string]subcmd.Subcmd{
		"get": c.get,
		"put": c.put,
	}
}

func (c maincmd) get(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var (
		anchor  = fs.String("anchor", "", "anchor of blob or tree to get")
		refstr  = fs.String("ref", "", "ref of blob or tree to get")
		dosplit = fs.Bool("split", false, "get a split tree instead of a single blob")
		atstr   = fs.String("at", "", "timestamp for anchor (default: now)")
	)
	err := fs.Parse(args)
	if err != nil {
		return errors.Wrap(err, "parsing args")
	}

	if (*anchor == "" && *refstr == "") || (*anchor != "" && *refstr != "") {
		return errors.New("must supply one of -anchor or -ref")
	}

	var ref bs.Ref

	if *anchor != "" {
		at := time.Now()
		if *atstr != "" {
			at, err = parsetime(*atstr)
			if err != nil {
				return errors.Wrap(err, "parsing -at")
			}
		}

		ref, err = c.s.GetAnchor(ctx, bs.Anchor(*anchor), at)
		if err != nil {
			return errors.Wrapf(err, "getting anchor %s at time %s", *anchor, at)
		}
	} else {
		ref, err = bs.RefFromHex(*refstr)
		if err != nil {
			return errors.Wrapf(err, "decoding ref %s", *refstr)
		}
	}

	if *dosplit {
		return split.Read(ctx, c.s, ref, os.Stdout)
	}

	blob, err := c.s.Get(ctx, ref)
	if err != nil {
		return errors.Wrapf(err, "getting blob %s", ref)
	}
	_, err = os.Stdout.Write(blob)
	return errors.Wrap(err, "writing blob to stdout")
}

func (c maincmd) put(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var (
		anchor  = fs.String("anchor", "", "anchor of blob or tree to get")
		dosplit = fs.Bool("split", false, "get a split tree instead of a single blob")
		atstr   = fs.String("at", "", "timestamp for anchor (default: now)")
	)
	err := fs.Parse(args)
	if err != nil {
		return errors.Wrap(err, "parsing args")
	}

	var (
		ref   bs.Ref
		added bool
	)
	if *dosplit {
		ref, err = split.Write(ctx, c.s, os.Stdin, nil)
		if err != nil {
			return errors.Wrap(err, "splitting stdin to store")
		}
	} else {
		blob, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return errors.Wrap(err, "reading stdin")
		}
		ref, added, err = c.s.Put(ctx, blob)
		if err != nil {
			return errors.Wrap(err, "storing blob")
		}
	}

	if *anchor != "" {
		at := time.Now()
		if *atstr != "" {
			at, err = parsetime(*atstr)
			if err != nil {
				return errors.Wrap(err, "parsing -at")
			}
		}

		err = c.s.PutAnchor(ctx, ref, bs.Anchor(*anchor), at)
		if err != nil {
			return errors.Wrapf(err, "associating anchor %s with blob %s at time %s", *anchor, ref, at)
		}
	}

	log.Printf("ref %s (added: %v)", ref, added)

	return nil
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
