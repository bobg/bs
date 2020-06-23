// Command fbs operates on a file-based blobstore.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/file"
)

func main() {
	var (
		anchor = flag.String("anchor", "", "anchor")
		get    = flag.Bool("get", false, "get")
		tree   = flag.Bool("tree", false, "get tree structure")
		put    = flag.Bool("put", false, "put")
		root   = flag.String("root", "", "root")
		atstr  = flag.String("at", "", "at")
	)
	flag.Parse()

	var (
		err       error
		ctx       = context.Background()
		filestore = file.New(*root)
	)

	at := time.Now()
	if *atstr != "" {
		at, err = parsetime(*atstr)
		if err != nil {
			log.Fatalf("parsing -at time: %s", err)
		}
	}

	if *put {
		ref, err := bs.SplitWrite(ctx, filestore, os.Stdin, nil)
		if err != nil {
			log.Fatalf("writing to filestore: %s", err)
		}
		fmt.Printf("%s\n", ref)
		if *anchor != "" {
			err = filestore.PutAnchor(ctx, ref, bs.Anchor(*anchor), at)
			if err != nil {
				log.Fatalf("storing anchor: %s", err)
			}
		}
	} else if *get || *tree {
		var ref bs.Ref
		if *anchor != "" {
			ref, err = filestore.GetAnchor(ctx, bs.Anchor(*anchor), at)
			if err != nil {
				log.Fatalf("getting anchor: %s", err)
			}
		} else if flag.NArg() != 1 {
			log.Fatalf("usage: %s -get REF", os.Args[0])
		} else {
			err = ref.FromHex(flag.Arg(0))
			if err != nil {
				log.Fatalf("parsing command-line ref: %s", err)
			}
		}

		if *get {
			err = bs.SplitRead(ctx, filestore, ref, os.Stdout)
			if err != nil {
				log.Fatalf("reading from filestore: %s", err)
			}
		} else {
			err = doTree(ctx, filestore, ref, 0)
			if err != nil {
				log.Fatal(err)
			}
		}
	} else {
		log.Fatal("must supply one of -put, -get, -tree")
	}
}

func doTree(ctx context.Context, g bs.Getter, ref bs.Ref, depth int) error {
	var tn bs.TreeNode
	err := bs.GetProto(ctx, g, ref, &tn)
	if err != nil {
		return errors.Wrapf(err, "getting treenode %s", ref)
	}

	indent := strings.Repeat("  ", depth)
	fmt.Printf("%ssize: %d\n", indent, tn.Size)
	if len(tn.Nodes) > 0 {
		fmt.Printf("%snodes:\n", indent)
		for _, n := range tn.Nodes {
			var subref bs.Ref
			copy(subref[:], n)
			fmt.Printf("%s %s:\n", indent, subref)
			err = doTree(ctx, g, subref, depth+1)
			if err != nil {
				return err
			}
		}
	} else {
		fmt.Printf("%sleaves:\n", indent)
		for _, l := range tn.Leaves {
			fmt.Printf("%s %x\n", indent, l)
		}
	}
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
