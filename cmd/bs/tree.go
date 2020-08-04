package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

func (c maincmd) tree(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var (
		anchor = fs.String("anchor", "", "anchor of blob or tree to get")
		refstr = fs.String("ref", "", "ref of blob or tree to get")
		atstr  = fs.String("at", "", "timestamp for anchor (default: now)")
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

		ref, _, err = c.s.GetAnchor(ctx, bs.Anchor(*anchor), at)
		if err != nil {
			return errors.Wrapf(err, "getting anchor %s at time %s", *anchor, at)
		}
	} else {
		ref, err = bs.RefFromHex(*refstr)
		if err != nil {
			return errors.Wrapf(err, "decoding ref %s", *refstr)
		}
	}

	return doTree(ctx, c.s, ref, 0)
}

func doTree(ctx context.Context, g bs.Getter, ref bs.Ref, depth int) error {
	var tn split.Node
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
