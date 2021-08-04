package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/split"
)

func (c maincmd) tree(ctx context.Context, a, refstr, atstr string, args []string) error {
	if (a == "" && refstr == "") || (a != "" && refstr != "") {
		return errors.New("must supply one of -anchor or -ref")
	}

	var (
		ref bs.Ref
		err error
	)

	if a != "" {
		at := time.Now()
		if atstr != "" {
			at, err = parsetime(atstr)
			if err != nil {
				return errors.Wrap(err, "parsing -at")
			}
		}

		as, ok := c.s.(anchor.Store)
		if !ok {
			return fmt.Errorf("%T is not an anchor.Store", c.s)
		}

		ref, err = as.GetAnchor(ctx, a, at)
		if err != nil {
			return errors.Wrapf(err, "getting anchor %s at time %s", a, at)
		}
	} else {
		ref, err = bs.RefFromHex(refstr)
		if err != nil {
			return errors.Wrapf(err, "decoding ref %s", refstr)
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
		for _, child := range tn.Nodes {
			var subref bs.Ref
			copy(subref[:], child.Ref)
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
