// Package split implements reading and writing of hashsplit trees in a blob store.
// See github.com/bobg/hashsplit for more information.
package split

import (
	"context"
	"io"

	"github.com/bobg/hashsplit"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs"
)

// Writer is an io.WriteCloser that splits its input with a hashsplit.Splitter,
// writing the chunks to a bs.Store as separate blobs.
// It additionally assembles those chunks into a tree with a hashsplit.TreeBuilder.
// The tree nodes are also written to the bs.Store as serialized Node objects.
// The bs.Ref of the tree root is available as Writer.Root after a call to Close.
type Writer struct {
	Ctx    context.Context
	Root   bs.Ref // populated by Close
	st     bs.Store
	spl    *hashsplit.Splitter
	tb     *hashsplit.TreeBuilder
	fanout uint
}

// NewWriter produces a new Writer writing to the given blob store.
// The given context object is stored in the Writer and used in subsequent calls to Write and Close.
// This is an antipattern but acceptable when an object must adhere to a context-free stdlib interface
// (https://github.com/golang/go/wiki/CodeReviewComments#contexts).
// Callers may replace the context object during the lifetime of the Writer as needed.
func NewWriter(ctx context.Context, st bs.Store, opts ...Option) *Writer {
	tb := hashsplit.NewTreeBuilder()
	w := &Writer{
		Ctx:    ctx,
		st:     st,
		tb:     tb,
		fanout: 4, // TODO: does this provide the best fan-out?
	}
	spl := hashsplit.NewSplitter(func(bytes []byte, level uint) error {
		size := len(bytes)
		ref, _, err := st.Put(ctx, bytes)
		if err != nil {
			return errors.Wrap(err, "writing split chunk to store")
		}
		tb.Add(ref[:], size, level/w.fanout)
		return nil
	})
	spl.MinSize = 1024
	spl.SplitBits = 14
	w.spl = spl
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Write implements io.Writer.
func (w *Writer) Write(inp []byte) (int, error) {
	return w.spl.Write(inp)
}

// Close implements io.Closer.
func (w *Writer) Close() error {
	if w.tb == nil {
		return nil
	}
	err := w.spl.Close()
	if err != nil {
		return err
	}
	root := w.tb.Root()
	rootRef, err := storeTree(w.Ctx, w.st, root)
	if err != nil {
		return err
	}
	w.Root = rootRef
	w.tb = nil
	return nil
}

func storeTree(ctx context.Context, s bs.Store, n *hashsplit.Node) (bs.Ref, error) {
	tn := &Node{Size: n.Size}
	if len(n.Leaves) > 0 {
		tn.Leaves = n.Leaves
	} else {
		for _, child := range n.Nodes {
			childRef, err := storeTree(ctx, s, child)
			if err != nil {
				return bs.Ref{}, err
			}
			tn.Nodes = append(tn.Nodes, childRef[:])
		}
	}
	ref, _, err := bs.PutProto(ctx, s, tn)
	return ref, err
}

type Option func(*Writer)

func Bits(n uint) Option {
	return func(w *Writer) {
		w.spl.SplitBits = n
	}
}

func MinSize(n int) Option {
	return func(w *Writer) {
		w.spl.MinSize = n
	}
}

func Fanout(n uint) Option {
	return func(w *Writer) {
		w.fanout = n
	}
}

// Read reads blobs from `g`,
// reassembling the content of the blob tree created with Write
// and writing it to `w`.
// The ref of the root Node is given by `ref`.
func Read(ctx context.Context, g bs.Getter, ref bs.Ref, w io.Writer) error {
	var tn Node
	err := bs.GetProto(ctx, g, ref, &tn)
	if err != nil {
		return err
	}
	return splitRead(ctx, g, &tn, w)
}

func splitRead(ctx context.Context, g bs.Getter, n *Node, w io.Writer) error {
	if len(n.Leaves) > 0 {
		return splitReadHelper(ctx, g, n.Leaves, func(m []byte) error {
			_, err := w.Write(m)
			return err
		})
	}
	return splitReadHelper(ctx, g, n.Nodes, func(m []byte) error {
		var tn Node
		err := proto.Unmarshal(m, &tn)
		if err != nil {
			return err
		}
		return splitRead(ctx, g, &tn, w)
	})
}

func splitReadHelper(ctx context.Context, g bs.Getter, subrefsBytes [][]byte, do func([]byte) error) error {
	for _, s := range subrefsBytes {
		b, err := g.Get(ctx, bs.RefFromBytes(s))
		if err != nil {
			return errors.Wrapf(err, "getting %x", s)
		}
		err = do(b)
		if err != nil {
			return err
		}
	}
	return nil
}
