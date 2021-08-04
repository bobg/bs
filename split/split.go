// Package split implements reading and writing of hashsplit trees in a blob store.
// See github.com/bobg/hashsplit for more information.
package split

import (
	"context"
	"io"
	"sort"

	"github.com/bobg/hashsplit/v2"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
)

var (
	_ io.WriteCloser = (*Writer)(nil)
	_ io.ReadSeeker  = (*Reader)(nil)
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
	w := &Writer{
		Ctx:    ctx,
		st:     st,
		fanout: 4, // TODO: does this provide the best fan-out?
	}

	tb := hashsplit.TreeBuilder{
		F: func(n *hashsplit.TreeBuilderNode) (hashsplit.Node, error) {
			var (
				offset = n.Offset()
				result = Node{
					Offset: offset,
					Size:   n.Size(),
				}
			)

			for _, child := range n.Nodes {
				childNodeWrapper := child.(*nodeWrapper)
				ref, _, err := bs.PutProto(w.Ctx, st, childNodeWrapper.node)
				if err != nil {
					return nil, err
				}
				result.Nodes = append(result.Nodes, &Child{Ref: ref[:], Offset: offset})
				offset += childNodeWrapper.Size()
			}

			for _, chunk := range n.Chunks {
				ref, _, err := st.Put(w.Ctx, chunk)
				if err != nil {
					return nil, err
				}
				result.Leaves = append(result.Leaves, &Child{Ref: ref[:], Offset: offset})
				offset += uint64(len(chunk))
			}

			return &nodeWrapper{node: &result, ctx: w.Ctx, st: st}, nil // Note, this *Node is not written to the store, but its children are
		},
	}
	w.tb = &tb

	spl := hashsplit.NewSplitter(func(bytes []byte, level uint) error {
		return tb.Add(bytes, level/w.fanout)
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
	root, err := w.tb.Root()
	if err != nil {
		return err
	}
	rootNodeWrapper := root.(*nodeWrapper)
	rootRef, _, err := bs.PutProto(w.Ctx, w.st, rootNodeWrapper.node)
	if err != nil {
		return err
	}
	w.Root = rootRef
	w.tb = nil
	return nil
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

type Reader struct {
	Ctx   context.Context
	g     bs.Getter
	pos   uint64
	stack []*Node // stack[0] is always filled and is the root of the split tree
}

func NewReader(ctx context.Context, g bs.Getter, ref bs.Ref) (*Reader, error) {
	var root Node
	err := bs.GetProto(ctx, g, ref, &root)
	if err != nil {
		return nil, errors.Wrapf(err, "getting root ref %s", ref)
	}
	return &Reader{
		Ctx:   ctx,
		g:     g,
		stack: []*Node{&root}, // stack[0] is always present
	}, nil
}

func (r *Reader) Read(buf []byte) (int, error) {
	var n int
	for len(buf) > 0 {
		// First, unwind the stack to a node containing r.pos
		for {
			node := r.stack[len(r.stack)-1]
			if r.pos >= node.Offset && r.pos < (node.Offset+node.Size) {
				break
			}
			if len(r.stack) == 1 {
				return n, io.EOF
			}
			r.stack = r.stack[:len(r.stack)-1]
		}

		// Now walk down the tree,
		// pushing nodes onto the stack,
		// until we get to the right leaf node.
		for {
			node := r.stack[len(r.stack)-1]
			if len(node.Leaves) > 0 {
				break
			}

			index := 0
			if r.pos > node.Offset {
				// TODO: We can do better than this Search in the case where we're moving sequentially from one Node to its next sibling.
				index = sort.Search(len(node.Nodes), func(i int) bool {
					return node.Nodes[i].Offset > r.pos
				})
				index--
			}

			childRef := bs.RefFromBytes(node.Nodes[index].Ref)
			var childNode Node
			err := bs.GetProto(r.Ctx, r.g, childRef, &childNode)
			if err != nil {
				return n, errors.Wrapf(err, "getting tree node %s", childRef)
			}
			r.stack = append(r.stack, &childNode)
		}

		// Now we have a leaf node on top of the stack.
		// Discard children that precede r.pos.

		children := r.stack[len(r.stack)-1].Leaves
		for len(children) > 1 && children[1].Offset <= r.pos {
			// TODO: This could be a sort.Search
			children = children[1:]
		}

		for len(children) > 0 && len(buf) > 0 {
			offset := children[0].Offset
			ref := bs.RefFromBytes(children[0].Ref)
			chunk, err := r.g.Get(r.Ctx, ref)
			if err != nil {
				return n, errors.Wrapf(err, "getting tree node %s", ref)
			}

			// Discard any part of chunk that precedes r.pos
			skip := r.pos - offset
			chunk = chunk[skip:]
			offset += skip

			if len(chunk) >= len(buf) {
				copy(buf, chunk)
				n += len(buf)
				r.pos += uint64(len(buf))
				return n, nil
			}

			copy(buf, chunk)
			n += len(chunk)
			r.pos += uint64(len(chunk))
			buf = buf[len(chunk):]
			children = children[1:]
		}
	}
	return n, nil
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.pos = uint64(offset)

	case io.SeekCurrent:
		if offset >= 0 {
			r.pos += uint64(offset)
		} else {
			r.pos -= uint64(-offset)
		}

	case io.SeekEnd:
		if offset >= 0 {
			r.pos = r.stack[0].Size + uint64(offset)
		} else {
			r.pos = r.stack[0].Size - uint64(-offset)
		}
	}

	return int64(r.pos), nil
}
