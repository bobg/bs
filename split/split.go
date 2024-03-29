// Package split implements reading and writing of hashsplit trees in a blob store.
// See github.com/bobg/hashsplit for more information.
package split

import (
	"context"
	"io"
	"sort"

	"github.com/bobg/hashsplit"
	"github.com/pkg/errors"

	"github.com/bobg/bs/gc"

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
// If no data was written before the Writer was closed,
// Root will be bs.Zero.
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
		fanout: 8, // TODO: does this provide the best fan-out?
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
	spl.SplitBits = 16
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
	if root != nil {
		rootNodeWrapper := root.(*nodeWrapper)
		rootRef, _, err := bs.PutProto(w.Ctx, w.st, rootNodeWrapper.node)
		if err != nil {
			return err
		}
		w.Root = rootRef
	}
	w.tb = nil
	return nil
}

// Option is the type of an option passed to NewWriter.
type Option = func(*Writer)

// Bits is an option for NewWriter that changes the number of trailing zero bits in the rolling checksum used to identify chunk boundaries.
// A chunk boundary occurs on average once every 2^n bytes.
// (But the actual median chunk size is the logarithm, base (2^n-1)/(2^n), of 0.5.)
// The default value for n is 16,
// producing a chunk boundary every 65,536 bytes,
// and a median chunk size of 45,426 bytes.
func Bits(n uint) Option {
	return func(w *Writer) {
		w.spl.SplitBits = n
	}
}

// MinSize is an option for NewWriter that sets a lower bound on the size of a chunk.
// No chunk may be smaller than this,
// except for the final one in the input stream.
// The value must be 64 or higher.
// The default is 1024.
func MinSize(n int) Option {
	return func(w *Writer) {
		w.spl.MinSize = n
	}
}

// Fanout is an option for NewWriter that can change the fanout of the nodes in the tree produced.
// The value of n must be 1 or higher.
// The default is 8.
// In a nutshell,
// nodes in the hashsplit tree will tend to have around 2n children each,
// because each chunk's "level" is reduced by dividing it by n.
// For more information see https://pkg.go.dev/github.com/bobg/hashsplit#TreeBuilder.Add.
func Fanout(n uint) Option {
	return func(w *Writer) {
		w.fanout = n
	}
}

// Reader traverses the nodes of a hashsplit tree to produce the original (unsplit) input.
// It implements io.ReadSeeker.
// The given context object is stored in the Reader and used in subsequent calls to Read.
// This is an antipattern but acceptable when an object must adhere to a context-free stdlib interface
// (https://github.com/golang/go/wiki/CodeReviewComments#contexts).
// Callers may replace the context object during the lifetime of the Reader as needed.
type Reader struct {
	Ctx   context.Context
	g     bs.Getter
	pos   uint64
	stack []*Node // stack[0] is always filled and is the root of the split tree
}

// NewReader produces a new Reader for the hashsplit tree stored in g and rooted at ref.
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

// Read implements io.Reader.
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

// Seek implements io.Seeker.
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

// Size returns the number of (unsplit) input bytes represented by the full hashsplit tree.
func (r *Reader) Size() uint64 {
	return r.stack[0].Size
}

// Protect is a gc.ProtectFunc for use on split.Node refs.
func Protect(ctx context.Context, g bs.Getter, ref bs.Ref) ([]gc.ProtectPair, error) {
	var n Node
	err := bs.GetProto(ctx, g, ref, &n)
	if err != nil {
		return nil, err
	}

	var result []gc.ProtectPair
	for _, child := range n.Nodes {
		result = append(result, gc.ProtectPair{Ref: bs.RefFromBytes(child.Ref), F: Protect})
	}
	for _, child := range n.Leaves {
		result = append(result, gc.ProtectPair{Ref: bs.RefFromBytes(child.Ref)})
	}

	return result, nil
}
