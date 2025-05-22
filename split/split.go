// Package split implements reading and writing of hashsplit trees in a blob store.
// See github.com/bobg/hashsplit for more information.
package split

import (
	"context"
	"io"
	"sort"

	"github.com/bobg/errors"
	"github.com/bobg/go-generics/v4/slices"
	"github.com/bobg/hashsplit/v2"
	"github.com/bobg/seqs"
	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs/v2"
	"github.com/bobg/bs/v2/gc"
)

func Split(ctx context.Context, st bs.Store, r io.Reader, opts ...Option) (bs.Ref, error) {
	spl := &splitter{spl: hashsplit.NewSplitter(), fanout: 8}
	spl.spl.SplitBits = 16
	for _, opt := range opts {
		opt(spl)
	}
	split, errptr := spl.spl.Split(r)
	reducedLevels := seqs.Map2(split, func(chunk []byte, level int) ([]byte, int) { return chunk, level / spl.fanout })
	tree := hashsplit.Tree(reducedLevels)
	var (
		root     *hashsplit.TreeNode
		nodeRefs = make(map[*hashsplit.TreeNode]bs.Ref)
	)
	for node := range tree {
		if err := storeNodeChildrenAndLeaves(ctx, st, node, nodeRefs); err != nil {
			return bs.Zero, errors.Wrap(err, "storing tree node")
		}
		root = node
	}
	if err := *errptr; err != nil {
		return bs.Zero, errors.Wrap(err, "splitting input")
	}
	if root == nil {
		return bs.Zero, nil
	}
	return storeNode(ctx, st, root, nodeRefs)
}

func storeNodeChildrenAndLeaves(ctx context.Context, st bs.Store, node *hashsplit.TreeNode, refs map[*hashsplit.TreeNode]bs.Ref) error {
	if err := storeNodeChildren(ctx, st, node, refs); err != nil {
		return errors.Wrap(err, "storing node children")
	}
	return storeNodeLeaves(ctx, st, node)
}

func storeNodeChildren(ctx context.Context, st bs.Store, node *hashsplit.TreeNode, refs map[*hashsplit.TreeNode]bs.Ref) error {
	for _, child := range node.Children {
		if _, err := storeNode(ctx, st, child, refs); err != nil {
			return errors.Wrap(err, "storing child node")
		}
	}
	return nil
}

func storeNode(ctx context.Context, st bs.Store, node *hashsplit.TreeNode, refs map[*hashsplit.TreeNode]bs.Ref) (bs.Ref, error) {
	p, err := treeNodeToProto(node, refs)
	if err != nil {
		return bs.Zero, errors.Wrap(err, "converting node to proto")
	}
	ref, _, err := bs.PutProto(ctx, st, p)
	if err != nil {
		return bs.Zero, errors.Wrap(err, "storing node")
	}
	refs[node] = ref
	return ref, nil
}

func treeNodeToProto(node *hashsplit.TreeNode, refs map[*hashsplit.TreeNode]bs.Ref) (*Node, error) {
	// Note: node.Chunks has already been mapped to (serialized) Child protos
	// in an earlier call to storeNodeLeaves.

	result := &Node{
		Offset: node.Offset,
		Size:   node.Size,
		Nodes: slices.Map(node.Children, func(child *hashsplit.TreeNode) *Child {
			ref := refs[child]
			return &Child{
				Ref:    ref[:],
				Offset: child.Offset,
			}
		}),
	}
	leaves, err := slices.Mapx(node.Chunks, func(_ int, chunk []byte) (*Child, error) {
		var p Child
		err := proto.Unmarshal(chunk, &p)
		return &p, err
	})
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling chunk")
	}
	result.Leaves = leaves
	return result, nil
}

func storeNodeLeaves(ctx context.Context, st bs.Store, node *hashsplit.TreeNode) error {
	offset := node.Offset
	for i, chunk := range node.Chunks {
		ref, _, err := st.Put(ctx, chunk)
		if err != nil {
			return errors.Wrap(err, "storing chunk")
		}
		child := &Child{
			Ref:    ref[:],
			Offset: offset,
		}
		offset += uint64(len(chunk)) // xxx ?
		p, err := proto.Marshal(child)
		if err != nil {
			return errors.Wrap(err, "marshalling chunk proto")
		}
		node.Chunks[i] = p
	}
	return nil
}

type splitter struct {
	spl    *hashsplit.Splitter
	fanout int
}

// Option is the type of an option passed to NewWriter.
type Option func(*splitter)

// Bits is an option for NewWriter that changes the number of trailing zero bits in the rolling checksum used to identify chunk boundaries.
// A chunk boundary occurs on average once every 2^n bytes.
// (But the actual median chunk size is the logarithm, base (2^n-1)/(2^n), of 0.5.)
// The default value for n is 16,
// producing a chunk boundary every 65,536 bytes,
// and a median chunk size of 45,426 bytes.
func Bits(n int) Option {
	return func(spl *splitter) {
		spl.spl.SplitBits = n
	}
}

// MinSize is an option for NewWriter that sets a lower bound on the size of a chunk.
// No chunk may be smaller than this,
// except for the final one in the input stream.
// The value must be 64 or higher.
// The default is 1024.
func MinSize(n int) Option {
	return func(spl *splitter) {
		spl.spl.MinSize = n
	}
}

// Fanout is an option for NewWriter that can change the fanout of the nodes in the tree produced.
// The value of n must be 1 or higher.
// The default is 8.
// In a nutshell,
// nodes in the hashsplit tree will tend to have around 2n children each,
// because each chunk's "level" is reduced by dividing it by n.
// For more information see https://pkg.go.dev/github.com/bobg/hashsplit#TreeBuilder.Add.
func Fanout(n int) Option {
	return func(spl *splitter) {
		spl.fanout = n
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
