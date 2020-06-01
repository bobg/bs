package bs

import (
	"context"
	"io"

	"github.com/bobg/hashsplit"
	"google.golang.org/protobuf/proto"
)

func SplitWrite(ctx context.Context, s Store, r io.Reader, splitter *hashsplit.Splitter) (Ref, error) {
	if splitter == nil {
		splitter = &hashsplit.Splitter{
			Reset:   true, // xxx ?
			MinSize: 1024, // xxx ?
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chunks := splitter.Split(ctx, r)
	chunks, errfn := hashsplit.Filter(chunks, func(chunk hashsplit.Chunk) (hashsplit.Chunk, error) {
		ref, _, err := s.Put(ctx, chunk.Bytes)
		if err != nil {
			return chunk, err
		}

		chunk2 := chunk
		chunk2.Bytes = ref[:]
		chunk2.Level /= 2 // xxx ?
		return chunk2, nil
	})

	root := hashsplit.Tree(chunks)
	if err := splitter.E; err != nil {
		return Zero, err
	}
	if err := errfn(); err != nil {
		return Zero, err
	}

	return splitWrite(ctx, s, root)
}

func splitWrite(ctx context.Context, s Store, n *hashsplit.Node) (Ref, error) {
	tn := &TreeNode{Size: n.Size}
	if len(n.Leaves) > 0 {
		tn.Leaves = n.Leaves
	} else {
		for _, child := range n.Nodes {
			childRef, err := splitWrite(ctx, s, child)
			if err != nil {
				return Zero, err
			}
			tn.Nodes = append(tn.Nodes, childRef[:])
		}
	}
	ref, _, err := PutProto(ctx, s, tn)
	return ref, err
}

func SplitRead(ctx context.Context, g Getter, ref Ref, w io.Writer) error {
	var tn TreeNode
	err := GetProto(ctx, g, ref, &tn)
	if err != nil {
		return err
	}
	return splitRead(ctx, g, &tn, w)
}

func splitRead(ctx context.Context, g Getter, n *TreeNode, w io.Writer) error {
	if len(n.Leaves) > 0 {
		return splitReadHelper(ctx, g, n.Leaves, func(m []byte) error {
			_, err := w.Write(m)
			return err
		})
	}
	return splitReadHelper(ctx, g, n.Nodes, func(m []byte) error {
		var tn TreeNode
		err := proto.Unmarshal(m, &tn)
		if err != nil {
			return err
		}
		return splitRead(ctx, g, &tn, w)
	})
}

func splitReadHelper(ctx context.Context, g Getter, subrefsBytes [][]byte, do func([]byte) error) error {
	subrefs := make([]Ref, len(subrefsBytes))
	for i, b := range subrefsBytes {
		copy(subrefs[i][:], b)
	}
	blobs, err := g.GetMulti(ctx, subrefs)
	if err != nil {
		return err
	}
	for _, subref := range subrefs {
		f := blobs[subref]
		m, err := f(ctx)
		if err != nil {
			return err
		}
		err = do(m)
		if err != nil {
			return err
		}
	}
	return nil
}
