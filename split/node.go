package split

import (
	"context"

	"github.com/bobg/hashsplit"

	"github.com/bobg/bs"
)

var _ hashsplit.Node = (*nodeWrapper)(nil)

type nodeWrapper struct {
	node *Node
	ctx  context.Context
	st   bs.Store
}

func (n *nodeWrapper) Offset() uint64   { return n.node.Offset }
func (n *nodeWrapper) Size() uint64     { return n.node.Size }
func (n *nodeWrapper) NumChildren() int { return len(n.node.Nodes) }

func (n *nodeWrapper) Child(i int) (hashsplit.Node, error) {
	var (
		node Node
		ref  = bs.RefFromBytes(n.node.Nodes[i].Ref)
	)
	err := bs.GetProto(n.ctx, n.st, ref, &node)
	return &nodeWrapper{
		node: &node,
		ctx:  n.ctx,
		st:   n.st,
	}, err
}
