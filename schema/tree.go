package schema

import (
	"bytes"
	"context"
	reflect "reflect"
	"sort"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs"
)

type tree interface {
	proto.Message
	treenode() *TreeNode
	numMembers() int32
	keyHash(int32) []byte
	zeroMembers()
	newAt(int32) tree
	copyMember(src tree, i int32)
	removeMember(int32)
	sortMembers()
}

const maxNode = 128

type Outcome int

const (
	ONone Outcome = iota
	OAdded
	OUpdated
)

func treeSet(ctx context.Context, t tree, store bs.Store, keyHash []byte, mutate func(tree, int32, bool) Outcome) (bs.Ref, Outcome, error) {
	tn := t.treenode()
	if tn.Left != nil {
		var subnode *SubNode
		if nthbit(keyHash, tn.Depth) {
			subnode = tn.Right
		} else {
			subnode = tn.Left
		}
		subref := subnode.Ref

		sub := t.newAt(0)
		err := bs.GetProto(ctx, store, bs.RefFromBytes(subref), sub)
		if err != nil {
			return bs.Ref{}, ONone, errors.Wrapf(err, "getting child %x at depth %d", subref, tn.Depth+1)
		}

		newSubref, outcome, err := treeSet(ctx, sub, store, keyHash, mutate)
		if err != nil {
			return bs.Ref{}, ONone, errors.Wrapf(err, "updating child %x at depth %d", subref, tn.Depth+1)
		}
		if outcome == ONone {
			selfRef, err := bs.ProtoRef(t)
			return selfRef, ONone, errors.Wrap(err, "computing self ref")
		}

		subnode.Ref = newSubref[:]
		if outcome == OAdded {
			subnode.Size++
			tn.Size++
		}

		selfRef, _, err := bs.PutProto(ctx, store, t)
		return selfRef, outcome, errors.Wrap(err, "storing updated object")
	}

	index := treeSearch(t, keyHash)
	if index < t.numMembers() && bytes.Equal(t.keyHash(index), keyHash) {
		outcome := mutate(t, index, false)
		if outcome == ONone {
			selfRef, err := bs.ProtoRef(t)
			return selfRef, ONone, errors.Wrap(err, "computing self ref")
		}
		selfRef, _, err := bs.PutProto(ctx, store, t)
		return selfRef, OUpdated, errors.Wrap(err, "storing updated tree node")
	}

	mutate(t, index, true)

	if t.numMembers() > maxNode {
		var (
			leftChild  = t.newAt(tn.Depth + 1)
			rightChild = t.newAt(tn.Depth + 1)
		)
		for i := int32(0); i < t.numMembers(); i++ {
			if nthbit(t.keyHash(i), tn.Depth) {
				rightChild.copyMember(t, i)
			} else {
				leftChild.copyMember(t, i)
			}
		}
		leftRef, _, err := bs.PutProto(ctx, store, leftChild)
		if err != nil {
			return bs.Ref{}, ONone, errors.Wrap(err, "storing new left child after split")
		}
		rightRef, _, err := bs.PutProto(ctx, store, rightChild)
		if err != nil {
			return bs.Ref{}, ONone, errors.Wrap(err, "storing new right child after split")
		}
		tn.Left = &SubNode{
			Ref:  leftRef[:],
			Size: leftChild.numMembers(),
		}
		tn.Right = &SubNode{
			Ref:  rightRef[:],
			Size: rightChild.numMembers(),
		}
		t.zeroMembers()
	}
	tn.Size++

	selfRef, _, err := bs.PutProto(ctx, store, t)
	return selfRef, OAdded, errors.Wrap(err, "storing updated tree node")
}

func treeSearch(t tree, keyHash []byte) int32 {
	return int32(sort.Search(int(t.numMembers()), func(i int) bool {
		return bytes.Compare(t.keyHash(int32(i)), keyHash) >= 0
	}))
}

func treeLookup(ctx context.Context, t tree, g bs.Getter, keyhash []byte, found func(tree, int32)) error {
	tn := t.treenode()
	if tn.Left != nil {
		var subref []byte
		if nthbit(keyhash, tn.Depth) {
			subref = tn.Right.Ref
		} else {
			subref = tn.Left.Ref
		}
		sub := t.newAt(0)
		err := bs.GetProto(ctx, g, bs.RefFromBytes(subref), sub)
		if err != nil {
			return errors.Wrapf(err, "getting child %x at depth %d", subref, tn.Depth+1)
		}
		return treeLookup(ctx, sub, g, keyhash, found)
	}

	index := treeSearch(t, keyhash)
	if index < t.numMembers() && bytes.Equal(t.keyHash(index), keyhash) {
		found(t, index)
	}
	return nil
}

func treeRemove(ctx context.Context, t tree, store bs.Store, keyhash []byte) (bs.Ref, bool, error) {
	tn := t.treenode()
	if tn.Left != nil {
		var (
			subnode *SubNode
			isRight bool
		)
		if nthbit(keyhash, tn.Depth) {
			subnode = tn.Right
			isRight = true
		} else {
			subnode = tn.Left
		}
		subref := subnode.Ref

		sub := t.newAt(0)
		err := bs.GetProto(ctx, store, bs.RefFromBytes(subref), sub)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "getting child %x at depth %d", subref, tn.Depth+1)
		}

		newSubref, removed, err := treeRemove(ctx, sub, store, keyhash)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "updating child %x at depth %d", subref, tn.Depth+1)
		}
		if !removed {
			selfRef, err := bs.ProtoRef(t)
			return selfRef, false, errors.Wrap(err, "computing self ref")
		}
		subnode.Ref = newSubref[:]
		subnode.Size--
		tn.Size--

		if tn.Size <= maxNode {
			// Collapse back down to a single node.

			var (
				left, right tree
			)
			if isRight {
				right = sub
				left = t.newAt(0)
				err = bs.GetProto(ctx, store, bs.RefFromBytes(tn.Left.Ref), left)
				if err != nil {
					return bs.Ref{}, false, errors.Wrapf(err, "getting left child %x at depth %d", tn.Left.Ref, tn.Depth+1)
				}
			} else {
				left = sub
				right = t.newAt(0)
				err = bs.GetProto(ctx, store, bs.RefFromBytes(tn.Right.Ref), right)
				if err != nil {
					return bs.Ref{}, false, errors.Wrapf(err, "getting right child %x at depth %d", tn.Right.Ref, tn.Depth+1)
				}
			}

			t2 := t.newAt(tn.Depth)
			err = treeEach(ctx, left, store, func(t3 tree, i int32) error {
				t2.copyMember(t3, i)
				return nil
			})
			if err != nil {
				return bs.Ref{}, false, errors.Wrapf(err, "iterating over members of left child %x", tn.Left.Ref)
			}
			err = treeEach(ctx, right, store, func(t3 tree, i int32) error {
				t2.copyMember(t3, i)
				return nil
			})
			if err != nil {
				return bs.Ref{}, false, errors.Wrapf(err, "iterating over members of right child %x", tn.Right.Ref)
			}
			t2.sortMembers()

			if reflect.TypeOf(t).Kind() == reflect.Ptr {
				// *t = *t2
				reflect.ValueOf(t).Elem().Set(reflect.ValueOf(t2).Elem())
			} else {
				// This case should be impossible.
				t = t2
			}
		}

		selfRef, _, err := bs.PutProto(ctx, store, t)
		return selfRef, true, errors.Wrap(err, "storing updated tree node")
	}

	index := treeSearch(t, keyhash)
	if index < t.numMembers() && bytes.Equal(t.keyHash(index), keyhash) {
		t.removeMember(index)
		tn.Size--
		// It's possible that t is now empty,
		// but we still store it
		// (rather than, say, return a sentinel value like the zero Ref)
		// to preserve the invariant (for now)
		// that any node with children has both children,
		// which simplifies the logic.
		selfRef, _, err := bs.PutProto(ctx, store, t)
		return selfRef, true, errors.Wrap(err, "storing updated tree node")
	}

	selfRef, err := bs.ProtoRef(t)
	return selfRef, false, errors.Wrap(err, "computing self ref")
}

func treeEach(ctx context.Context, t tree, g bs.Getter, f func(tree, int32) error) error {
	tn := t.treenode()
	if tn.Left != nil {
		sub := t.newAt(0)
		err := bs.GetProto(ctx, g, bs.RefFromBytes(tn.Left.Ref), sub)
		if err != nil {
			return errors.Wrapf(err, "getting left child %x at depth %d", tn.Left.Ref, tn.Depth+1)
		}
		err = treeEach(ctx, sub, g, f)
		if err != nil {
			return errors.Wrapf(err, "iterating over left child %x", tn.Left.Ref)
		}
		err = bs.GetProto(ctx, g, bs.RefFromBytes(tn.Right.Ref), sub)
		if err != nil {
			return errors.Wrapf(err, "getting right child %x at depth %d", tn.Right.Ref, tn.Depth+1)
		}
		err = treeEach(ctx, sub, g, f)
		return errors.Wrapf(err, "iterating over right child %x", tn.Right.Ref)
	}

	for i := int32(0); i < t.numMembers(); i++ {
		err := f(t, i)
		if err != nil {
			return errors.Wrapf(err, "operating on member %d at depth %d", i, tn.Depth)
		}
	}

	return nil
}

func nthbit(b []byte, n int32) bool {
	m := n % 8
	n /= 8
	if n >= int32(len(b)) {
		return false
	}
	return b[n]&(1<<m) != 0
}
