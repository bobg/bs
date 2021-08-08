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

// The tree interface factors out common logic shared by Set and Map.
// Both are implemented as a binary tree.
// A node contains a sorted list of members, until it grows too large;
// then it is split into left and right children.
// If the depth of the node is D,
// the left child gets all the members whose "key hash" has a 0 in bit position D
// and the right child gets all the members whose key hash has a 1 in that position.
// On removing an element, subtrees are coalesced back into a single node if they fit.
// The goal is to make the shape of the tree (and its resulting Ref)
// insensitive to the order of addition and removal operations.
//
// In a Set, the members are Refs and the "key hash" is simply the Ref itself.
//
// In a Map, the members are pairs with arbitrary byte slices for both key and payload.
// The "key hash" is a hash of the key.
type tree interface {
	proto.Message
	copyMember(src tree, i int32)
	keyHash(int32) []byte
	newAt(int32) tree
	numMembers() int32
	removeMember(int32)
	sortMembers()
	treenode() *TreeNode
	zeroMembers()
}

const maxNode = 128

// Outcome is the outcome of a Map.Set operation.
type Outcome int

const (
	// ONone means no change was needed.
	ONone Outcome = iota

	// OAdded means a key was not present, and was added.
	OAdded

	// OUpdated means a key was present but its payload was changed.
	OUpdated
)

type treeItem interface {
	keyHash() []byte
	mutate(tree, int32, bool) Outcome
}

func treeFromGo(ctx context.Context, s bs.Store, items []treeItem, newAt func(int32) tree) (tree, bs.Ref, error) {
	sort.Slice(items, func(i, j int) bool {
		return bytes.Compare(items[i].keyHash(), items[j].keyHash()) < 0
	})
	return treeFromGoHelper(ctx, s, items, 0, newAt)
}

func treeFromGoHelper(ctx context.Context, s bs.Store, items []treeItem, level int32, newAt func(int32) tree) (tree, bs.Ref, error) {
	t := newAt(level)
	tn := t.treenode()
	tn.Size = int32(len(items))
	if len(items) <= maxNode {
		for i := 0; i < len(items); i++ {
			items[i].mutate(t, int32(i), true)
		}
	} else {
		var leftItems, rightItems []treeItem
		for _, item := range items {
			if nthbit(item.keyHash(), level) {
				rightItems = append(rightItems, item)
			} else {
				leftItems = append(leftItems, item)
			}
		}
		_, leftRef, err := treeFromGoHelper(ctx, s, leftItems, level+1, newAt)
		if err != nil {
			return nil, bs.Zero, errors.Wrap(err, "computing left node child")
		}
		_, rightRef, err := treeFromGoHelper(ctx, s, rightItems, level+1, newAt)
		if err != nil {
			return nil, bs.Zero, errors.Wrap(err, "computing right node child")
		}
		tn.Left = &SubNode{
			Ref:  leftRef[:],
			Size: int32(len(leftItems)),
		}
		tn.Right = &SubNode{
			Ref:  rightRef[:],
			Size: int32(len(rightItems)),
		}
	}
	ref, _, err := bs.PutProto(ctx, s, t)
	return t, ref, errors.Wrap(err, "storing tree node")
}

// Add or update an element to the tree.
// When the right position in the member list of the right node is found,
// mutate is called with the node, position, and a boolean: true for insertion,
// false for update-in-place.
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
			return bs.Zero, ONone, errors.Wrapf(err, "getting child %x at depth %d", subref, tn.Depth+1)
		}

		newSubref, outcome, err := treeSet(ctx, sub, store, keyHash, mutate)
		if err != nil {
			return bs.Zero, ONone, errors.Wrapf(err, "updating child %x at depth %d", subref, tn.Depth+1)
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
			return bs.Zero, ONone, errors.Wrap(err, "storing new left child after split")
		}
		rightRef, _, err := bs.PutProto(ctx, store, rightChild)
		if err != nil {
			return bs.Zero, ONone, errors.Wrap(err, "storing new right child after split")
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
			return bs.Zero, false, errors.Wrapf(err, "getting child %x at depth %d", subref, tn.Depth+1)
		}

		newSubref, removed, err := treeRemove(ctx, sub, store, keyhash)
		if err != nil {
			return bs.Zero, false, errors.Wrapf(err, "updating child %x at depth %d", subref, tn.Depth+1)
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
					return bs.Zero, false, errors.Wrapf(err, "getting left child %x at depth %d", tn.Left.Ref, tn.Depth+1)
				}
			} else {
				left = sub
				right = t.newAt(0)
				err = bs.GetProto(ctx, store, bs.RefFromBytes(tn.Right.Ref), right)
				if err != nil {
					return bs.Zero, false, errors.Wrapf(err, "getting right child %x at depth %d", tn.Right.Ref, tn.Depth+1)
				}
			}

			t2 := t.newAt(tn.Depth)
			err = treeEach(ctx, left, store, func(t3 tree, i int32) error {
				t2.copyMember(t3, i)
				return nil
			})
			if err != nil {
				return bs.Zero, false, errors.Wrapf(err, "iterating over members of left child %x", tn.Left.Ref)
			}
			err = treeEach(ctx, right, store, func(t3 tree, i int32) error {
				t2.copyMember(t3, i)
				return nil
			})
			if err != nil {
				return bs.Zero, false, errors.Wrapf(err, "iterating over members of right child %x", tn.Right.Ref)
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

// Calls f for each member of t (recursively) in keyhash order.
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
			return err
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
