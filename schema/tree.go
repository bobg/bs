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

type maplike interface {
	proto.Message
	treenode() *TreeNode
	numMembers() int32
	keyHash(int32) []byte
	zeroMembers()
	newAt(int32) maplike
	copyMember(src maplike, i int32)
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

func maplikeSet(ctx context.Context, m maplike, store bs.Store, keyHash []byte, mutate func(maplike, int32, bool) Outcome) (bs.Ref, Outcome, error) {
	tn := m.treenode()
	if tn.Left != nil {
		var subnode *SubNode
		if nthbit(keyHash, tn.Depth) {
			subnode = tn.Right
		} else {
			subnode = tn.Left
		}
		subref := subnode.Ref

		sub := m.newAt(0)
		err := bs.GetProto(ctx, store, bs.RefFromBytes(subref), sub)
		if err != nil {
			return bs.Ref{}, ONone, errors.Wrapf(err, "getting child %x at depth %d", subref, tn.Depth+1)
		}

		newSubref, outcome, err := maplikeSet(ctx, sub, store, keyHash, mutate)
		if err != nil {
			return bs.Ref{}, ONone, errors.Wrapf(err, "updating child %x at depth %d", subref, tn.Depth+1)
		}
		if outcome == ONone {
			selfRef, err := bs.ProtoRef(m)
			return selfRef, ONone, errors.Wrap(err, "computing self ref")
		}

		subnode.Ref = newSubref[:]
		if outcome == OAdded {
			subnode.Size++
			tn.Size++
		}

		selfRef, _, err := bs.PutProto(ctx, store, m)
		return selfRef, outcome, errors.Wrap(err, "storing updated object")
	}

	index := maplikeSearch(m, keyHash)
	if index < m.numMembers() && bytes.Equal(m.keyHash(index), keyHash) {
		outcome := mutate(m, index, false)
		if outcome == ONone {
			selfRef, err := bs.ProtoRef(m)
			return selfRef, ONone, errors.Wrap(err, "computing self ref")
		}
		selfRef, _, err := bs.PutProto(ctx, store, m)
		return selfRef, OUpdated, errors.Wrap(err, "storing updated tree node")
	}

	mutate(m, index, true)

	if m.numMembers() > maxNode {
		var (
			leftChild  = m.newAt(tn.Depth + 1)
			rightChild = m.newAt(tn.Depth + 1)
		)
		for i := int32(0); i < m.numMembers(); i++ {
			if nthbit(m.keyHash(i), tn.Depth) {
				rightChild.copyMember(m, i)
			} else {
				leftChild.copyMember(m, i)
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
		m.zeroMembers()
	}
	tn.Size++

	selfRef, _, err := bs.PutProto(ctx, store, m)
	return selfRef, OAdded, errors.Wrap(err, "storing updated tree node")
}

func maplikeSearch(m maplike, keyHash []byte) int32 {
	return int32(sort.Search(int(m.numMembers()), func(i int) bool {
		return bytes.Compare(m.keyHash(int32(i)), keyHash) >= 0
	}))
}

func maplikeLookup(ctx context.Context, m maplike, g bs.Getter, keyhash []byte, found func(maplike, int32)) error {
	tn := m.treenode()
	if tn.Left != nil {
		var subref []byte
		if nthbit(keyhash, tn.Depth) {
			subref = tn.Right.Ref
		} else {
			subref = tn.Left.Ref
		}
		sub := m.newAt(0)
		err := bs.GetProto(ctx, g, bs.RefFromBytes(subref), sub)
		if err != nil {
			return errors.Wrapf(err, "getting child %x at depth %d", subref, tn.Depth+1)
		}
		return maplikeLookup(ctx, sub, g, keyhash, found)
	}

	index := maplikeSearch(m, keyhash)
	if index < m.numMembers() && bytes.Equal(m.keyHash(index), keyhash) {
		found(m, index)
	}
	return nil
}

func maplikeRemove(ctx context.Context, m maplike, store bs.Store, keyhash []byte) (bs.Ref, bool, error) {
	tn := m.treenode()
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

		sub := m.newAt(0)
		err := bs.GetProto(ctx, store, bs.RefFromBytes(subref), sub)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "getting child %x at depth %d", subref, tn.Depth+1)
		}

		newSubref, removed, err := maplikeRemove(ctx, sub, store, keyhash)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "updating child %x at depth %d", subref, tn.Depth+1)
		}
		if !removed {
			selfRef, err := bs.ProtoRef(m)
			return selfRef, false, errors.Wrap(err, "computing self ref")
		}
		subnode.Ref = newSubref[:]
		subnode.Size--
		tn.Size--

		if tn.Size <= maxNode {
			// Collapse back down to a single node.

			var (
				left, right maplike
			)
			if isRight {
				right = sub
				left = m.newAt(0)
				err = bs.GetProto(ctx, store, bs.RefFromBytes(tn.Left.Ref), left)
				if err != nil {
					return bs.Ref{}, false, errors.Wrapf(err, "getting left child %x at depth %d", tn.Left.Ref, tn.Depth+1)
				}
			} else {
				left = sub
				right = m.newAt(0)
				err = bs.GetProto(ctx, store, bs.RefFromBytes(tn.Right.Ref), right)
				if err != nil {
					return bs.Ref{}, false, errors.Wrapf(err, "getting right child %x at depth %d", tn.Right.Ref, tn.Depth+1)
				}
			}

			m2 := m.newAt(tn.Depth)
			err = maplikeEach(ctx, left, store, func(m3 maplike, i int32) error {
				m2.copyMember(m3, i)
				return nil
			})
			if err != nil {
				return bs.Ref{}, false, errors.Wrapf(err, "iterating over members of left child %x", tn.Left.Ref)
			}
			err = maplikeEach(ctx, right, store, func(m3 maplike, i int32) error {
				m2.copyMember(m3, i)
				return nil
			})
			if err != nil {
				return bs.Ref{}, false, errors.Wrapf(err, "iterating over members of right child %x", tn.Right.Ref)
			}
			m2.sortMembers()

			if reflect.TypeOf(m).Kind() == reflect.Ptr {
				// *m = *m2
				reflect.ValueOf(m).Elem().Set(reflect.ValueOf(m2).Elem())
			} else {
				// This case should be impossible.
				m = m2
			}
		}

		selfRef, _, err := bs.PutProto(ctx, store, m)
		return selfRef, true, errors.Wrap(err, "storing updated tree node")
	}

	index := maplikeSearch(m, keyhash)
	if index < m.numMembers() && bytes.Equal(m.keyHash(index), keyhash) {
		m.removeMember(index)
		tn.Size--
		// It's possible that m is now empty,
		// but we still store it
		// (rather than, say, return a sentinel value like the zero Ref)
		// to preserve the invariant (for now)
		// that any node with children has both children,
		// which simplifies the logic.
		selfRef, _, err := bs.PutProto(ctx, store, m)
		return selfRef, true, errors.Wrap(err, "storing updated tree node")
	}

	selfRef, err := bs.ProtoRef(m)
	return selfRef, false, errors.Wrap(err, "computing self ref")
}

func maplikeEach(ctx context.Context, m maplike, g bs.Getter, f func(maplike, int32) error) error {
	tn := m.treenode()
	if tn.Left != nil {
		sub := m.newAt(0)
		err := bs.GetProto(ctx, g, bs.RefFromBytes(tn.Left.Ref), sub)
		if err != nil {
			return errors.Wrapf(err, "getting left child %x at depth %d", tn.Left.Ref, tn.Depth+1)
		}
		err = maplikeEach(ctx, sub, g, f)
		if err != nil {
			return errors.Wrapf(err, "iterating over left child %x", tn.Left.Ref)
		}
		err = bs.GetProto(ctx, g, bs.RefFromBytes(tn.Right.Ref), sub)
		if err != nil {
			return errors.Wrapf(err, "getting right child %x at depth %d", tn.Right.Ref, tn.Depth+1)
		}
		err = maplikeEach(ctx, sub, g, f)
		return errors.Wrapf(err, "iterating over right child %x", tn.Right.Ref)
	}

	for i := int32(0); i < m.numMembers(); i++ {
		err := f(m, i)
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
