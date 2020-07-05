package schema

import (
	"bytes"
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
)

const maxNode = 128

// Add adds a ref to s.
// It returns the new root ref for s,
// and a boolean indicating whether ref was added.
// (If false, ref was already present, and the root ref is unchanged.)
func (s *Set) Add(ctx context.Context, store bs.Store, ref bs.Ref) (bs.Ref, bool, error) {
	if s.Left != nil {
		var (
			left   bool
			subref []byte
		)

		if nthbit(ref[:], s.Depth) {
			subref = s.Right.Ref
		} else {
			left = true
			subref = s.Left.Ref
		}

		var sub Set
		err := bs.GetProto(ctx, store, bs.RefFromBytes(subref), &sub)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "getting subset %x", subref)
		}
		newSubref, added, err := sub.Add(ctx, store, ref)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "adding %s to subset %x", ref, subref)
		}
		if !added {
			selfRef, err := bs.ProtoRef(s)
			return selfRef, false, errors.Wrap(err, "computing self ref")
		}
		if left {
			s.Left.Ref = newSubref[:]
			s.Left.Size++
		} else {
			s.Right.Ref = newSubref[:]
			s.Right.Size++
		}
		s.Size++

		selfRef, _, err := bs.PutProto(ctx, store, s)
		return selfRef, true, err
	}

	index := s.search(ref)

	if index < len(s.Members) && bytes.Equal(s.Members[index], ref[:]) {
		// Ref already present in this set.
		selfRef, err := bs.ProtoRef(s)
		return selfRef, false, errors.Wrap(err, "computing self ref")
	}

	// Ref not present; add it at index.
	var (
		before = s.Members[:index]
		after  = s.Members[index:]
	)

	s.Members = append([][]byte{}, before...)
	s.Members = append(s.Members, ref[:])
	s.Members = append(s.Members, after...)

	if len(s.Members) > maxNode {
		var leftMembers, rightMembers [][]byte

		for _, member := range s.Members {
			if nthbit(member, s.Depth) {
				rightMembers = append(rightMembers, member)
			} else {
				leftMembers = append(leftMembers, member)
			}
		}

		var (
			leftSet  = &Set{Depth: s.Depth + 1, Members: leftMembers, Size: int32(len(leftMembers))}
			rightSet = &Set{Depth: s.Depth + 1, Members: rightMembers, Size: int32(len(rightMembers))}
		)

		leftRef, _, err := bs.PutProto(ctx, store, leftSet)
		if err != nil {
			return bs.Ref{}, false, errors.Wrap(err, "storing new left subtree")
		}
		rightRef, _, err := bs.PutProto(ctx, store, rightSet)
		if err != nil {
			return bs.Ref{}, false, errors.Wrap(err, "storing new right subtree")
		}

		*s = Set{
			Depth: s.Depth,
			Left: &Subset{
				Ref:  leftRef[:],
				Size: leftSet.Size,
			},
			Right: &Subset{
				Ref:  rightRef[:],
				Size: rightSet.Size,
			},
			Size: leftSet.Size + rightSet.Size,
		}
	} else {
		s.Size++
	}

	selfRef, _, err := bs.PutProto(ctx, store, s)
	return selfRef, true, errors.Wrap(err, "storing updated set proto")
}

func (s *Set) search(ref bs.Ref) int {
	return sort.Search(len(s.Members), func(i int) bool {
		return bytes.Compare(s.Members[i], ref[:]) >= 0
	})
}

// Check checks whether ref is in s.
func (s *Set) Check(ctx context.Context, g bs.Getter, ref bs.Ref) (bool, error) {
	if s.Left != nil {
		var subref []byte
		if nthbit(ref[:], s.Depth) {
			subref = s.Right.Ref
		} else {
			subref = s.Left.Ref
		}
		var sub Set
		err := bs.GetProto(ctx, g, bs.RefFromBytes(subref), &sub)
		if err != nil {
			return false, errors.Wrapf(err, "getting left subset %x", s.Left.Ref)
		}
		return sub.Check(ctx, g, ref)
	}

	index := s.search(ref)
	if index == len(s.Members) {
		return false, nil
	}
	return bytes.Equal(s.Members[index], ref[:]), nil
}

// Remove removes ref from s.
// It returns the set's new root ref,
// and a boolean indicating whether the ref was removed.
// (If false, the ref was not present in the set, and the root ref is unchanged.)
func (s *Set) Remove(ctx context.Context, store bs.Store, ref bs.Ref) (bs.Ref, bool, error) {
	if s.Left != nil {
		var (
			isRight = nthbit(ref[:], s.Depth)
			subref  []byte
		)
		if isRight {
			subref = s.Right.Ref
		} else {
			subref = s.Left.Ref
		}

		var sub Set
		err := bs.GetProto(ctx, store, bs.RefFromBytes(subref), &sub)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "getting subset %x", subref)
		}

		newSubref, removed, err := sub.Remove(ctx, store, ref)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "removing %s from subset %x", ref, subref)
		}
		if !removed {
			selfRef, err := bs.ProtoRef(s)
			return selfRef, false, errors.Wrap(err, "computing self ref")
		}
		s.Size--
		if newSubref == (bs.Ref{}) || s.Size <= maxNode {
			// A subtree evanesced or the overall number of children is small enough;
			// coalesce the subtrees in this node.
			var (
				otherRef []byte
				other    Set
				leftSet  *Set
				rightSet *Set
			)
			if isRight {
				otherRef = s.Left.Ref
				leftSet = &other
				rightSet = &sub
			} else {
				otherRef = s.Right.Ref
				leftSet = &sub
				rightSet = &other
			}
			err = bs.GetProto(ctx, store, bs.RefFromBytes(otherRef), &other)
			if err != nil {
				return bs.Ref{}, false, errors.Wrapf(err, "getting other subset %x", otherRef)
			}

			newMembers := append(leftSet.Members, rightSet.Members...)
			sort.Slice(newMembers, func(i, j int) bool { return bytes.Compare(newMembers[i], newMembers[j]) < 0 })

			s.Left = nil
			s.Right = nil
			s.Members = newMembers
		} else {
			if isRight {
				s.Right.Ref = newSubref[:]
				s.Right.Size--
			} else {
				s.Left.Ref = newSubref[:]
				s.Left.Size--
			}
		}
	} else {
		index := s.search(ref)
		if index == len(s.Members) {
			selfRef, err := bs.ProtoRef(s)
			return selfRef, false, errors.Wrap(err, "computing self ref")
		}
		var (
			before = s.Members[:index]
			after  = s.Members[index+1:]
		)
		newMembers := append([][]byte{}, before...)
		newMembers = append(newMembers, after...)
		s.Members = newMembers
		s.Size--
	}

	selfRef, _, err := bs.PutProto(ctx, store, s)
	return selfRef, true, errors.Wrap(err, "storing updated set proto")
}

// Each sends all the members of s on ch.
func (s *Set) Each(ctx context.Context, g bs.Getter, ch chan<- bs.Ref) error {
	if s.Left != nil {
		var sub Set
		err := bs.GetProto(ctx, g, bs.RefFromBytes(s.Left.Ref), &sub)
		if err != nil {
			return errors.Wrapf(err, "getting left subset %x", s.Left.Ref)
		}
		err = sub.Each(ctx, g, ch)
		if err != nil {
			return errors.Wrapf(err, "iterating over left subset %x", s.Left.Ref)
		}
		err = bs.GetProto(ctx, g, bs.RefFromBytes(s.Right.Ref), &sub)
		if err != nil {
			return errors.Wrapf(err, "getting right subset %x", s.Right.Ref)
		}
		err = sub.Each(ctx, g, ch)
		return errors.Wrapf(err, "right subset %x", s.Right.Ref)
	}

	for _, m := range s.Members {
		ch <- bs.RefFromBytes(m)
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
