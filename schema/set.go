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
			cmp    = bytes.Compare(ref[:], s.Right.Min)
		)

		if cmp == 0 {
			selfRef, err := bs.ProtoRef(s)
			return selfRef, false, errors.Wrap(err, "computing self ref")
		}
		if cmp < 0 {
			left = true
			subref = s.Left.Ref
		} else {
			subref = s.Right.Ref
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
			if bytes.Compare(ref[:], s.Left.Min) < 0 {
				s.Left.Min = ref[:]
			}
		} else {
			s.Right.Ref = newSubref[:]
			s.Right.Size++
			if bytes.Compare(ref[:], s.Right.Min) < 0 {
				s.Right.Min = ref[:]
			}
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

	if len(s.Members) >= maxNode {
		var (
			mid = len(s.Members) / 2
			m1  = s.Members[:mid]
			m2  = s.Members[mid:]
			s1  = &Set{Members: m1, Size: int32(len(m1))}
			s2  = &Set{Members: m2, Size: int32(len(m2))}
		)

		r1, _, err := bs.PutProto(ctx, store, s1)
		if err != nil {
			return bs.Ref{}, false, errors.Wrap(err, "storing new left subtree")
		}
		r2, _, err := bs.PutProto(ctx, store, s2)
		if err != nil {
			return bs.Ref{}, false, errors.Wrap(err, "storing new right subtree")
		}

		*s = Set{
			Left: &Subset{
				Ref:  r1[:],
				Min:  m1[0],
				Size: int32(len(m1)),
			},
			Right: &Subset{
				Ref:  r2[:],
				Min:  m2[0],
				Size: int32(len(m2)),
			},
			Size: int32(len(s.Members)),
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
		var sub Set
		err := bs.GetProto(ctx, g, bs.RefFromBytes(s.Left.Ref), &sub)
		if err != nil {
			return false, errors.Wrapf(err, "getting left subset %x", s.Left.Ref)
		}
		ok, err := sub.Check(ctx, g, ref)
		if err != nil || ok {
			return ok, errors.Wrapf(err, "checking left subset %x", s.Left.Ref)
		}
		err = bs.GetProto(ctx, g, bs.RefFromBytes(s.Right.Ref), &sub)
		if err != nil {
			return false, errors.Wrapf(err, "getting right subset %x", s.Right.Ref)
		}
		ok, err = sub.Check(ctx, g, ref)
		return ok, errors.Wrapf(err, "checking right subset %x", s.Right.Ref)
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
	selfRef, _, removed, err := s.remove(ctx, store, ref)
	return selfRef, removed, err
}

func (s *Set) remove(ctx context.Context, store bs.Store, ref bs.Ref) (newRef, newMin bs.Ref, removed bool, err error) {
	if s.Left != nil {
		var (
			left   bool
			subref []byte
		)
		if bytes.Compare(ref[:], s.Right.Min) >= 0 {
			subref = s.Right.Ref
		} else {
			subref = s.Left.Ref
			left = true
		}
		var sub Set
		err := bs.GetProto(ctx, store, bs.RefFromBytes(subref), &sub)
		if err != nil {
			return bs.Ref{}, bs.Ref{}, false, errors.Wrapf(err, "getting subset %x", subref)
		}

		// Note: newMin is only valid when removed is true.
		newSubref, newMin, removed, err := sub.remove(ctx, store, ref)
		if err != nil {
			return bs.Ref{}, bs.Ref{}, false, errors.Wrapf(err, "removing %s from subset %x", ref, subref)
		}
		if !removed {
			return ref, bs.Ref{}, false, nil
		}
		if newSubref == (bs.Ref{}) {
			// Left or right subtree evanesced.
			// Return the other subtree in place of s.
			if left {
				return bs.RefFromBytes(s.Right.Ref), bs.RefFromBytes(s.Right.Min), true, nil // sic
			}
			return bs.RefFromBytes(s.Left.Ref), bs.RefFromBytes(s.Left.Min), true, nil
		}
		if left {
			s.Left.Ref = newSubref[:]
			s.Left.Min = newMin[:]
			s.Left.Size--
		} else {
			s.Right.Ref = newSubref[:]
			s.Right.Min = newMin[:]
			s.Right.Size--
		}
		s.Size--
		ref, _, err = bs.PutProto(ctx, store, s)
		return ref, bs.RefFromBytes(s.Left.Min), true, err
	}

	index := s.search(ref)
	if index == len(s.Members) {
		return ref, bs.Ref{}, false, nil
	}
	if !bytes.Equal(s.Members[index], ref[:]) {
		return ref, bs.Ref{}, false, nil
	}
	if len(s.Members) == 1 {
		// Removing last member.
		return bs.Ref{}, bs.Ref{}, true, nil
	}

	before := s.Members[:index]
	after := s.Members[index+1:]
	s.Members = append([][]byte{}, before...)
	s.Members = append(s.Members, after...)
	s.Size--

	ref, _, err = bs.PutProto(ctx, store, s)
	return ref, bs.RefFromBytes(s.Members[0]), true, errors.Wrap(err, "storing updated proto")
}

// Each sends all the members of s on ch, in order.
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
