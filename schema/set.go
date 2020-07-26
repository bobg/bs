package schema

import (
	"bytes"
	"context"
	"sort"

	"github.com/bobg/bs"
)

func (s *Set) Add(ctx context.Context, store bs.Store, ref bs.Ref) (bs.Ref, bool, error) {
	newref, outcome, err := treeSet(ctx, s, store, ref[:], func(m tree, i int32, insert bool) Outcome {
		if !insert {
			return ONone
		}
		s := m.(*Set)
		newMembers := make([][]byte, 1+len(s.Members))
		copy(newMembers[:i], s.Members[:i])
		newMembers[i] = ref[:]
		copy(newMembers[i+1:], s.Members[i:])
		s.Members = newMembers
		return OAdded
	})
	return newref, outcome == OAdded, err
}

func (s *Set) treenode() *TreeNode    { return s.Node }
func (s *Set) numMembers() int32      { return int32(len(s.Members)) }
func (s *Set) keyHash(i int32) []byte { return s.Members[i] }
func (s *Set) zeroMembers()           { s.Members = nil }

func (s *Set) newAt(depth int32) tree {
	return &Set{
		Node: &TreeNode{
			Depth: depth,
		},
	}
}

func (s *Set) copyMember(other tree, i int32) {
	s.Members = append(s.Members, (other.(*Set)).Members[i])
	s.Node.Size++
}

func (s *Set) removeMember(i int32) {
	n := len(s.Members)
	copy(s.Members[i:], s.Members[i+1:])
	s.Members[n-1] = nil
	s.Members = s.Members[:n-1]
}

func (s *Set) sortMembers() {
	sort.Slice(s.Members, func(i, j int) bool {
		return bytes.Compare(s.Members[i], s.Members[j]) < 0
	})
}

// Check checks whether ref is in s.
func (s *Set) Check(ctx context.Context, g bs.Getter, ref bs.Ref) (bool, error) {
	var ok bool
	err := treeLookup(ctx, s, g, ref[:], func(tree, int32) {
		ok = true
	})
	return ok, err
}

// Remove removes ref from s.
// It returns the set's new root ref,
// and a boolean indicating whether the ref was removed.
// (If false, the ref was not present in the set, and the root ref is unchanged.)
func (s *Set) Remove(ctx context.Context, store bs.Store, ref bs.Ref) (bs.Ref, bool, error) {
	return treeRemove(ctx, s, store, ref[:])
}

// Each sends all the members of s on ch.
// It blocks until all members are sent,
// so you'll probably want a separate goroutine to consume ch.
func (s *Set) Each(ctx context.Context, g bs.Getter, ch chan<- bs.Ref) error {
	return treeEach(ctx, s, g, func(ml tree, i int32) error {
		m := ml.(*Set)
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ch <- bs.RefFromBytes(m.Members[i]):
			return nil
		}
	})
}
