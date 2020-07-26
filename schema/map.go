package schema

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sort"

	"github.com/bobg/bs"
)

func (m *Map) Set(ctx context.Context, store bs.Store, key []byte, ref bs.Ref) (bs.Ref, Outcome, error) {
	return treeSet(ctx, m, store, hashKey(key), func(t tree, i int32, insert bool) Outcome {
		m := t.(*Map)
		if insert {
			newMembers := make([]*MapPair, 1+len(m.Members))
			copy(newMembers[:i], m.Members[:i])
			newMembers[i] = &MapPair{
				Key:     key,
				Payload: ref[:],
			}
			copy(newMembers[i+1:], m.Members[i:])
			m.Members = newMembers
			return OAdded
		}
		if bytes.Equal(ref[:], m.Members[i].Payload) {
			return ONone
		}
		m.Members[i].Payload = ref[:]
		return OUpdated
	})
}

func (m *Map) treenode() *TreeNode    { return m.Node }
func (m *Map) numMembers() int32      { return int32(len(m.Members)) }
func (m *Map) keyHash(i int32) []byte { return hashKey(m.Members[i].Key) }
func (m *Map) zeroMembers()           { m.Members = nil }

func (m *Map) newAt(depth int32) tree {
	return &Map{
		Node: &TreeNode{
			Depth: depth,
		},
	}
}

func (m *Map) copyMember(other tree, i int32) {
	m.Members = append(m.Members, (other.(*Map)).Members[i])
	m.Node.Size++
}

func (m *Map) removeMember(i int32) {
	n := len(m.Members)
	copy(m.Members[i:], m.Members[i+1:])
	m.Members[n-1] = nil
	m.Members = m.Members[:n-1]
}

func (m *Map) sortMembers() {
	sort.Slice(m.Members, func(i, j int) bool {
		return bytes.Compare(hashKey(m.Members[i].Key), hashKey(m.Members[j].Key)) < 0
	})
}

func hashKey(k []byte) []byte {
	h := sha256.Sum256(k)
	return h[:]
}

func (m *Map) Lookup(ctx context.Context, g bs.Getter, key []byte) (bs.Ref, bool, error) {
	var (
		ok  bool
		ref bs.Ref
	)
	err := treeLookup(ctx, m, g, hashKey(key), func(t tree, i int32) {
		m := t.(*Map)
		ok = true
		ref = bs.RefFromBytes(m.Members[i].Payload)
	})
	return ref, ok, err
}

func (m *Map) Remove(ctx context.Context, store bs.Store, key []byte) (bs.Ref, bool, error) {
	return treeRemove(ctx, m, store, hashKey(key))
}

func (m *Map) Each(ctx context.Context, g bs.Getter, ch chan<- MapPair) error {
	return treeEach(ctx, m, g, func(t tree, i int32) error {
		m := t.(*Map)
		ch <- *(m.Members[i])
		return nil
	})
}
