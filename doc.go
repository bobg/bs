// Package bs is a content-addressable blob store.
//
// A blob store stores arbitrarily sized sequences of bytes,
// or _blobs_,
// and indexes them by their hash,
// which is used as a unique key.
// This key is called the blob’s reference, or _ref_.
//
// With a sufficiently good hash algorithm,
// the likelihood of any two distinct blobs “colliding” is so small
// that you’re better off worrying about the much-more-likely danger of spontaneously combusting
// while also facing a thundering herd of angry rhinoceroses,
// at the very moment of a solar eclipse.
//
// This module uses sha2-256,
// which is a sufficiently good hash algorithm.
//
// The fact that the lookup key is computed from a blob’s content,
// rather than by its location in memory or the order in which it was added,
// is the meaning of “content-addressable.”
//
// Content addressability has some desirable properties,
// but it does mean that if some data changes,
// so does its ref,
// which can make it tricky to keep track of a piece of data over its lifetime.
// So in addition to a plain blob store,
// this module provides an "anchor" store.
// An anchor is a structured blob containing a name,
// a timestamp,
// and a blob ref.
// You can give a blob a name
// (such as a filename)
// by storing an anchor pointing to the blob’s ref.
// As the data changes,
// you can store new anchors with the same name but an updated ref and timestamp.
// An anchor store lets you retrieve the latest ref for a given name as of a given timestamp.
//
// Blob stores work best when blobs are not too big,
// so when storing potentially large bytestreams,
// use the split.Write function
// (in the split subpackage).
// This splits the input into multiple blobs organized as a tree,
// and it returns the ref of the tree’s root.
// The bytestream can be reassembled with split.Read.
//
// BS is inspired by,
// and a simplification of,
// the Perkeep project
// (https://perkeep.org/),
// which is presently dormant.
package bs
