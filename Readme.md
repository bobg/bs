# BS, a content-addressable blob store

[![GoDoc](https://godoc.org/github.com/bobg/bs?status.svg)](https://godoc.org/github.com/bobg/bs)
[![Go Report Card](https://goreportcard.com/badge/github.com/bobg/bs)](https://goreportcard.com/report/github.com/bobg/bs)

This is BS, an implementation of a content-addressable blob store.

A blob store stores arbitrarily sized sequences of bytes,
or _blobs_,
and indexes them by their hash,
which is used as a unique key.
This key is called the blob’s reference, or _ref_.

With a sufficiently good hash algorithm,
the likelihood of any two distinct blobs “colliding” is so small
that you’re better off worrying about spontaneously combusting
while also facing a thundering herd of angry rhinoceroses,
at the very moment of a solar eclipse.

This module uses sha2-256, which is a sufficiently good hash algorithm.

The fact that the lookup key is computed from a blob’s content,
rather than by its location in memory or the order in which it was added,
is the meaning of “content-addressable.”

Content addressability has some desirable properties,
but it does mean that if some data changes,
so does its ref,
which can make it tricky to keep track of a piece of data over its lifetime.
So a BS blob store also stores _anchors_,
which maps an arbitrary string
(such as a filename)
to a ref and a timestamp.
At a later timestamp,
the same anchor may map to a different ref.

BS works best when blobs are not too big,
so when storing potentially large bytestreams,
use the SplitWrite function.
This splits the input into multiple blobs organized as a tree,
and it returns the ref of the tree’s root.
The bytestream can be reassembled with SplitRead.

When splitting,
blob boundaries are determined not by position or size but by content,
using the technique of _hashsplitting_.
This same technique is used by git and rsync to represent file changes very compactly:
if two versions of a file have a small difference,
only the blob containing the difference is affected.
The other blobs of the file are unchanged.
This is the same reason that the blobs are organized into a tree.
If the blobs were organized as a list,
the whole list would have to change any time a blob is added, removed, or replaced.
But as a tree, only the subtree with the affected blob has to change.

Subpackages of this module implement different versions of blob stores:
a memory-based one, a file-based one, a Postgresql-based one,
and a Google Cloud Storage one.
There is also a blob store that is an LRU cache for an underlying blob store.

BS is inspired by, and a simplification of,
[the Perkeep project](https://perkeep.org/),
which is presently dormant.
