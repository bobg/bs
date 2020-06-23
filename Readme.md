# BS, a content-addressable blob store

This is BS, an implementation of a content-addressable blob store.

A blob store stores arbitrarily sized sequences of bytes (“blobs”)
and indexes them by their hash, which is used as a unique key.
(The fact that the lookup key is derived from a blob’s content is the meaning of “content-addressable.”)
With a sufficiently good hash algorithm,
the likelihood of any two distinct blobs “colliding” is so small
that you’re better off worrying about spontaneously combusting
while also facing a thundering herd of angry rhinoceroses,
at the very moment of a solar eclipse.

This module uses sha2-256, which is a sufficiently good hash algorithm.

Subpackages of this module implement different versions of blob stores:
a memory-based one, a file-based one, a Postgresql-based one,
and a Google Cloud Storage one.
There is also a blob store that is an LRU cache for an underlying blob store.

BS is inspired by, and a simplification of,
[the Perkeep project](https://perkeep.org/),
which is presently dormant.
