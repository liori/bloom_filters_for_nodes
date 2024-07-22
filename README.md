Context: Storj is a service that aggregates unused storage of many, many people
and makes it available for commercial purposes.

Each storage node is identified by a 32-byte *node ID*. The basic unit of
content is a *piece*, identified by a 32-byte *piece ID*. Nodes store millions
of *pieces*. To save on API calls and other types of overhead, instead of
deleting every single *piece* separately, Storj sends to nodes a bloom filter
which defines a probabilistic bitmask of pieces that the node is supposed to
store, with the message that the node can remove any pieces that do not match
the bloom filter.

As of writing this document, the approach to construct bloom filters is to keep
bloom filters for all nodes in memory, iterate over content metadata and update
all relevant filters. This requires a lot of memory, and Storj has noted this is
a problem.

This repository is a proof-of-concept of a different approach to constructing
bloom filters, where the list of pieces is first sorted by nodes using radix
sort on external memory (files). Then, iterating over the sorted list, only a
single filter needs to be stored in memory.

I wrote this proof-of-concept in C++ to refresh my knowledge of C++. As it is
just a feasibility study, code is probably a bit substandard. Yet I believe that
it demonstrates that a faster approach to construct bloom filters in Storj is
possible, and pretty simple.

References:

  * [Bloom filter on Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter)
  * [My explanation of bloom filters in the context of Storj](https://forum.storj.io/t/how-bloom-filters-work/12104/7)
  * [Statement of problems with the current approach to constructing bloom filters by a Storj employee](https://forum.storj.io/t/current-situation-with-garbage-collection/25711/268)
  * [My post that presents this code](https://forum.storj.io/t/current-situation-with-garbage-collection/25711/274)
