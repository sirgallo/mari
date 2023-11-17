# Compaction


## Issue

Due to the design decision to use an append only memory map, there are a couple of downsides. While the append only nature is great for concurrency, the file will infinitely grow and the elements in the concurrent ordered array mapped trie will be spread across the memory map. Not only this, but as the file grows, the amount of unused nodes increases, resulting in wasted space.


## Solution

A compaction strategy is implemented, where after a certain number of versions, a compaction operation is employed.

A separate go routine runs the compaction strategy, and on signal, acquires the write lock, blocking all subsequent reads and writes. It then opens a temporary memory mapped file and recursively traverses the branches of the trie for the current root version. When the end of a branch is reached, the node is serialized directly to the temporary memory map at the computed offset, and then traverses back up the tree, serializing each parent node to the memory map until the root is reached. This bypasses the need to build the entire structure in memory and should scale reasonably well for large datasets. Once the nodes are completely serialized, the original memory mapped file is removed and the new memory mapped file is swapped in and all operations can continue. 

A benefit of compaction is that there will no longer be duplicated nodes for each version, reducing overall size of the structure and reducing the space that an operation may need to travel along the memory map to find a node. For iterators and range operations, nodes will be more localized as well reducing the need to load and evict data from the system cache.


## Notes

Additionally, a separate memory mapped file is kept for version indexes. Each version index contains the offset to that particular root. On compaction, the index is reset and the version is set back to 0. The version index can be utilized to query previously created versions within `mari`