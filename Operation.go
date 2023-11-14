package mari

import "bytes"
import "runtime"
import "sync/atomic"
import "unsafe"


//============================================= Mari Operations


// Put inserts or updates key-value pair into the ordered array mapped trie.
//	The operation begins at the root of the trie and traverses through the tree until the correct location is found, copying the entire path.
//	If the operation fails, the copied and modified path is discarded and the operation retries back at the root until completed.
//	The operation begins at the latest known version of root, read from the metadata in the memory map. The version of the copy is incremented
//	and if the metadata is the same after the path copying has occured, the path is serialized and appended to the memory-map, with the metadata
//	also being updated to reflect the new version and the new root offset.
func (mariInst *Mari) Put(key, value []byte) (bool, error) {
	for {
		for atomic.LoadUint32(&mariInst.IsResizing) == 1 { runtime.Gosched() }
		mariInst.RWResizeLock.RLock()

		versionPtr, version, loadVErr := mariInst.loadMetaVersion()
		if loadVErr != nil { return false, loadVErr }

		if version == atomic.LoadUint64(versionPtr) {
			_, rootOffset, loadROffErr := mariInst.loadMetaRootOffset()
			if loadROffErr != nil { return false, loadROffErr }
	
			currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
			if readRootErr != nil {
				mariInst.RWResizeLock.RUnlock()
				return false, readRootErr
			}
	
			currRoot.Version = currRoot.Version + 1
			rootPtr := storeINodeAsPointer(currRoot)
			_, putErr := mariInst.putRecursive(rootPtr, key, value, 0)
			if putErr != nil {
				mariInst.RWResizeLock.RUnlock()
				return false, putErr
			}

			updatedRootCopy := loadINodeFromPointer(rootPtr)
			ok, writeErr := mariInst.exclusiveWriteMmap(updatedRootCopy)
			if writeErr != nil {
				mariInst.RWResizeLock.RUnlock()
				return false, writeErr
			}

			if ok {
				mariInst.RWResizeLock.RUnlock() 
				return true, nil 
			}
		}

		mariInst.RWResizeLock.RUnlock()
		runtime.Gosched()
	}
}

// putRecursive
//	Attempts to traverse through the trie, locating the node at a given level to modify for the key-value pair.
//	It first hashes the key, determines the sparse index in the bitmap to modify, and creates a copy of the current node to be modified.
//	If the bit in the bitmap of the node is not set, a new leaf node is created, the bitmap of the copy is modified to reflect the position of the new leaf node, and the child node array is extended to include the new leaf node.
//	Then, an atomic compare and swap operation is performed where the operation attempts to replace the current node with the modified copy.
//	If the operation succeeds the response is returned by moving back up the tree. If it fails, the copy is discarded and the operation returns to the root to be reattempted.
//	If the current bit is set in the bitmap, the operation checks if the node at the location in the child node array is a leaf node or an internal node.
//	If it is a leaf node and the key is the same as the incoming key, the copy is modified with the new value and we attempt to compare and swap the current child leaf node with the new copy.
//	If the leaf node does not contain the same key, the operation creates a new internal node, and inserts the new leaf node for the incoming key and value as well as the existing child node into the new internal node.
//	Attempts to compare and swap the current leaf node with the new internal node containing the existing child node and the new leaf node for the incoming key and value.
//	If the node is an internal node, the operation traverses down the tree to the internal node and the above steps are repeated until the key-value pair is inserted.
func (mariInst *Mari) putRecursive(node *unsafe.Pointer, key, value []byte, level int) (bool, error) {
	var putErr error

	currNode := loadINodeFromPointer(node)
	nodeCopy := mariInst.copyINode(currNode)
	nodeCopy.Leaf.Version = nodeCopy.Version

	putNewINode := func(node *MariINode, currIdx byte, uKey, uVal []byte) (*MariINode, error) {
		node.Bitmap = setBit(node.Bitmap, currIdx)
		pos := mariInst.getPosition(node.Bitmap, currIdx, level)

		newINode := mariInst.newInternalNode(node.Version)
		iNodePtr := storeINodeAsPointer(newINode)
		_, putINodeErr := mariInst.putRecursive(iNodePtr, uKey, uVal, level + 1)
		if putINodeErr != nil { return nil, putINodeErr }

		updatedINode:= loadINodeFromPointer(iNodePtr)
		node.Children = extendTable(node.Children, node.Bitmap, pos, updatedINode)

		return node, nil
	}

	if len(key) == level {
		switch {
			case bytes.Equal(nodeCopy.Leaf.Key, key):
				if ! bytes.Equal(nodeCopy.Leaf.Value, value) { nodeCopy.Leaf = mariInst.newLeafNode(key, value, nodeCopy.Version) }
			default:
				currentLeaf := nodeCopy.Leaf
				nodeCopy.Leaf = mariInst.newLeafNode(key, value, nodeCopy.Version)

				if len(currentLeaf.Key) > len(key) {
					idx := getIndexForLevel(currentLeaf.Key, level)

					if ! isBitSet(nodeCopy.Bitmap, idx) { 
						nodeCopy, putErr = putNewINode(nodeCopy, idx, currentLeaf.Key, currentLeaf.Value)
						if putErr != nil { return false, putErr }
					}
				}
		}
	} else {
		index := getIndexForLevel(key, level)
		
		switch {
			case ! isBitSet(nodeCopy.Bitmap, index):
				if level > 0 {
					popCount := populationCount(nodeCopy.Bitmap)
					currentLeaf := nodeCopy.Leaf

					switch {
						case bytes.Equal(currentLeaf.Key, key):
							if ! bytes.Equal(currentLeaf.Value, value) { nodeCopy.Leaf = mariInst.newLeafNode(key, value, nodeCopy.Version) }
						case len(currentLeaf.Key) == 0 && popCount == 0:
							nodeCopy.Leaf = mariInst.newLeafNode(key, value, nodeCopy.Version)
						case len(currentLeaf.Key) == 0 && popCount > 0:
							nodeCopy, putErr = putNewINode(nodeCopy, index, key, value)
							if putErr != nil { return false, putErr }
						default:
							switch {
								case len(key) > len(currentLeaf.Key) && len(currentLeaf.Key) > 0:
									nodeCopy, putErr = putNewINode(nodeCopy, index, key, value)
									if putErr != nil { return false, putErr }
								case len(currentLeaf.Key) > len(key):
									nodeCopy.Leaf = mariInst.newLeafNode(key, value, nodeCopy.Version)
									newIdx := getIndexForLevel(currentLeaf.Key, level)
									
									if ! isBitSet(nodeCopy.Bitmap, newIdx) {
										nodeCopy, putErr = putNewINode(nodeCopy, newIdx, currentLeaf.Key, currentLeaf.Value)
										if putErr != nil { return false, putErr }
									}
								default:
									nodeCopy.Leaf = mariInst.newLeafNode(nil, nil, nodeCopy.Version)

									nodeCopy, putErr = putNewINode(nodeCopy, index, key, value)
									if putErr != nil { return false, putErr }
		
									newIdx := getIndexForLevel(currentLeaf.Key, level)

									if ! isBitSet(nodeCopy.Bitmap, newIdx) {
										nodeCopy, putErr = putNewINode(nodeCopy, newIdx, currentLeaf.Key, currentLeaf.Value)
										if putErr != nil { return false, putErr }
									} else {
										newPos := mariInst.getPosition(nodeCopy.Bitmap, newIdx, level)
										
										childOffset := nodeCopy.Children[newPos]
										childNode, getChildErr := mariInst.getChildNode(childOffset, nodeCopy.Version)
										if getChildErr != nil { return false, getChildErr }
							
										childNode.Version = nodeCopy.Version
										childPtr := storeINodeAsPointer(childNode)
										_, putErr = mariInst.putRecursive(childPtr, currentLeaf.Key, currentLeaf.Value, level + 1)
										if putErr != nil { return false, putErr }

										updatedCNode := loadINodeFromPointer(childPtr)
										nodeCopy.Children[newPos] = updatedCNode
									}
							}
					}
				} else {
					nodeCopy, putErr = putNewINode(nodeCopy, index, key, value)
					if putErr != nil { return false, putErr }
				}
			default:
				pos := mariInst.getPosition(nodeCopy.Bitmap, index, level)

				childOffset := nodeCopy.Children[pos]
				childNode, getChildErr := mariInst.getChildNode(childOffset, nodeCopy.Version)
				if getChildErr != nil { return false, getChildErr }
	
				childNode.Version = nodeCopy.Version
				childPtr := storeINodeAsPointer(childNode)
	
				_, putErr = mariInst.putRecursive(childPtr, key, value, level + 1)
				if putErr != nil { return false, putErr }
	
				nodeCopy.Children[pos] = loadINodeFromPointer(childPtr)
		}
	}

	return mariInst.compareAndSwap(node, currNode, nodeCopy), nil
}

// Get
//	Attempts to retrieve the value for a key within the ordered array mapped trie.
//	It gets the latest version of the ordered array mapped trie and starts from that offset in the mem-map.
//	The operation begins at the root of the trie and traverses down the path to the key.
//	Get is concurrent since it will perform the operation on an existing path, so new paths can be written at the same time with new versions.
func (mariInst *Mari) Get(key []byte) (*KeyValuePair, error) {
	for atomic.LoadUint32(&mariInst.IsResizing) == 1 { runtime.Gosched() }

	mariInst.RWResizeLock.RLock()
	defer mariInst.RWResizeLock.RUnlock()

	_, rootOffset, loadROffErr := mariInst.loadMetaRootOffset()
	if loadROffErr != nil { return nil, loadROffErr }

	currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
	if readRootErr != nil { return nil, readRootErr }

	rootPtr := unsafe.Pointer(currRoot)
	return mariInst.getRecursive(&rootPtr, key, 0)
}

// getRecursive
//	Attempts to recursively retrieve a value for a given key within the ordered array mapped trie.
//	For each node traversed to at each level the operation travels to, the sparse index is calculated for the hashed key.
//	If the bit is not set in the bitmap, return nil since the key has not been inserted yet into the trie.
//	Otherwise, determine the position in the child node array for the sparse index.
//	If the child node is a leaf node and the key to be searched for is the same as the key of the child node, the value has been found.
//	Since the trie utilizes path copying, any threads modifying the trie are modifying copies so it the get operation returns the value at the point in time of the get operation.
//	If the node is node a leaf node, but instead an internal node, recurse down the path to the next level to the child node in the position of the child node array and repeat the above.
func (mariInst *Mari) getRecursive(node *unsafe.Pointer, key []byte, level int) (*KeyValuePair, error) {
	currNode := loadINodeFromPointer(node)
	
	getKeyVal := func() *KeyValuePair {
		return &KeyValuePair{
			Version: currNode.Leaf.Version,
			Key: currNode.Leaf.Key,
			Value: currNode.Leaf.Value,
		}
	}

	if len(key) == level {
		if bytes.Equal(key, currNode.Leaf.Key) { return getKeyVal(), nil }
		return nil, nil
	} else {
		if bytes.Equal(key, currNode.Leaf.Key) { return getKeyVal(), nil }
		
		index := getIndexForLevel(key, level)
		
		switch {
			case ! isBitSet(currNode.Bitmap, index):
				return nil, nil
			default:
				pos := mariInst.getPosition(currNode.Bitmap, index, level)
				childOffset := currNode.Children[pos]

				childNode, desErr := mariInst.readINodeFromMemMap(childOffset.StartOffset)
				if desErr != nil { return nil, desErr }

				childPtr := storeINodeAsPointer(childNode)
				return mariInst.getRecursive(childPtr, key, level + 1)
		}
	}
}

// Delete attempts to delete a key-value pair within the ordered array mapped trie.
//	It starts at the root of the trie and recurses down the path to the key to be deleted.
//	It first loads in the current metadata, and starts at the latest version of the root.
//	The operation creates an entire, in-memory copy of the path down to the key, where if the metadata hasn't changed during the copy, will get exclusive
//	write access to the memory-map, where the new path is serialized and appened to the end of the mem-map.
//	If the operation succeeds truthy value is returned, otherwise the operation returns to the root to retry the operation.
func (mariInst *Mari) Delete(key []byte) (bool, error) {
	for {		
		for atomic.LoadUint32(&mariInst.IsResizing) == 1 { runtime.Gosched() }
		mariInst.RWResizeLock.RLock()

		versionPtr, version, loadROffErr := mariInst.loadMetaVersion()
		if loadROffErr != nil { return false, loadROffErr }

		if version == atomic.LoadUint64(versionPtr) {
			_, rootOffset, loadROffErr := mariInst.loadMetaRootOffset()
			if loadROffErr != nil { return false, loadROffErr }

			currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
			if readRootErr != nil {
				mariInst.RWResizeLock.RUnlock()
				return false, readRootErr
			}

			currRoot.Version = currRoot.Version + 1
			rootPtr := storeINodeAsPointer(currRoot)
			_, delErr := mariInst.deleteRecursive(rootPtr, key, 0)
			if delErr != nil {
				mariInst.RWResizeLock.RUnlock()
				return false, delErr
			}

			updatedRootCopy := loadINodeFromPointer(rootPtr)
			ok, writeErr := mariInst.exclusiveWriteMmap(updatedRootCopy)
			if writeErr != nil {
				mariInst.RWResizeLock.RUnlock()
				return false, writeErr
			}

			if ok { 
				mariInst.RWResizeLock.RUnlock()
				return true, nil 
			}
		}

		mariInst.RWResizeLock.RUnlock()
		runtime.Gosched()
	}
}

// deleteRecursive
//	Attempts to recursively move down the path of the trie to the key-value pair to be deleted.
//	The byte index for the key is calculated, the sparse index in the bitmap is determined for the given level, and a copy of the current node is created to be modifed.
//	If the bit in the bitmap is not set, the key doesn't exist so truthy is returned since there is nothing to delete and the operation completes.
//	If the bit is set, the child node for the position within the child node array is found.
//	If the child node is a leaf node and the key of the child node is equal to the key of the key to delete, the copy is modified to update the bitmap and shrink the table and remove the given node.
//	A compare and swap operation is performed, and if successful traverse back up the trie and complete, otherwise the operation is returned to the root to retry.
//	If the child node is an internal node, the operation recurses down the trie to the next level.
//	On return, if the internal node is empty, the copy modified so the bitmap is updated and table is shrunk.
//	A compare and swap operation is performed on the current node with the new copy.
func (mariInst *Mari) deleteRecursive(node *unsafe.Pointer, key []byte, level int) (bool, error) {
	currNode := loadINodeFromPointer(node)
	nodeCopy := mariInst.copyINode(currNode)

	deleteKeyVal := func() bool {
		nodeCopy.Leaf = mariInst.newLeafNode(nil, nil, nodeCopy.Version)
		return mariInst.compareAndSwap(node, currNode, nodeCopy)
	}

	if len(key) == level {
		switch {
			case bytes.Equal(nodeCopy.Leaf.Key, key):
				return deleteKeyVal(), nil
			default:
				return true, nil
		}
	} else {
		index := getIndexForLevel(key, level)

		switch {
			case bytes.Equal(nodeCopy.Leaf.Key, key):
				return deleteKeyVal(), nil
			default:
				pos := mariInst.getPosition(nodeCopy.Bitmap, index, level)
				childOffset := nodeCopy.Children[pos]
		
				childNode, getChildErr := mariInst.getChildNode(childOffset, nodeCopy.Version)
				if getChildErr != nil { return false, getChildErr }
		
				childNode.Version = nodeCopy.Version
				childPtr := storeINodeAsPointer(childNode)

				_, delErr := mariInst.deleteRecursive(childPtr, key, level + 1)
				if delErr != nil { return false, delErr }

				updatedChildNode := loadINodeFromPointer(childPtr)
				nodeCopy.Children[pos] = updatedChildNode

				if updatedChildNode.Leaf.Version == nodeCopy.Version {
					childNodePopCount := populationCount(updatedChildNode.Bitmap)
					
					if childNodePopCount == 0 {
						nodeCopy.Bitmap = setBit(nodeCopy.Bitmap, index)
						nodeCopy.Children = shrinkTable(nodeCopy.Children, nodeCopy.Bitmap, pos)
					}
				}

				return mariInst.compareAndSwap(node, currNode, nodeCopy), nil
		}
	}
}

// compareAndSwap
//	Performs CAS opertion.
func (mariInst *Mari) compareAndSwap(node *unsafe.Pointer, currNode, nodeCopy *MariINode) bool {
	if atomic.CompareAndSwapPointer(node, unsafe.Pointer(currNode), unsafe.Pointer(nodeCopy)) {
		return true
	} else {
		mariInst.NodePool.LNodePool.Put(nodeCopy.Leaf)
		mariInst.NodePool.INodePool.Put(nodeCopy)

		return false
	}
}

// getChildNode
//	Get the child node of an internal node.
//	If the version is the same, set child as that node since it exists in the path.
//	Otherwise, read the node from the memory map.
func (mariInst *Mari) getChildNode(childOffset *MariINode, version uint64) (*MariINode, error) {
	var childNode *MariINode
	var desErr error

	if childOffset.Version == version {
		childNode = childOffset
	} else {
		childNode, desErr = mariInst.readINodeFromMemMap(childOffset.StartOffset)
		if desErr != nil { return nil, desErr }
	}

	return childNode, nil
}