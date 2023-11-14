package mari

import "bytes"
import "unsafe"


//============================================= Mari Iterate


// Iterate
//	Creates an ordered iterator starting at the given start key up to the range specified by total results.
//	Since the array mapped trie is sorted, the iterate function starts at the startKey and recursively builds the result set up the specified end.
//	A minimum version can be provided which will limit results to the min version forward.
//	If nil is passed for the minimum version, the earliest version in the structure will be used.
func (mariInst *Mari) Iterate(startKey []byte, totalResults int, minVersion *uint64) ([]*KeyValuePair, error) {
	var minV uint64 
	if minVersion != nil {
		minV = *minVersion
	} else { minV = 0 }

	_, rootOffset, loadROffErr := mariInst.loadMetaRootOffset()
	if loadROffErr != nil { return nil, loadROffErr }

	currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
	if readRootErr != nil { return nil, readRootErr }

	accumulator := []*KeyValuePair{}
	rootPtr := storeINodeAsPointer(currRoot)

	kvPairs, iterErr := mariInst.iterateRecursive(rootPtr, minV, startKey, totalResults, 0, accumulator)
	if iterErr != nil { return nil, iterErr }

	return kvPairs, nil
}

// iterateRecursive
//	Essentially create a cursor that begins at the specified start key and recursively builds an accumulator of key value pairs until it reaches the max size.
func (mariInst *Mari) iterateRecursive(node *unsafe.Pointer, minVersion uint64, startKey []byte, totalResults, level int, acc []*KeyValuePair) ([]*KeyValuePair, error) {
	genKeyValPair := func(node *MariINode) *KeyValuePair {
		kvPair := &KeyValuePair {
			Version: node.Leaf.Version,
			Key: node.Leaf.Key,
			Value: node.Leaf.Value,
		}

		return kvPair
	}

	currNode := loadINodeFromPointer(node)

	var startKeyPos int

	if level > 0 {
		switch {
			case totalResults == len(acc):
				return acc, nil
			case startKey != nil && len(startKey) > level:
				if currNode.Leaf.Version >= minVersion && bytes.Compare(currNode.Leaf.Key, startKey) == 1 {
					acc = append(acc, genKeyValPair(currNode))
				} else { return acc, nil }

				startKeyIndex := getIndexForLevel(startKey, level)
				startKeyPos = mariInst.getPosition(currNode.Bitmap, startKeyIndex, level)
			default:
				if currNode.Leaf.Version >= minVersion && len(currNode.Leaf.Key) > 0 { acc = append(acc, genKeyValPair(currNode)) }
				startKeyPos = 0
		}
	} else {
		startKeyIdx := getIndexForLevel(startKey, level)
		startKeyPos = mariInst.getPosition(currNode.Bitmap, startKeyIdx, level)
	}

	if len(currNode.Children) > 0 {
		currPos := startKeyPos

		for totalResults > len(acc) || currPos > len(currNode.Children) {
			childOffset := currNode.Children[currPos]

			childNode, getChildErr := mariInst.getChildNode(childOffset, currNode.Version)
			if getChildErr != nil { return nil, getChildErr}
			childPtr := storeINodeAsPointer(childNode)

			var iterErr error

			switch {
				case currPos == startKeyPos && startKey != nil:
					acc, iterErr = mariInst.iterateRecursive(childPtr, minVersion, startKey, totalResults, level + 1, acc)
					if iterErr != nil { return nil, iterErr }
				default:
					acc, iterErr = mariInst.iterateRecursive(childPtr, minVersion, nil, totalResults, level + 1, acc)
					if iterErr != nil { return nil, iterErr }
			}

			startKeyPos = startKeyPos + 1
		}
	}

	return acc, nil
}