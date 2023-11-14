package mari

import "bytes"
import "errors"
import "unsafe"


//============================================= Mari Range


// Range
//	Since the array mapped trie is sorted by nature, the range operation begins at the root of the trie.
//	It checks the root bitmap and determines which indexes to check in the range.
//	It then recursively checks each index, traversing the paths and building the sorted results.
//	A minimum version can be provided which will limit results to the min version forward.
//	If nil is passed for the minimum version, the earliest version in the structure will be used.
func (mariInst *Mari) Range(startKey, endKey []byte, opts *MariRangeOpts) ([]*KeyValuePair, error) {
	if bytes.Compare(startKey, endKey) == 1 { return nil, errors.New("start key is larger than end key") }

	var minV uint64 
	var transform MariOpTransform

	if opts != nil && opts.MinVersion != nil {
		minV = *opts.MinVersion
	} else { minV = 0 }

	if opts != nil && opts.Transform != nil {
		transform = *opts.Transform
	} else { transform = func(kvPair *KeyValuePair) *KeyValuePair { return kvPair } }

	_, rootOffset, loadROffErr := mariInst.loadMetaRootOffset()
	if loadROffErr != nil { return nil, loadROffErr }

	currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
	if readRootErr != nil { return nil, readRootErr }

	rootPtr := storeINodeAsPointer(currRoot)
	kvPairs, rangeErr := mariInst.rangeRecursive(rootPtr, minV, startKey, endKey, 0, transform)
	if rangeErr != nil { return nil, rangeErr }

	return kvPairs, nil
}

// rangeRecursive
//	Limit the indexes to check in the range at level 0, and then recursively traverse the paths between the start and end index.
//	On the start key path, continue to use the start index to check the level to see which index forward should be recursively checked.
//	The opposite is done for the end key path.
func (mariInst *Mari) rangeRecursive(
	node *unsafe.Pointer, minVersion uint64, 
	startKey, endKey []byte, level int, 
	transform MariOpTransform,
) ([]*KeyValuePair, error) {
	genKeyValPair := func(node *MariINode) *KeyValuePair {
		kvPair := &KeyValuePair {
			Version: node.Leaf.Version,
			Key: node.Leaf.Key,
			Value: node.Leaf.Value,
		}

		return kvPair
	}

	currNode := loadINodeFromPointer(node)

	var sortedKvPairs []*KeyValuePair
	var startKeyPos, endKeyPos int

	if level > 0 {
		switch {
			case startKey != nil && len(startKey) > level:
				if currNode.Leaf.Version >= minVersion && bytes.Compare(currNode.Leaf.Key, startKey) == 1 {
					sortedKvPairs = append(sortedKvPairs, transform(genKeyValPair(currNode)))
				} else { return sortedKvPairs, nil }

				startKeyIndex := getIndexForLevel(startKey, level)
				startKeyPos = mariInst.getPosition(currNode.Bitmap, startKeyIndex, level)
				endKeyPos = len(currNode.Children)
			case endKey != nil && len(endKey) > level:
				if currNode.Leaf.Version >= minVersion && bytes.Compare(currNode.Leaf.Key, endKey) == -1 {
					sortedKvPairs = append(sortedKvPairs, transform(genKeyValPair(currNode)))
				} else { return sortedKvPairs, nil }

				startKeyPos = 0
				endKeyIndex := getIndexForLevel(endKey, level)
				endKeyPos = mariInst.getPosition(currNode.Bitmap, endKeyIndex, level)
			default:
				if currNode.Leaf.Version >= minVersion && len(currNode.Leaf.Key) > 0 { sortedKvPairs = append(sortedKvPairs, transform(genKeyValPair(currNode))) }

				startKeyPos = 0
				endKeyPos = len(currNode.Children)
		}
	} else {
		startKeyIndex := getIndexForLevel(startKey, 0)
		startKeyPos = mariInst.getPosition(currNode.Bitmap, startKeyIndex, 0)

		endKeyIndex := getIndexForLevel(endKey, 0)
		endKeyPos = mariInst.getPosition(currNode.Bitmap, endKeyIndex, 0)
	}

	if len(currNode.Children) > 0 {
		var kvPairs []*KeyValuePair
		var rangeErr error

		switch {
			case startKeyPos == endKeyPos:
				childNode, getChildErr := mariInst.getChildNode(currNode.Children[startKeyPos], currNode.Version)
				if getChildErr != nil { return nil, getChildErr}
				childPtr := storeINodeAsPointer(childNode)

				kvPairs, rangeErr = mariInst.rangeRecursive(childPtr, minVersion, startKey, endKey, level + 1, transform)
				if rangeErr != nil { return nil, rangeErr }

				if len(kvPairs) > 0 { sortedKvPairs = append(sortedKvPairs, kvPairs...) }
			default:
				for idx, childOffset := range currNode.Children[startKeyPos:endKeyPos] {		
					childNode, getChildErr := mariInst.getChildNode(childOffset, currNode.Version)
					if getChildErr != nil { return nil, getChildErr}
					childPtr := storeINodeAsPointer(childNode)
		
					switch {
						case idx == 0 && startKey != nil:
							kvPairs, rangeErr = mariInst.rangeRecursive(childPtr, minVersion, startKey, nil, level + 1, transform)
							if rangeErr != nil { return nil, rangeErr }
						case idx == endKeyPos && endKey != nil:
							kvPairs, rangeErr = mariInst.rangeRecursive(childPtr, minVersion, nil, endKey, level + 1, transform)
							if rangeErr != nil { return nil, rangeErr }
						default:
							kvPairs, rangeErr = mariInst.rangeRecursive(childPtr, minVersion, nil, nil, level + 1, transform)
							if rangeErr != nil { return nil, rangeErr }
					}
		
					if len(kvPairs) > 0 { sortedKvPairs = append(sortedKvPairs, kvPairs...) }
				}
		}
	}

	return sortedKvPairs, nil
}