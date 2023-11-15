package mari

import "bytes"
import "unsafe"


//============================================= Mari Range


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