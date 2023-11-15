package mari

import "bytes"
import "unsafe"


//============================================= Mari Iterate


// iterateRecursive
//	Essentially create a cursor that begins at the specified start key.
//	Recursively builds an accumulator of key value pairs until it reaches the max size.
func (mariInst *Mari) iterateRecursive(
	node *unsafe.Pointer, minVersion uint64, 
	startKey []byte, totalResults, level int, 
	acc []*KeyValuePair, transform MariOpTransform,
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

	var startKeyPos int

	if level > 0 {
		switch {
			case totalResults == len(acc):
				return acc, nil
			case startKey != nil && len(startKey) > level:
				if currNode.Leaf.Version >= minVersion && bytes.Compare(currNode.Leaf.Key, startKey) == 1 {
					acc = append(acc, transform(genKeyValPair(currNode)))
				} else { return acc, nil }

				startKeyIndex := getIndexForLevel(startKey, level)
				startKeyPos = mariInst.getPosition(currNode.Bitmap, startKeyIndex, level)
			default:
				if currNode.Leaf.Version >= minVersion && len(currNode.Leaf.Key) > 0 { 
					acc = append(acc, transform(genKeyValPair(currNode)))
				}

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
					acc, iterErr = mariInst.iterateRecursive(childPtr, minVersion, startKey, totalResults, level + 1, acc, transform)
					if iterErr != nil { return nil, iterErr }
				default:
					acc, iterErr = mariInst.iterateRecursive(childPtr, minVersion, nil, totalResults, level + 1, acc, transform)
					if iterErr != nil { return nil, iterErr }
			}

			startKeyPos = startKeyPos + 1
		}
	}

	return acc, nil
}