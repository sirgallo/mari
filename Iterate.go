package mari

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
	
	kvPairs, rangeErr := mariInst.iterateRecursive(rootPtr, minV, startKey, 0, accumulator)
	if rangeErr != nil { return nil, rangeErr }

	return kvPairs, nil
}

func (mariInst *Mari) iterateRecursive(node *unsafe.Pointer, minVersion uint64, startKey []byte, level int, acc []*KeyValuePair) ([]*KeyValuePair, error) {
	return nil, nil
}