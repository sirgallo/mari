package mari

import "fmt"
import "math/bits"
import "sync/atomic"
import "unsafe"



//============================================= Mari Utilities


// Print Children
//	Debugging function for printing nodes in the hash array mapped trie.
func (mariInst *Mari) PrintChildren() error {
	mMap := mariInst.Data.Load().(MMap)
	rootOffsetPtr := (*uint64)(unsafe.Pointer(&mMap[MetaRootOffsetIdx]))
	rootOffset := atomic.LoadUint64(rootOffsetPtr)

	currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
	if readRootErr != nil { return readRootErr }
	
	readChildrenErr := mariInst.printChildrenRecursive(currRoot, 0)
	if readChildrenErr != nil { return readChildrenErr }

	return nil
}

// calculateHammingWeight
//	Determines the total number of 1s in the binary representation of a number. 0s are ignored.
func calculateHammingWeight(bitmap uint32) int {
	return bits.OnesCount32(bitmap)
}

// extendTable
//	Utility function for dynamically expanding the child node array if a bit is set and a value needs to be inserted into the array.
func extendTable(orig []*MariINode, bitMap [8]uint32, pos int, newNode *MariINode) []*MariINode {
	var tableSize int
	for _, subBitmap := range bitMap {
		tableSize += calculateHammingWeight(subBitmap)
	}
	
	newTable := make([]*MariINode, tableSize)

	copy(newTable[:pos], orig[:pos])
	newTable[pos] = newNode
	copy(newTable[pos + 1:], orig[pos:])
	
	return newTable
}

// getIndexForLevel
//	Determines the local level for a hash at a particular seed.
func getIndexForLevel(key []byte, level int) byte {
	return key[level]
}

// getPosition
//	Calculates the position in the child node array based on the sparse index and the current bitmap of internal node.
//	The sparse index is calculated using the hash and bitchunk size.
//	A mask is calculated by performing a bitwise left shift operation, which shifts the binary representation of the value 1 the number of positions associated with the sparse index value and then subtracts 1.
//	This creates a binary number with all 1s to the right sparse index positions.
//	The mask is then applied the bitmap and the resulting isolated bits are the 1s right of the sparse index. 
//	The hamming weight, or total bits right of the sparse index, is then calculated.
func (mariInst *Mari) getPosition(bitMap [8]uint32, index byte, level int) int {
	subBitmapIndex := index / 32
	indexInSubBitmap := index % 32

	subBitmap := bitMap[subBitmapIndex]
	mask := uint32((1 << uint32(indexInSubBitmap)) - 1)
	isolatedBits := subBitmap & mask
	
	return calculateHammingWeight(isolatedBits)
}

// isBitSet
//	Determines whether or not a bit is set in a bitmap by taking the bitmap and applying a mask with a 1 at the position in the bitmap to check.
//	A logical and operation is applied and if the value is not equal to 0, then the bit is set.
func isBitSet(bitmap [8]uint32, index byte) bool {
	subBitmapIndex := index / 32

	subBitmap := bitmap[subBitmapIndex]
	indexInSubBitmap := index % 32

	return (subBitmap & (1 << indexInSubBitmap)) != 0
}

// populationCount
//	Determine the total population for the combination of all 8 32 bit bitmaps making up the 256 bit bitmap.
func populationCount(bitmap [8]uint32) int {
	var popCount int
	for _, subBitmap := range bitmap {
		popCount += calculateHammingWeight(subBitmap)
	}

	return popCount
}

// setBit
//	Performs a logical xor operation on the current bitmap and the a 32 bit value where the value is all 0s except for at the position of the incoming index.
//	Essentially flips the bit if incoming is 1 and bitmap is 0 at that position, or 0 to 1. 
//	If 0 and 0 or 1 and 1, bitmap is not changed.
func setBit(bitmap [8]uint32, index byte) [8]uint32 {
	subBitmapIndex := index / 32
	indexInSubBitmap := index % 32

	bitmap[subBitmapIndex] = bitmap[subBitmapIndex] ^ (1 << indexInSubBitmap)
	
	return bitmap
}

// shrinkTable
//	Inverse of the extendTable utility function.
//	It dynamically shrinks a table by removing an element at a given position.
func shrinkTable(orig []*MariINode, bitMap [8]uint32, pos int) []*MariINode {
	var tableSize int 
	for _, subBitmap := range bitMap {
		tableSize += calculateHammingWeight(subBitmap)
	}

	newTable := make([]*MariINode, tableSize)

	copy(newTable[:pos], orig[:pos])
	copy(newTable[pos:], orig[pos + 1:])
	
	return newTable
}

// printChildrenRecursive
//	Recursively print nodes in the mariInst as we traverse down levels.
func (mariInst *Mari) printChildrenRecursive(node *MariINode, level int) error {
	if node == nil { return nil }

	for idx := range node.Children {
		childPtr := node.Children[idx]
		child, desErr := mariInst.readINodeFromMemMap(childPtr.StartOffset)
		if desErr != nil { return desErr }

		if child != nil {
			fmt.Printf("Level: %d, Index: %d, Key: %s, Value: %s, Version:%d\n", level + 1, idx, child.Leaf.Key, string(child.Leaf.Value), child.Leaf.Version)
			mariInst.printChildrenRecursive(child, level + 1)
		}
	}

	return nil
}