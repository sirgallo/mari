package mari

import "errors"
import "sync/atomic"
import "unsafe"


//============================================= MariNode Operations


// ReadNodeFromMemMap
//	Reads a node in the mmcmap from the serialized memory map.
func (mariInst *Mari) ReadINodeFromMemMap(startOffset uint64) (node *MariINode, err error) {
	defer func() {
		r := recover()
		if r != nil {
			node = nil
			err = errors.New("error reading node from mem map")
		}
	}()
	
	endOffsetIdx := startOffset + NodeEndOffsetIdx
	
	mMap := mariInst.Data.Load().(MMap)
	sEndOffset := mMap[endOffsetIdx:endOffsetIdx + OffsetSize]

	endOffset, decEndOffErr := deserializeUint64(sEndOffset)
	if decEndOffErr != nil { return nil, decEndOffErr }

	sNode := mMap[startOffset:endOffset + 1]
	node, decNodeErr := mariInst.DeserializeINode(sNode)
	if decNodeErr != nil { return nil, decNodeErr }

	leaf, readLeafErr := mariInst.ReadLNodeFromMemMap(node.Leaf.StartOffset)
	if readLeafErr != nil { return nil, readLeafErr }

	node.Leaf = leaf
	return node, nil
}

// WriteINodeToMemMap
//	Serializes and writes an internal node instance to the memory map.
func (mariInst *Mari) WriteINodeToMemMap(node *MariINode) (offset uint64, err error) {
	defer func() {
		r := recover()
		if r != nil {
			offset = 0
			err = errors.New("error writing new path to mmap")
		}
	}()

	sNode, serializeErr := node.serializeINode(false)
	if serializeErr != nil { return 0, serializeErr	}

	mMap := mariInst.Data.Load().(MMap)
	copy(mMap[node.StartOffset:node.Leaf.StartOffset], sNode)

	flushErr := mariInst.flushRegionToDisk(node.StartOffset, node.EndOffset)
	if flushErr != nil { return 0, flushErr } 
	
	lEndOffset, writErr := mariInst.WriteLNodeToMemMap(node.Leaf)
	if writErr != nil { return 0, writErr }

	return lEndOffset, nil
}

// ReadI	NodeFromMemMap
//	Reads an node in the mmcmap from the serialized memory map.
func (mariInst *Mari) ReadLNodeFromMemMap(startOffset uint64) (node *MariLNode, err error) {
	defer func() {
		r := recover()
		if r != nil {
			node = nil
			err = errors.New("error reading node from mem map")
		}
	}()
	
	endOffsetIdx := startOffset + NodeEndOffsetIdx
	mMap := mariInst.Data.Load().(MMap)
	sEndOffset := mMap[endOffsetIdx:endOffsetIdx + OffsetSize]

	endOffset, decEndOffErr := deserializeUint64(sEndOffset)
	if decEndOffErr != nil { return nil, decEndOffErr }

	sNode := mMap[startOffset:endOffset + 1]
	node, decNodeErr := mariInst.DeserializeLNode(sNode)
	if decNodeErr != nil { return nil, decNodeErr }

	return node, nil
}

// WriteNodeToMemMap
//	Serializes and writes a MariNode instance to the memory map.
func (mariInst *Mari) WriteLNodeToMemMap(node *MariLNode) (offset uint64, err error) {
	defer func() {
		r := recover()
		if r != nil {
			offset = 0
			err = errors.New("error writing new path to mmap")
		}
	}()

	sNode, serializeErr := node.serializeLNode()
	if serializeErr != nil { return 0, serializeErr	}

	endOffset := node.determineEndOffsetLNode()
	mMap := mariInst.Data.Load().(MMap)
	copy(mMap[node.StartOffset:endOffset + 1], sNode)

	flushErr := mariInst.flushRegionToDisk(node.StartOffset, endOffset)
	if flushErr != nil { return 0, flushErr } 
	
	return endOffset + 1, nil
}

// copyNode
//	Creates a copy of an existing node.
//	This is used for path copying, so on operations that modify the trie, a copy is created instead of modifying the existing node.
//	The data structure is essentially immutable. 
//	If an operation succeeds, the copy replaces the existing node, otherwise the copy is discarded.
func (mariInst *Mari) copyINode(node *MariINode) *MariINode {
	nodeCopy := mariInst.NodePool.GetINode()
	
	nodeCopy.Version = node.Version
	nodeCopy.Bitmap = node.Bitmap
	nodeCopy.Leaf = node.Leaf
	nodeCopy.Children = make([]*MariINode, len(node.Children))

	copy(nodeCopy.Children, node.Children)
	
	return nodeCopy
}

// determineEndOffset
//	Determine the end offset of a serialized MariNode.
//	For Leaf Nodes, this will be the start offset through the key index, plus the length of the key and the length of the value.
//	For Internal Nodes, this will be the start offset through the children index, plus (number of children * 8 bytes).
func (node *MariINode) determineEndOffsetINode() uint64 {
	nodeEndOffset := node.StartOffset

	encodedChildrenLength := func() int {
		var totalChildren int 
		for _, subBitmap := range node.Bitmap {
			totalChildren += calculateHammingWeight(subBitmap)
		}
			
		return totalChildren * NodeChildPtrSize
	}()

	if encodedChildrenLength != 0 {
		nodeEndOffset += uint64(NodeChildrenIdx + encodedChildrenLength)
	} else { nodeEndOffset += NodeChildrenIdx }

	return nodeEndOffset - 1
}

func (node *MariLNode) determineEndOffsetLNode() uint64 {
	nodeEndOffset := node.StartOffset
	if node.Key != nil {
		nodeEndOffset += uint64(NodeKeyIdx + int(node.KeyLength) + len(node.Value))
	} else { nodeEndOffset += uint64(NodeKeyIdx) }
	
	return nodeEndOffset - 1
}

// getSerializedNodeSize
//	Get the length of the node based on the length of its serialized representation.
func getSerializedNodeSize(data []byte) uint64 {
	return uint64(len(data))
}

// initRoot
//	Initialize the Version 0 root where operations will begin traversing.
func (mariInst *Mari) initRoot() (uint64, error) {
	root := mariInst.NodePool.GetINode()
	root.StartOffset = uint64(InitRootOffset)

	endOffset, writeNodeErr := mariInst.WriteINodeToMemMap(root)
	if writeNodeErr != nil { return 0, writeNodeErr }

	return endOffset, nil
}

// loadNodeFromPointer
//	Load a mmcmap node from an unsafe pointer.
func loadINodeFromPointer(ptr *unsafe.Pointer) *MariINode {
	return (*MariINode)(atomic.LoadPointer(ptr))
}

// newInternalNode
//	Creates a new internal node in the hash array mapped trie, which is essentially a branch node that contains pointers to child nodes.
func (mariInst *Mari) newInternalNode(version uint64) *MariINode {
	iNode := mariInst.NodePool.GetINode()
	iNode.Version = version

	return iNode
}

// newLeafNode
//	Creates a new leaf node when path copying the mmcmap, which stores a key value pair.
//	It will also include the version of the mmcmap.
func (mariInst *Mari) newLeafNode(key, value []byte, version uint64) *MariLNode {
	lNode := mariInst.NodePool.GetLNode()
	lNode.Version = version
	lNode.KeyLength = uint16(len(key))
	lNode.Key = key
	lNode.Value = value

	return lNode
}

// storeNodeAsPointer
//	Store a mmcmap node as an unsafe pointer.
func storeINodeAsPointer(node *MariINode) *unsafe.Pointer {
	ptr := unsafe.Pointer(node)
	return &ptr
}

// writeNodesToMemMap
//	Write a list of serialized nodes to the memory map. If the mem map is too small for the incoming nodes, dynamically resize.
func (mariInst *Mari) writeNodesToMemMap(snodes []byte, offset uint64) (ok bool, err error) {
	defer func() {
		r := recover()
		if r != nil {
			ok = false
			err = errors.New("error writing new path to mmap")
		}
	}()

	lenSNodes := uint64(len(snodes))
	endOffset := offset + lenSNodes

	mMap := mariInst.Data.Load().(MMap)
	copy(mMap[offset:endOffset], snodes)

	return true, nil
}