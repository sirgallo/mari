package mari

import "encoding/binary"
import "errors"


//============================================= Mari Serialization


// SerializeMetaData
//	Serialize the metadata at the first 0-23 bytes of the memory map. Version is 8 bytes and Root Offset is 8 bytes.
func (meta *MariMetaData) SerializeMetaData() []byte {
	versionBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(versionBytes, meta.Version)

	rootOffsetBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(rootOffsetBytes, meta.RootOffset)

	nextStartOffsetBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(nextStartOffsetBytes, meta.NextStartOffset)

	offsets := append(rootOffsetBytes, nextStartOffsetBytes...)
	return append(versionBytes, offsets...)
}

// DeserializeMetaData
//	Deserialize the byte representation of the meta data object in the memory mapped file.
func DeserializeMetaData(smeta []byte) (*MariMetaData, error) {
	if len(smeta) != 24 { return nil, errors.New("meta data incorrect size") }

	versionBytes := smeta[MetaVersionIdx:MetaRootOffsetIdx]
	version := binary.LittleEndian.Uint64(versionBytes)

	rootOffsetBytes := smeta[MetaRootOffsetIdx:MetaEndSerializedOffset]
	rootOffset := binary.LittleEndian.Uint64(rootOffsetBytes)

	nextStartOffsetBytes := smeta[MetaEndSerializedOffset:]
	nextStartOffset := binary.LittleEndian.Uint64(nextStartOffsetBytes)

	return &MariMetaData{
		Version: version,
		RootOffset: rootOffset,
		NextStartOffset: nextStartOffset,
	}, nil
}

// DeserializeINode
//	Deserialize the byte representation of an internal in the memory mapped file.
func (mmcMap *Mari) DeserializeINode(snode []byte) (*MariINode, error) {
	version, decVersionErr := deserializeUint64(snode[NodeVersionIdx:NodeStartOffsetIdx])
	if decVersionErr != nil { return nil, decVersionErr }

	startOffset, decStartOffErr := deserializeUint64(snode[NodeStartOffsetIdx:NodeEndOffsetIdx])
	if decStartOffErr != nil { return nil, decStartOffErr	}

	endOffset, decEndOffsetErr := deserializeUint64(snode[NodeEndOffsetIdx:NodeBitmapIdx])
	if decEndOffsetErr != nil { return nil, decEndOffsetErr }

	var bitmaps [8]uint32
	for i := range make([]int, 8) {
		bitmap, decBitmapErr := deserializeUint32(snode[NodeBitmapIdx + (4 * i):NodeBitmapIdx + (4 * i) + 4])
		if decBitmapErr != nil { return nil, decBitmapErr }

		bitmaps[i] = bitmap
	}

	leafOffset, decLeafOffErr := deserializeUint64(snode[NodeLeafOffsetIdx:NodeChildrenIdx])
	if decLeafOffErr != nil { return nil, decLeafOffErr }

	var totalChildren int 
	for _, subBitmap := range bitmaps {
		totalChildren += calculateHammingWeight(subBitmap)
	}

	var children []*MariINode

	currOffset := NodeChildrenIdx
	for range make([]int, totalChildren) {
		offset, decChildErr := deserializeUint64(snode[currOffset:currOffset + OffsetSize])
		if decChildErr != nil { return nil, decChildErr }

		nodePtr := &MariINode{ StartOffset: offset }
		children = append(children, nodePtr)
		currOffset += NodeChildPtrSize
	}

	return &MariINode{
		Version: version,
		StartOffset: startOffset,
		EndOffset: endOffset,
		Bitmap: bitmaps,
		Leaf: &MariLNode{ StartOffset: leafOffset },
		Children: children,
	}, nil
}

// DeserializeLNode
//	Deserialize the byte representation of a leaf node in the memory mapped file.
func (mmcMap *Mari) DeserializeLNode(snode []byte) (*MariLNode, error) {
	version, decVersionErr := deserializeUint64(snode[NodeVersionIdx:NodeStartOffsetIdx])
	if decVersionErr != nil { return nil, decVersionErr }

	startOffset, decStartOffErr := deserializeUint64(snode[NodeStartOffsetIdx:NodeEndOffsetIdx])
	if decStartOffErr != nil { return nil, decStartOffErr	}

	endOffset, decEndOffsetErr := deserializeUint64(snode[NodeEndOffsetIdx:NodeKeyLength])
	if decEndOffsetErr != nil { return nil, decEndOffsetErr }

	keyLength, decKeyLenErr := deserializeUint16(snode[NodeKeyLength:NodeKeyIdx])
	if decKeyLenErr != nil { return nil, decKeyLenErr }

	key := snode[NodeKeyIdx:NodeKeyIdx + keyLength]
	value := snode[NodeKeyIdx + keyLength:]

	return &MariLNode{
		Version: version,
		StartOffset: startOffset,
		EndOffset: endOffset,
		KeyLength: keyLength,
		Key: key,
		Value: value,
	}, nil
}


// SerializePathToMemMap
//	Serializes a path copy by starting at the root, getting the latest available offset in the memory map, and recursively serializing.
func (mmcMap *Mari) SerializePathToMemMap(root *MariINode, nextOffsetInMMap uint64) ([]byte, error) {
	serializedPath, serializeErr := mmcMap.serializeRecursive(root, 0, nextOffsetInMMap)
	if serializeErr != nil { return nil, serializeErr }

	return serializedPath, nil
}

// serializeRecursive
//	Traverses the path copy down to the end of the path.
//	If the node is a leaf, serialize it and return. If the node is a internal node, serialize each of the children recursively if
//	the version matches the version of the root. If it is an older version, just serialize the existing offset in the memory map.
func (mmcMap *Mari) serializeRecursive(node *MariINode, level int, offset uint64) ([]byte, error) {
	node.StartOffset = offset
	
	sNode, serializeErr := node.serializeINode(true)
	if serializeErr != nil { return nil, serializeErr }

	serializedKeyVal, sLeafErr := node.Leaf.serializeLNode()
	if sLeafErr != nil { return nil, sLeafErr }

	var childrenOnPaths []byte
	nextStartOffset := node.Leaf.EndOffset + 1

	for _, child := range node.Children {
		if child.Version != node.Version {
			sNode = append(sNode, serializeUint64(child.StartOffset)...)
		} else {
			sNode = append(sNode, serializeUint64(nextStartOffset)...)
			childrenOnPath, serializeErr := mmcMap.serializeRecursive(child, level + 1, nextStartOffset)
			if serializeErr != nil { return nil, serializeErr }

			nextStartOffset += getSerializedNodeSize(childrenOnPath)
			childrenOnPaths = append(childrenOnPaths, childrenOnPath...)
		}
	}

	sNode = append(sNode, serializedKeyVal...)

	if len(childrenOnPaths) > 0 { sNode = append(sNode, childrenOnPaths...) }

	mmcMap.NodePool.PutLNode(node.Leaf)
	mmcMap.NodePool.PutINode(node)
	
	return sNode, nil
}

// SerializeLNode
//	Serialize a leaf node in the mariInst. Append the key and value together since both are already byte slices.
func (node *MariLNode) serializeLNode() ([]byte, error) {
	var sLNode []byte

	node.EndOffset = node.determineEndOffsetLNode()

	sVersion := serializeUint64(node.Version)
	sStartOffset := serializeUint64(node.StartOffset)
	sEndOffset := serializeUint64(node.EndOffset)
	sKeyLength := serializeUint16(node.KeyLength)

	sLNode = append(sLNode, sVersion...)
	sLNode = append(sLNode, sStartOffset...)
	sLNode = append(sLNode, sEndOffset...)
	sLNode = append(sLNode, sKeyLength...)
	
	sLNode = append(sLNode, node.Key...)
	sLNode = append(sLNode, node.Value...)

	return sLNode, nil
}

// SerializeINode
//	Serialize an internal node in the mariInst. This involves scanning the children nodes and serializing the offset in the memory map for each one.
func (node *MariINode) serializeINode(serializePath bool) ([]byte, error) {
	var sINode []byte

	node.EndOffset = node.determineEndOffsetINode()
	node.Leaf.StartOffset = node.EndOffset + 1
	
	sVersion := serializeUint64(node.Version)
	sStartOffset := serializeUint64(node.StartOffset)
	sEndOffset := serializeUint64(node.EndOffset)
	sLeafOffset := serializeUint64(node.Leaf.StartOffset)
	
	var sBitmap []byte
	for _, subBitmap := range node.Bitmap {
		sSubBitmap := serializeUint32(subBitmap)
		sBitmap = append(sBitmap, sSubBitmap...)
	}

	sINode = append(sINode, sVersion...)
	sINode = append(sINode, sStartOffset...)
	sINode = append(sINode, sEndOffset...)
	sINode = append(sINode, sBitmap...)
	sINode = append(sINode, sLeafOffset...)

	if ! serializePath { 
		for _, cnode := range node.Children {
			snode := serializeUint64(cnode.StartOffset)
			sINode = append(sINode, snode...)
		}
	}

	return sINode, nil
}


//============================================= Helper Functions for Serialize/Deserialize primitives


func serializeUint64(in uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, in)
	return buf
}

func deserializeUint64(data []byte) (uint64, error) {
	if len(data) != 8 { return uint64(0), errors.New("invalid data length for byte slice to uint64") }
	return binary.LittleEndian.Uint64(data), nil
}

func serializeUint32(in uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, in)
	return buf
}

func deserializeUint32(data []byte) (uint32, error) {
	if len(data) != 4 { return uint32(0), errors.New("invalid data length for byte slice to uint32") }
	return binary.LittleEndian.Uint32(data), nil
}

func serializeUint16(in uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, in)
	return buf
}

func deserializeUint16(data []byte) (uint16, error) {
	if len(data) != 2 { return uint16(0), errors.New("invalid data length for byte slice to uint16") }
	return binary.LittleEndian.Uint16(data), nil
}