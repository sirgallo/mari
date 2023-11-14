package mari

import "os"
import "sync"
import "sync/atomic"


// MMap
//	The byte array representation of the memory mapped file in memory.
type MMap []byte

// MariOpts initialize the Mari
type MariOpts struct {
	// Filepath: the path to the memory mapped file
	Filepath string
}

// MariMetaData contains information related to where the root is located in the mem map and the version.
type MariMetaData struct {
	// Version: a tag for Copy-on-Write indicating the version of Mari
	Version uint64
	// RootOffset: the offset of the latest version root node in Mari
	RootOffset uint64
	// NextStartOffset: the offset where the last node in the mmap is located
	NextStartOffset uint64
}

// MariNode represents a singular node within the hash array mapped trie data structure.
type MariINode struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	Version uint64
	// StartOffset: the offset from the beginning of the serialized node is located
	StartOffset uint64
	// EndOffset: the offset from the end of the serialized node is located
	EndOffset uint64
	// Bitmap: a 256 bit sparse index that indicates the location of each hashed key within the array of child nodes. Only stored in internal nodes
	Bitmap [8]uint32
	// LeafOffset: the offset of the leaf node associated with the current byte chunk
	Leaf *MariLNode
	// Children: an array of child nodes, which are MariNodes. Location in the array is determined by the sparse index
	Children []*MariINode
}

// MariNode represents a singular node within the hash array mapped trie data structure.
type MariLNode struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	Version uint64
	// StartOffset: the offset from the beginning of the serialized node is located
	StartOffset uint64
	// EndOffset: the offset from the end of the serialized node is located
	EndOffset uint64
	// KeyLength: the length of the key in a Leaf Node. Keys can be variable size
	KeyLength uint16
	// Key: The key associated with a value. Keys are in byte array representation. Keys are only stored within leaf nodes
	Key []byte
	// Value: The value associated with a key, in byte array representation. Values are only stored within leaf nodes
	Value []byte
}

// KeyValuePair
type KeyValuePair struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	Version uint64
	// Key: The key associated with a value. Keys are in byte array representation. Keys are only stored within leaf nodes
	Key []byte
	// Value: The value associated with a key, in byte array representation. Values are only stored within leaf nodes
	Value []byte
}

// Mari contains the memory mapped buffer for Mari, as well as all metadata for operations to occur
type Mari struct {
	// Filepath: path to the Mari file
	Filepath string
	// File: the Mari file
	File *os.File
	// Opened: flag indicating if the file has been opened
	Opened bool
	// Data: the memory mapped file as a byte slice
	Data atomic.Value
	// IsResizing: atomic flag to determine if the mem map is being resized or not
	IsResizing uint32
	// SignalResize: send a signal to the resize go routine with the offset for resizing
	SignalResize chan bool
	// SignalFlush: send a signal to flush to disk on writes to avoid contention
	SignalFlush chan bool
	// ReadResizeLock: A Read-Write mutex for locking reads on resize operations
	RWResizeLock sync.RWMutex
	// NodePool: the sync.Pool for recycling nodes so nodes are not constantly allocated/deallocated
	NodePool *MariNodePool
}

// MariNodePool contains pre-allocated MariINodes/MariLNodes to improve performance so go garbage collection doesn't handle allocating/deallocating nodes on every op
type MariNodePool struct {
	// MaxSize: the max size for the node pool
	MaxSize int64
	// Size: the current number of allocated nodes in the node pool
	Size int64
	// Pool: the node pool that contains pre-allocated nodes
	INodePool *sync.Pool

	LNodePool *sync.Pool
}

type MariOpTransform = func(kvPair *KeyValuePair) *KeyValuePair

type MariRangeOpts struct {
	MinVersion *uint64
	Transform *MariOpTransform
}

// DefaultPageSize is the default page size set by the underlying OS. Usually will be 4KiB
var DefaultPageSize = os.Getpagesize()

const (
	// Index of Mari Version in serialized metadata
	MetaVersionIdx = 0
	// Index of Root Offset in serialized metadata
	MetaRootOffsetIdx = 8
	// Index of Node Version in serialized node
	MetaEndSerializedOffset = 16
	// The current node version index in serialized node
	NodeVersionIdx = 0
	// Index of StartOffset in serialized node
	NodeStartOffsetIdx = 8
	// Index of EndOffset in serialized node
	NodeEndOffsetIdx = 16
	// Index of Bitmap in serialized node
	NodeBitmapIdx = 24
	// Index of IsLeaf in serialized node
	NodeLeafOffsetIdx = 56
	// Index of Children in serialized internal node
	NodeChildrenIdx = 64
	// Index of Key Length in serialized node
	NodeKeyLength = 24
	// Index of Key in serialized leaf node node
	NodeKeyIdx = 26
	// OffsetSize for uint64 in serialized node
	OffsetSize = 8
	// Bitmap size in bytes since bitmap sis uint32
	BitmapSize = 4
	// Size of child pointers, where the pointers are uint64 offsets in the memory map
	NodeChildPtrSize = 8
	// Offset for the first version of root on Mari initialization
	InitRootOffset = 24
	// 1 GB MaxResize
	MaxResize = 1000000000
)

const (
	// RDONLY: maps the memory read-only. Attempts to write to the MMap object will result in undefined behavior.
	RDONLY = 0
	// RDWR: maps the memory as read-write. Writes to the MMap object will update the underlying file.
	RDWR = 1 << iota
	// COPY: maps the memory as copy-on-write. Writes to the MMap object will affect memory, but the underlying file will remain unchanged.
	COPY
	// EXEC: marks the mapped memory as executable.
	EXEC
)

const (
	// If the ANON flag is set, the mapped memory will not be backed by a file.
	ANON = 1 << iota
)

// 1 << iota // this creates powers of 2

/*
	Offsets explained:

	Meta:
		0 Version - 8 bytes
		8 RootOffset - 8 bytes
		16 EndMmapOffset - 8 bytes

	[0-7, 8-15, 16-23, 24-27, 28, 29-92, 93+]
	Node (Leaf):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 8 bytes
		24 KeyLength - 2 bytes, size of the key
		26 Key - variable length


	Node (Internal):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 8 bytes
		24 8 Bitmaps - 32 bytes
		56 LeafOffset - 8 bytes
		64 Children -->
			every child will then be 8 bytes, up to 256 * 8 = 2048 bytes
*/