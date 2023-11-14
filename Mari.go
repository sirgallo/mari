package mari

import "os"
import "sync/atomic"

import "github.com/sirgallo/utils"


//============================================= Mari


// Open initializes a new mmcmap
//	This will create the memory mapped file or read it in if it already exists.
//	Then, the meta data is initialized and written to the first 0-23 bytes in the memory map.
//	An initial root MariNode will also be written to the memory map as well.
func Open(opts MariOpts) (*Mari, error) {
	np := newMariNodePool(100000)	// let's initialize with 100,000 pre-allocated nodes

	mariInst := &Mari{
		Opened: true,
		SignalResize: make(chan bool),
		SignalFlush: make(chan bool),
		NodePool: np,
	}

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	var openFileErr error

	mariInst.File, openFileErr = os.OpenFile(opts.Filepath, flag, 0600)
	if openFileErr != nil { return nil, openFileErr	}

	mariInst.Filepath = mariInst.File.Name()
	atomic.StoreUint32(&mariInst.IsResizing, 0)
	mariInst.Data.Store(MMap{})

	initFileErr := mariInst.initializeFile()
	if initFileErr != nil { return nil, initFileErr	}

	go mariInst.handleFlush()
	go mariInst.handleResize()

	return mariInst, nil
}

// Close
//	Close the mmcmap, unmapping the file from memory and closing the file.
func (mariInst *Mari) Close() error {
	if ! mariInst.Opened { return nil }
	mariInst.Opened = false

	flushErr := mariInst.File.Sync()
	if flushErr != nil { return flushErr }

	unmapErr := mariInst.munmap()
	if unmapErr != nil { return unmapErr }

	if mariInst.File != nil {
		closeErr := mariInst.File.Close()
		if closeErr != nil { return closeErr }
	}

	mariInst.Filepath = utils.GetZero[string]()
	return nil
}

// FileSize
//	Determine the memory mapped file size.
func (mariInst *Mari) FileSize() (int, error) {
	stat, statErr := mariInst.File.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())
	return size, nil
}

// Remove
//	Close the Mari and remove the source file.
func (mariInst *Mari) Remove() error {
	closeErr := mariInst.Close()
	if closeErr != nil { return closeErr }

	removeErr := os.Remove(mariInst.File.Name())
	if removeErr != nil { return removeErr }

	return nil
}

// initializeFile
//	Initialize the memory mapped file to persist the hamt.
//	If file size is 0, initiliaze the file size to 64MB and set the initial metadata and root values into the map.
//	Otherwise, just map the already initialized file into the memory map.
func (mariInst *Mari) initializeFile() error {
	fSize, fSizeErr := mariInst.FileSize()
	if fSizeErr != nil { return fSizeErr }

	switch {
		case fSize == 0:
			_, resizeErr := mariInst.resizeMmap()
			if resizeErr != nil { return resizeErr }

			endOffset, initRootErr := mariInst.initRoot()
			if initRootErr != nil { return initRootErr }

			initMetaErr := mariInst.initMeta(endOffset)
			if initMetaErr != nil { return initMetaErr }
		default:
			mmapErr := mariInst.mMap()
			if mmapErr != nil { return mmapErr }
	}

	return nil
}