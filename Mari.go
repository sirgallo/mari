package mari

import "os"
import "fmt"
import "path/filepath"
import "sync/atomic"


//============================================= Mari


// Open initializes Mari
//	This will create the memory mapped file or read it in if it already exists.
//	Then, the meta data is initialized and written to the first 0-23 bytes in the memory map.
//	An initial root MariINode will also be written to the memory map as well.
func Open(opts MariOpts) (*Mari, error) {
	np := newMariNodePool(opts.NodePoolSize)	// let's initialize with 100,000 pre-allocated nodes
	
	fileWithFilePath := filepath.Join(opts.Filepath, opts.FileName)
	versionWithFilePath := filepath.Join(opts.Filepath, opts.FileName + VersionIndexFileName)

	mariInst := &Mari{
		filepath: opts.Filepath,
		opened: true,
		signalCompactChan: make(chan bool),
		signalFlushChan: make(chan bool),
		signalResizeChan: make(chan bool),
		nodePool: np,
	}

	if opts.CompactAtVersion != nil {
		compactVersion := *opts.CompactAtVersion
		
		if compactVersion > MaxCompactVersion {
			fmt.Println("compact version higher than", MaxCompactVersion, "setting to default...")
			mariInst.compactAtVersion = MaxCompactVersion
		} else { mariInst.compactAtVersion = compactVersion }
	} else { mariInst.compactAtVersion = MaxCompactVersion }

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	var openFileErr, openVIdxErr error

	mariInst.file, openFileErr = os.OpenFile(fileWithFilePath, flag, 0600)
	if openFileErr != nil { return nil, openFileErr	}

	mariInst.versionIndex, openVIdxErr = os.OpenFile(versionWithFilePath, flag, 0600)
	if openVIdxErr != nil { return nil, openVIdxErr }

	mariInst.filepath = opts.Filepath
	
	atomic.StoreUint32(&mariInst.isResizing, 0)
	
	mariInst.data.Store(MMap{})
	mariInst.vIdx.Store(MMap{})

	initFileErr := mariInst.initializeFile()
	if initFileErr != nil { return nil, initFileErr	}

	go mariInst.compactHandler()
	go mariInst.handleFlush()
	go mariInst.handleResize()

	return mariInst, nil
}

// Close
//	Close Mari, unmapping the file from memory and closing the file.
func (mariInst *Mari) Close() error {
	if ! mariInst.opened { return nil }
	mariInst.opened = false

	flushErr := mariInst.file.Sync()
	if flushErr != nil { return flushErr }

	unmapErr := mariInst.munmap()
	if unmapErr != nil { return unmapErr }

	flushVIdxErr := mariInst.versionIndex.Sync()
	if flushVIdxErr != nil { return flushVIdxErr }

	unmapVIdxErr := mariInst.munmapVIdx()
	if unmapVIdxErr != nil { return unmapVIdxErr }

	if mariInst.file != nil {
		closeErr := mariInst.file.Close()
		if closeErr != nil { return closeErr }
	}

	if mariInst.versionIndex != nil {
		closeErr := mariInst.versionIndex.Close()
		if closeErr != nil { return closeErr }
	}

	// mariInst.filepath = utils.GetZero[string]()
	return nil
}

// FileSize
//	Determine the memory mapped file size.
func (mariInst *Mari) FileSize() (int, error) {
	stat, statErr := mariInst.file.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())
	return size, nil
}

// Remove
//	Close Mari and remove the source file.
func (mariInst *Mari) Remove() error {
	closeErr := mariInst.Close()
	if closeErr != nil { return closeErr }

	removeErr := os.Remove(mariInst.file.Name())
	if removeErr != nil { return removeErr }

	removeErr = os.Remove(mariInst.versionIndex.Name())
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

			truncateErr := mariInst.versionIndex.Truncate(int64(DefaultPageSize) * 8 * 1000)
			if truncateErr != nil { return truncateErr }

			mmapErr := mariInst.mMapVIdx()
			if mmapErr != nil { return mmapErr }

			endOffset, initRootErr := mariInst.initRoot()
			if initRootErr != nil { return initRootErr }

			initMetaErr := mariInst.initMeta(endOffset)
			if initMetaErr != nil { return initMetaErr }
		default:
			mmapErr := mariInst.mMapVIdx()
			if mmapErr != nil { return mmapErr }

			mmapErr = mariInst.mMap()
			if mmapErr != nil { return mmapErr }
	}

	return nil
}