package mari

import "runtime"
import "sync/atomic"


//============================================= Mari IO Utils


// determineIfResize
//	Helper function that signals go routine for resizing if the condition to resize is met.
func (mariInst *Mari) determineIfResize(offset uint64) bool {
	mMap := mariInst.Data.Load().(MMap)

	switch {
		case offset > 0 && int(offset) < len(mMap):
			return false
		case len(mMap) == 0 || ! atomic.CompareAndSwapUint32(&mariInst.IsResizing, 0, 1):
			return true
		default:
			mariInst.SignalResize <- true
			return true
	}
}

// flushRegionToDisk
//	Flushes a region of the memory map to disk instead of flushing the entire map. 
//	When a startoffset is provided, if it is not aligned with the start of the last page, the offset needs to be normalized.
func (mariInst *Mari) flushRegionToDisk(startOffset, endOffset uint64) error {
	startOffsetOfPage := startOffset & ^(uint64(DefaultPageSize) - 1)

	mMap := mariInst.Data.Load().(MMap)
	if len(mMap) == 0 { return nil }

	flushErr := mMap[startOffsetOfPage:endOffset].Flush()
	if flushErr != nil { return flushErr }

	return nil
}

// handleFlush
//	This is "optimistic" flushing. 
//	A separate go routine is spawned and signalled to flush changes to the mmap to disk.
func (mariInst *Mari) handleFlush() {
	for range mariInst.SignalFlush {
		func() {
			for atomic.LoadUint32(&mariInst.IsResizing) == 1 { runtime.Gosched() }
			
			mariInst.RWResizeLock.RLock()
			defer mariInst.RWResizeLock.RUnlock()

			mariInst.File.Sync()
		}()
	}
}

// handleResize
//	A separate go routine is spawned to handle resizing the memory map.
//	When the mmap reaches its size limit, the go routine is signalled.
func (mariInst *Mari) handleResize() {
	for range mariInst.SignalResize { mariInst.resizeMmap() }
}

// mmap
//	Helper to memory map the mariInst File in to buffer.
func (mariInst *Mari) mMap() error {
	mMap, mmapErr := Map(mariInst.File, RDWR, 0)
	if mmapErr != nil { return mmapErr }

	mariInst.Data.Store(mMap)
	return nil
}

// munmap
//	Unmaps the memory map from RAM.
func (mariInst *Mari) munmap() error {
	mMap := mariInst.Data.Load().(MMap)
	unmapErr := mMap.Unmap()
	if unmapErr != nil { return unmapErr }

	mariInst.Data.Store(MMap{})
	return nil
}

// resizeMmap
//	Dynamically resizes the underlying memory mapped file.
//	When a file is first created, default size is 64MB and doubles the mem map on each resize until 1GB.
func (mariInst *Mari) resizeMmap() (bool, error) {
	mariInst.RWResizeLock.Lock()
	
	defer mariInst.RWResizeLock.Unlock()
	defer atomic.StoreUint32(&mariInst.IsResizing, 0)

	mMap := mariInst.Data.Load().(MMap)

	allocateSize := func() int64 {
		switch {
			case len(mMap) == 0:
				return int64(DefaultPageSize) * 16 * 1000 // 64MB
			case len(mMap) >= MaxResize:
				return int64(len(mMap) + MaxResize)
			default:
				return int64(len(mMap) * 2)
		}
	}()

	if len(mMap) > 0 {
		flushErr := mariInst.File.Sync()
		if flushErr != nil { return false, flushErr }
		
		unmapErr := mariInst.munmap()
		if unmapErr != nil { return false, unmapErr }
	}

	truncateErr := mariInst.File.Truncate(allocateSize)
	if truncateErr != nil { return false, truncateErr }

	mmapErr := mariInst.mMap()
	if mmapErr != nil { return false, mmapErr }

	return true, nil
}

// signalFlush
//	Called by all writes to "optimistically" handle flushing changes to the mmap to disk.
func (mariInst *Mari) signalFlush() {
	select {
		case mariInst.SignalFlush <- true:
		default:
	}
}

// exclusiveWriteMmap
//	Takes a path copy and writes the nodes to the memory map, then updates the metadata.
func (mariInst *Mari) exclusiveWriteMmap(path *MariINode) (bool, error) {
	if atomic.LoadUint32(&mariInst.IsResizing) == 1 { return false, nil }

	versionPtr, version, loadVErr := mariInst.loadMetaVersion()
	if loadVErr != nil { return false, nil }

	rootOffsetPtr, prevRootOffset, loadROffErr := mariInst.loadMetaRootOffset()
	if loadROffErr != nil { return false, nil }

	endOffsetPtr, endOffset, loadSOffErr := mariInst.loadMetaEndSerialized()
	if loadSOffErr != nil { return false, nil }

	newVersion := path.Version
	newOffsetInMMap := endOffset
	
	serializedPath, serializeErr := mariInst.serializePathToMemMap(path, newOffsetInMMap)
	if serializeErr != nil { return false, serializeErr }

	updatedMeta := &MariMetaData{
		Version: newVersion,
		RootOffset: newOffsetInMMap,
		NextStartOffset: newOffsetInMMap + uint64(len(serializedPath)),
	}

	isResize := mariInst.determineIfResize(updatedMeta.NextStartOffset)
	if isResize { return false, nil }

	if atomic.LoadUint32(&mariInst.IsResizing) == 0 {
		if version == updatedMeta.Version - 1 && atomic.CompareAndSwapUint64(versionPtr, version, updatedMeta.Version) {
			mariInst.storeMetaPointer(endOffsetPtr, updatedMeta.NextStartOffset)
			
			_, writeNodesToMmapErr := mariInst.writeNodesToMemMap(serializedPath, newOffsetInMMap)
			if writeNodesToMmapErr != nil {
				mariInst.storeMetaPointer(endOffsetPtr, updatedMeta.NextStartOffset)
				mariInst.storeMetaPointer(versionPtr, version)
				mariInst.storeMetaPointer(rootOffsetPtr, prevRootOffset)

				return false, writeNodesToMmapErr
			}
			
			mariInst.storeMetaPointer(rootOffsetPtr, updatedMeta.RootOffset)
			mariInst.signalFlush()
			
			return true, nil
		}
	}

	return false, nil
}