package mari

import "fmt"
import "runtime"
import "sync/atomic"
import "unsafe"


//============================================= Mari IO Utils


// compareAndSwap
//	Performs CAS operation.
func (mariInst *Mari) compareAndSwap(node *unsafe.Pointer, currNode, nodeCopy *MariINode) bool {
	if atomic.CompareAndSwapPointer(node, unsafe.Pointer(currNode), unsafe.Pointer(nodeCopy)) {
		return true
	} else {
		mariInst.nodePool.iNodePool.Put(nodeCopy.leaf)
		mariInst.nodePool.lNodePool.Put(nodeCopy)

		return false
	}
}

// determineIfResize
//	Helper function that signals go routine for resizing if the condition to resize is met.
func (mariInst *Mari) determineIfResize(offset uint64) bool {
	mMap := mariInst.data.Load().(MMap)

	switch {
		case offset > 0 && int(offset) < len(mMap):
			return false
		case len(mMap) == 0 || ! atomic.CompareAndSwapUint32(&mariInst.isResizing, 0, 1):
			return true
		default:
			mariInst.signalResizeChan <- true
			return true
	}
}

// flushRegionToDisk
//	Flushes a region of the memory map to disk instead of flushing the entire map. 
//	When a startoffset is provided, if it is not aligned with the start of the last page, the offset needs to be normalized.
func (mariInst *Mari) flushRegionToDisk(startOffset, endOffset uint64) error {
	startOffsetOfPage := startOffset & ^(uint64(DefaultPageSize) - 1)

	mMap := mariInst.data.Load().(MMap)
	if len(mMap) == 0 { return nil }

	flushErr := mMap[startOffsetOfPage:endOffset].Flush()
	if flushErr != nil { return flushErr }

	return nil
}

// handleFlush
//	This is "optimistic" flushing. 
//	A separate go routine is spawned and signalled to flush changes to the mmap to disk.
func (mariInst *Mari) handleFlush() {
	for range mariInst.signalFlushChan {
		func() {
			for atomic.LoadUint32(&mariInst.isResizing) == 1 { runtime.Gosched() }
			
			mariInst.rwResizeLock.RLock()
			defer mariInst.rwResizeLock.RUnlock()

			flushErr := mariInst.file.Sync()
			if flushErr != nil { 
				fmt.Println("error flushing to disk") 
			} else { mariInst.versionIndex.Sync() }
		}()
	}
}

// handleResize
//	A separate go routine is spawned to handle resizing the memory map.
//	When the mmap reaches its size limit, the go routine is signalled.
func (mariInst *Mari) handleResize() {
	for range mariInst.signalResizeChan { mariInst.resizeMmap() }
}

// mmap
//	Helper to memory map the mariInst File in to buffer.
func (mariInst *Mari) mMap() error {
	mMap, mmapErr := Map(mariInst.file, RDWR, 0)
	if mmapErr != nil { return mmapErr }

	mariInst.data.Store(mMap)

	return nil
}

func (mariInst *Mari) mMapVIdx() error {
	vIdx, vIdxErr := Map(mariInst.versionIndex, RDWR, 0)
	if vIdxErr != nil { return vIdxErr }

	mariInst.vIdx.Store(vIdx)

	return nil
}

// munmap
//	Unmaps the memory map from RAM.
func (mariInst *Mari) munmap() error {
	mMap := mariInst.data.Load().(MMap)
	unmapErr := mMap.Unmap()
	if unmapErr != nil { return unmapErr }

	mariInst.data.Store(MMap{})
	return nil
}

func (mariInst *Mari) munmapVIdx() error {
	vIdx := mariInst.vIdx.Load().(MMap)
	unmapErr := vIdx.Unmap()
	if unmapErr != nil { return unmapErr }

	mariInst.vIdx.Store(MMap{})
	return nil
}

// resizeMmap
//	Dynamically resizes the underlying memory mapped file.
//	When a file is first created, default size is 64MB and doubles the mem map on each resize until 1GB.
func (mariInst *Mari) resizeMmap() (bool, error) {
	mariInst.rwResizeLock.Lock()
	
	defer mariInst.rwResizeLock.Unlock()
	defer atomic.StoreUint32(&mariInst.isResizing, 0)

	mMap := mariInst.data.Load().(MMap)

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
		flushErr := mariInst.file.Sync()
		if flushErr != nil { return false, flushErr }
		
		unmapErr := mariInst.munmap()
		if unmapErr != nil { return false, unmapErr }
	}

	truncateErr := mariInst.file.Truncate(allocateSize)
	if truncateErr != nil { return false, truncateErr }

	mmapErr := mariInst.mMap()
	if mmapErr != nil { return false, mmapErr }

	return true, nil
}

// signalFlush
//	Called by all writes to "optimistically" handle flushing changes to the mmap to disk.
func (mariInst *Mari) signalFlush() {
	select {
		case mariInst.signalFlushChan <- true:
		default:
	}
}

// exclusiveWriteMmap
//	Takes a path copy and writes the nodes to the memory map, then updates the metadata.
func (mariInst *Mari) exclusiveWriteMmap(path *MariINode) (bool, error) {
	if atomic.LoadUint32(&mariInst.isResizing) == 1 { return false, nil }

	versionPtr, version, loadVErr := mariInst.loadMetaVersion()
	if loadVErr != nil { return false, nil }

	rootOffsetPtr, prevRootOffset, loadROffErr := mariInst.loadMetaRootOffset()
	if loadROffErr != nil { return false, nil }

	endOffsetPtr, endOffset, loadSOffErr := mariInst.loadMetaEndSerialized()
	if loadSOffErr != nil { return false, nil }

	newVersion := path.version
	newOffsetInMMap := endOffset
	
	serializedPath, serializeErr := mariInst.serializePathToMemMap(path, newOffsetInMMap)
	if serializeErr != nil { return false, serializeErr }

	updatedMeta := &MariMetaData{
		version: newVersion,
		rootOffset: newOffsetInMMap,
		nextStartOffset: newOffsetInMMap + uint64(len(serializedPath)),
	}

	isResize := mariInst.determineIfResize(updatedMeta.nextStartOffset)
	if isResize { return false, nil }


	if version >= mariInst.compactAtVersion {
		mariInst.signalCompact()
		return false, nil
	}

	if atomic.LoadUint32(&mariInst.isResizing) == 0 {
		if version == updatedMeta.version - 1 && atomic.CompareAndSwapUint64(versionPtr, version, updatedMeta.version) {
			mariInst.storeMetaPointer(endOffsetPtr, updatedMeta.nextStartOffset)
			
			_, writeNodesToMmapErr := mariInst.writeNodesToMemMap(serializedPath, newOffsetInMMap)
			if writeNodesToMmapErr != nil {
				mariInst.storeMetaPointer(endOffsetPtr, endOffset)
				mariInst.storeMetaPointer(versionPtr, version)
				mariInst.storeMetaPointer(rootOffsetPtr, prevRootOffset)

				return false, writeNodesToMmapErr
			}
			
			mariInst.storeMetaPointer(rootOffsetPtr, updatedMeta.rootOffset)
			mariInst.storeStartOffset(updatedMeta.version, updatedMeta.rootOffset)

			mariInst.signalFlush()

			return true, nil
		}
	}

	return false, nil
}