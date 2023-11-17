package mari

import "fmt"
import "os"
import "runtime"
import "sync/atomic"
import "unsafe"


//============================================= Mari Compact


// newCompaction
//	Instatiate the compaction strategy on compaction signal.
//	Creates a new temporary memory mapped file where the version to be snapshotted will be written to.
func (mariInst *Mari) newCompaction(compactedVersion uint64) (*MariCompaction, error) {
	tempFileName := mariInst.file.Name() + "temp"

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	tempFile, openTempFileErr := os.OpenFile(tempFileName, flag, 0600)
	if openTempFileErr != nil { return nil, openTempFileErr }

	compact := &MariCompaction{ 
		tempFile: tempFile,
		compactedVersion: compactedVersion,
	}

	compact.tempData.Store(MMap{})

	resizeErr := compact.resizeTempFile(0)
	if resizeErr != nil { return nil, resizeErr }

	return compact, nil
}

// signalCompact
//	When the maximum version is reached, signal the compaction go routine
func (mariInst *Mari) signalCompact() {
	select { 
		case mariInst.signalCompactChan <- true:
		default:
	}
}

// compactHandler
//	Run in a separate go routine.
//	On signal, sets the resizing flag and acquires the write lock.
//	The current root is loaded and then the elements are recursively written to the new file.
//	On completion, the original memory mapped file is removed and the new file is swapped in.
func (mariInst *Mari) compactHandler() {
	for range mariInst.signalCompactChan {
		compactErr := func() error {
			for ! atomic.CompareAndSwapUint32(&mariInst.isResizing, 0, 1) { runtime.Gosched() }
			defer atomic.StoreUint32(&mariInst.isResizing, 0)

			mariInst.rwResizeLock.Lock()
			defer mariInst.rwResizeLock.Unlock()

			_, rootOffset, loadROffErr := mariInst.loadMetaRootOffset()
			if loadROffErr != nil { return loadROffErr }
		
			currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
			if readRootErr != nil { return readRootErr }
		
			compact, newCompactStratErr := mariInst.newCompaction(currRoot.version)
			if newCompactStratErr != nil { return newCompactStratErr }
		
			currRootPtr := storeINodeAsPointer(currRoot)
			endOff, serializeVersionErr := mariInst.serializeCurrentVersionToNewFile(compact, currRootPtr, 0, 0, InitRootOffset)
			if serializeVersionErr != nil { 
				os.Remove(compact.tempFile.Name())
				return serializeVersionErr 
			}
		
			newMeta := &MariMetaData{
				version: 0,
				rootOffset: uint64(InitRootOffset),
				nextStartOffset: endOff,
			}
		
			serializedMeta := newMeta.serializeMetaData()
			_, writeErr := compact.writeMetaToTempMemMap(serializedMeta)
			if writeErr != nil { 
				os.Remove(compact.tempFile.Name())
				return writeErr 
			}
			
			swapErr := mariInst.swapTempFileWithMari(compact)
			if swapErr != nil { 
				os.Remove(compact.tempFile.Name())
				return swapErr 
			}

			return nil
		}()

		if compactErr != nil { fmt.Println("error on compaction process:", compactErr) }
	}
}

// serializeCurrentVersionToNewFile
//	Recursively builds the new copy of the current version to the new file.
//	All previous unused paths are discarded.
//	At each level, the nodes are directly written to the memory map as to avoid loading the entire structure into memory.
func (mariInst *Mari) serializeCurrentVersionToNewFile(compact *MariCompaction, node *unsafe.Pointer, level int, version, offset uint64) (uint64, error) {
	currNode := loadINodeFromPointer(node)
	
	currNode.version = version
	currNode.startOffset = offset
	currNode.leaf.version = version

	sNode, serializeErr := currNode.serializeINode(true)
	if serializeErr != nil { return 0, serializeErr }

	serializedKeyVal, sLeafErr := currNode.leaf.serializeLNode()
	if sLeafErr != nil { return 0, sLeafErr }

	nextStartOffset := currNode.leaf.endOffset + 1

	if len(currNode.children) > 0 {
		for _, child := range currNode.children {
			sNode = append(sNode, serializeUint64(nextStartOffset)...)
	
			childNode, getChildErr := mariInst.readINodeFromMemMap(child.startOffset)
			if getChildErr != nil { return 0, getChildErr }
	
			childPtr := storeINodeAsPointer(childNode)
			updatedOffset, serializeErr := mariInst.serializeCurrentVersionToNewFile(compact, childPtr, level + 1, version, nextStartOffset)
			if serializeErr != nil { return 0, serializeErr }
	
			nextStartOffset = updatedOffset
		}
	}

	resizeErr := compact.resizeTempFile(currNode.leaf.endOffset + 1)
	if resizeErr != nil { return 0, resizeErr }

	sNode = append(sNode, serializedKeyVal...)

	temp := compact.tempData.Load().(MMap)
	copy(temp[currNode.startOffset:currNode.leaf.endOffset + 1], sNode)

	return nextStartOffset, nil
}

// swapTempFileWithMari
//	Close the current mari memory mapped file and swap the new compacted copy.
//	Rebuild the version index on compaction
func (mariInst *Mari) swapTempFileWithMari(compact *MariCompaction) error {
	currFileName := mariInst.file.Name()
	tempFileName := compact.tempFile.Name()
	swapFileName := mariInst.file.Name() + "swap"

	closeErr := mariInst.Close()
	if closeErr != nil { return closeErr }

	flushTempErr := compact.tempFile.Sync()
	if flushTempErr != nil { return flushTempErr }

	unmapTempErr := compact.munmapTemp()
	if unmapTempErr != nil { return unmapTempErr }

	closeTempErr := compact.tempFile.Close()
	if closeTempErr != nil { return closeTempErr }
	
	os.Rename(currFileName, swapFileName)
	os.Rename(tempFileName, currFileName)

	os.Remove(swapFileName)

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	
	var openFileErr error
	mariInst.file, openFileErr = os.OpenFile(currFileName, flag, 0600)
	if openFileErr != nil { return openFileErr }

	mmapErr := mariInst.mMap()
	if mmapErr != nil { return mmapErr }

	return nil
}