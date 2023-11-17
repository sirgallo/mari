package mari

import "errors"
import "sync/atomic"
import "unsafe"


//============================================= Mari Version


// loadStartOffset
//	Load the provided version's associated offset from the version index mmap
func (mariInst *Mari) loadStartOffset(version uint64) (ptr *uint64, v uint64, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ptr = nil
			v = 0
			err = errors.New("error getting version from mmap")
		}
	}()

	versionIndexOffset := (version * OffsetSize)

	vIdx := mariInst.vIdx.Load().(MMap)
	startOffsetPtr := (*uint64)(unsafe.Pointer(&vIdx[versionIndexOffset]))
	startOffset := atomic.LoadUint64(startOffsetPtr)

	return startOffsetPtr, startOffset, nil
}

// storeStartOffset
//	Store the startoffset for the provided version. 
//	The index in the version index is calculated by taking the version * the uint64 byte size
func (mariInst *Mari) storeStartOffset(version uint64, startOffset uint64) (err error) {
	defer func() {
		r := recover()
		if r != nil { 
			err = errors.New("error storing start offset value in vIdx")
		}
	}()

	versionIndexOffset := (version * OffsetSize)

	vIdx := mariInst.vIdx.Load().(MMap)
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&vIdx[versionIndexOffset])), startOffset)
	
	return nil
}

