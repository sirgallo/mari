package mari

import "errors"


//============================================= Mari Compact Utils


// mMapTemp
//	Mmap helper for the temporary memory mapped file.
func (compact *MariCompaction) mMapTemp() error {
	temp, tempErr := Map(compact.tempFile, RDWR, 0)
	if tempErr != nil { return tempErr }

	compact.tempData.Store(temp)

	return nil
}

// munmapTemp
//	Unmap helper for the tempory memory mapped file
func (compact *MariCompaction) munmapTemp() error {
	temp := compact.tempData.Load().(MMap)
	unmapErr := temp.Unmap()
	if unmapErr != nil { return unmapErr }

	compact.tempData.Store(MMap{})
	return nil
}

// resizeTempFile
//	As the new copy is being built, the file will need to be resized as more elements are appended.
//	Follow a similar strategy to the resizeFile method for the mari memory map.
func (compact *MariCompaction) resizeTempFile(offset uint64) error {
	temp := compact.tempData.Load().(MMap)
	if offset > 0 && int(offset) < len(temp) { return nil }
	
	allocateSize := func() int64 {
		switch {
			case len(temp) == 0:
				return int64(DefaultPageSize) * 16 * 1000 // 64MB
			case len(temp) >= MaxResize:
				return int64(len(temp) + MaxResize)
			default:
				return int64(len(temp) * 2)
		}
	}()

	if len(temp) > 0 {
		flushErr := compact.tempFile.Sync()
		if flushErr != nil { return flushErr }
		
		unmapErr := compact.munmapTemp()
		if unmapErr != nil { return unmapErr }
	}

	truncateErr := compact.tempFile.Truncate(allocateSize)
	if truncateErr != nil { return truncateErr }

	mmapErr := compact.mMapTemp()
	if mmapErr != nil { return mmapErr }
	
	return nil
}

// writeMetaToTempMemMap
//	Copy the serialized metadata into the memory map.
func (compact *MariCompaction) writeMetaToTempMemMap(sMeta []byte) (ok bool, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ok = false
			err = errors.New("error writing metadata to mmap")
		}
	}()

	temp := compact.tempData.Load().(MMap)
	copy(temp[MetaVersionIdx:MetaEndSerializedOffset + OffsetSize], sMeta)

	flushErr := compact.tempFile.Sync()
	if flushErr != nil { return false, flushErr }

	return true, nil
}