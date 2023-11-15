package mari

import "bytes"
import "errors"
import "runtime"
import "sync/atomic"
import "unsafe"


//============================================= Mari Transaction


// newTx
//	Creates a new transaction.
//	The current root is operated on for "Optimistic Concurrency Control".
//	If isWrite is false, then write operations in the read only transaction will fail.
func newTx(mariInst *Mari, rootPtr *unsafe.Pointer, isWrite bool) *MariTx {
	return &MariTx{
		store: mariInst,
		root: rootPtr,
		isWrite: isWrite,
	}
}

// ViewTx
//	Handles all read related operations.
//	It gets the latest version of the ordered array mapped trie and starts from that offset in the mem-map.
//	Get is concurrent since it will perform the operation on an existing path, so new paths can be written at the same time with new versions.
func (mariInst *Mari) ViewTx(txOps func(tx *MariTx) error) error {
	_, rootOffset, loadROffErr := mariInst.loadMetaRootOffset()
	if loadROffErr != nil { return loadROffErr }

	currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
	if readRootErr != nil { return readRootErr }

	rootPtr := storeINodeAsPointer(currRoot)

	transaction := newTx(mariInst, rootPtr, false)
	viewErr := txOps(transaction)
	if viewErr != nil { return viewErr }

	return nil
}

// UpdateTx
//	Handles all write related operations.
//	If the operation fails, the copied and modified path is discarded and the operation retries back at the root until completed.
//	The operation begins at the latest known version of root, reads from the metadata in the memory map.
//  The version of the copy is incremented and if the metadata is the same after the path copying has occured, the path is serialized and appended to the memory-map.
//	The metadata is also being updated to reflect the new version and the new root offset.
func (mariInst *Mari) UpdateTx(txOps func(tx *MariTx) error) error {
	for {
		for atomic.LoadUint32(&mariInst.IsResizing) == 1 { runtime.Gosched() }
		mariInst.RWResizeLock.RLock()

		versionPtr, version, loadVErr := mariInst.loadMetaVersion()
		if loadVErr != nil { return loadVErr }

		if version == atomic.LoadUint64(versionPtr) {
			_, rootOffset, loadROffErr := mariInst.loadMetaRootOffset()
			if loadROffErr != nil { return loadROffErr }
	
			currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
			if readRootErr != nil {
				mariInst.RWResizeLock.RUnlock()
				return readRootErr
			}
	
			currRoot.Version = currRoot.Version + 1
			rootPtr := storeINodeAsPointer(currRoot)
			
			transaction := newTx(mariInst, rootPtr, true)
			updateErr := txOps(transaction)
			if updateErr != nil { return updateErr }

			updatedRootCopy := loadINodeFromPointer(rootPtr)
			ok, writeErr := mariInst.exclusiveWriteMmap(updatedRootCopy)
			if writeErr != nil {
				mariInst.RWResizeLock.RUnlock()
				return writeErr
			}

			if ok {
				mariInst.RWResizeLock.RUnlock() 
				return nil
			}
		}

		mariInst.RWResizeLock.RUnlock()
		runtime.Gosched()
	}
}

// Put inserts or updates key-value pair into the ordered array mapped trie.
//	The operation begins at the root of the trie and traverses through the tree until the correct location is found, copying the entire path.
func (tx *MariTx) Put(key, value []byte) error {
	if ! tx.isWrite { return errors.New("attempting to perform a write in a read only transaction, use tx.UpdateTx") }

	_, putErr := tx.store.putRecursive(tx.root, key, value, 0)
	if putErr != nil { return putErr }
	
	return nil
}

// Get
//	Attempts to retrieve the value for a key within the ordered array mapped trie.
//	The operation begins at the root of the trie and traverses down the path to the key.
func (tx *MariTx) Get(key []byte, transform *MariOpTransform) (*KeyValuePair, error) {
	var newTransform MariOpTransform
	if transform != nil {
		newTransform = *transform
	} else { newTransform = func(kvPair *KeyValuePair) *KeyValuePair { return kvPair } }

	return tx.store.getRecursive(tx.root, key, 0, newTransform)
}

// Delete attempts to delete a key-value pair within the ordered array mapped trie.
//	It starts at the root of the trie and recurses down the path to the key to be deleted.
//	The operation creates an entire, in-memory copy of the path down to the key.
func (tx *MariTx) Delete(key []byte) error {
	if ! tx.isWrite { return errors.New("attempting to perform a write in a read only transaction, use tx.UpdateTx") }

	_, delErr := tx.store.deleteRecursive(tx.root, key, 0)
	if delErr != nil { return delErr }
	
	return nil
}

// Iterate
//	Creates an ordered iterator starting at the given start key up to the range specified by total results.
//	Since the array mapped trie is sorted, the iterate function starts at the startKey and recursively builds the result set up the specified end.
//	A minimum version can be provided which will limit results to the min version forward.
//	If nil is passed for the minimum version, the earliest version in the structure will be used.
// 	If nil is passed for the transformer, then the kv pair will be returned as is.
func (tx *MariTx) Iterate(startKey []byte, totalResults int, opts *MariRangeOpts) ([]*KeyValuePair, error) {
	var minV uint64 
	var transform MariOpTransform
	
	if opts != nil && opts.MinVersion != nil {
		minV = *opts.MinVersion
	} else { minV = 0 }

	if opts != nil && opts.Transform != nil {
		transform = *opts.Transform
	} else { transform = func(kvPair *KeyValuePair) *KeyValuePair { return kvPair } }

	accumulator := []*KeyValuePair{}
	kvPairs, iterErr := tx.store.iterateRecursive(tx.root, minV, startKey, totalResults, 0, accumulator, transform)
	if iterErr != nil { return nil, iterErr }

	return kvPairs, nil
}

// Range
//	Since the array mapped trie is sorted by nature, the range operation begins at the root of the trie.
//	It checks the root bitmap and determines which indexes to check in the range.
//	It then recursively checks each index, traversing the paths and building the sorted results.
//	A minimum version can be provided which will limit results to the min version forward.
//	If nil is passed for the minimum version, the earliest version in the structure will be used.
// 	If nil is passed for the transformer, then the kv pair will be returned as is.
func (tx *MariTx) Range(startKey, endKey []byte, opts *MariRangeOpts) ([]*KeyValuePair, error) {
	if bytes.Compare(startKey, endKey) == 1 { return nil, errors.New("start key is larger than end key") }

	var minV uint64 
	var transform MariOpTransform

	if opts != nil && opts.MinVersion != nil {
		minV = *opts.MinVersion
	} else { minV = 0 }

	if opts != nil && opts.Transform != nil {
		transform = *opts.Transform
	} else { transform = func(kvPair *KeyValuePair) *KeyValuePair { return kvPair } }

	kvPairs, rangeErr := tx.store.rangeRecursive(tx.root, minV, startKey, endKey, 0, transform)
	if rangeErr != nil { return nil, rangeErr }

	return kvPairs, nil
}