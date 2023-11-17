# Mari

## A Concurrent, Persistent, Embedded Key-Value Store


## Mari?

`mari` is named after the ancient Library of Mari, which was located in Mari, Syria around `1900 BC`. The library contained over 22,000 documents at its peak. 


## Overview 

`mari` is a simple, embedded key-value store that utilzes a memory mapped file to back the contents of the data. 

Data is stored in a concurrent ordered array mapped trie that utilizes versioning and is serialized to an append-only data structure containing all versions within the store. Concurrent operations are lock free, so multiple writers and readers can operate on the data in parallel. However, note that due to contention on writes, write performance may degrade as more writers attempt to write the memory map due to the nature of retries on atomic operations, while read performance will increase as more readers are added. Writers do not lock reads since reads operate on the current/previous version in the data. Since the trie is ordered, range operations and ordered iterations are supported, which are also concurrent and lock free. Writes that succeed are immediately flushed to disk to preserve data integrity. 

Every operation on mari is a transaction. Transactions can be either read only (`ReadTx`) or read-write (`UpdateTx`). Write operations will only modify the current version supplied in the transaction and will be isolated from updates to the data. If a transaction succeeds in full, it is written to the memory map, otherwise it is discarded and retried. This ensures that transactions are `ACID`. Read only transactions are also performed in isolation but can run while read-write operations are occuring. Batching writes into a single transaction can also significantly improve performance of writes, including any writes that occur after the batched write, since all paths for each write will be serialized together onto the memory map instead of writing paths one at a time. This also reduces the amount of node repeats on path copies so the overall footprint on the size of the memory mapped file will decrease. However, if batched writes are too large performance may degrade as the serialized path can take up a significant amount of memory.

Ordered iterations and range operations will also perform better than sequential lookups of singular keys as entire paths do not need to be traversed for each, while a singular key lookup requires full path traversal. If a range of values is required for a lookup, consider using the `Iterate` or `Range` function.

Transforms can be created for read operations to transform results before being returned to the user. This can be useful for situations where data pre-processing is required.

The `NodePoolSize` option is used for defining the total number of internal/leaf nodes to be pre-allocated and recycled. The use of the nodepool helps to reduce the load on the garbage collector and allows for nodes to be reused instead of destroyed after use.

A compaction strategy can also be implemented as well, which is passed in the instance options using the `CompactTrigger` option. [Compaction](./docs/Compaction.md) is explained further in depth here.

This project is an exploration of memory mapped files and taking a different approach to storing and retrieving data within a database.


## Usage

```go
package main

import "os"
import "path/filepath"

import "github.com/sirgallo/mari"


const FILENAME = "<your-file-name>"


func main() {
  homedir, homedirErr := os.UserHomeDir()
  if homedirErr != nil { panic(homedirErr.Error()) }
  
  // set options
  opts := mari.MariOpts{ 
    Filepath: filepath,
    FileName: FILENAME,
    NodePoolSize: int64(1000000),
  }

  // open mari
  mariInst, openErr := mari.Open(opts)
  if openErr != nil { panic(openErr.Error()) }

  key := []byte("hello")
  value := []byte("world")

  // put a value in mari
  putErr := mariInst.UpdateTx(func(tx *mari.MariTx) error {
    putTxErr := tx.Put(key, value)
    if putTxErr != nil { return putTxErr }

    return nil
  })

  if putErr != nil { panic(putErr.Error()) }

  // get a value in mari
  // if transform is nil, kvPair is returned as is
  var kvPair *mari.KeyValuePair
  getErr := mariInst.ReadTx(func(tx *mari.MariTx) error {
    var getTxErr error
    kvPair, getTxErr = tx.Get(key, nil)
    if getTxErr != nil { return getTxErr }

    return nil
  })

  if getErr != nil { panic(getErr.Error()) }

  // get a set of ordered iterated key value pairs from a start key to the total result size
  // if opts is nil, version is set to the earliest version and transform will not be used
  var iteratedkvPairs []*mari.KeyValuePair
  iterErr := mariInst.ReadTx(func(tx *mari.MariTx) error {
    var iterTxErr error
    iteratedkvPairs, iterTxErr = tx.Iterate([]("hello"), 10000, nil)
    if iterTxErr != nil { return iterTxErr }

    return nil
  })

  if iterErr != nil { panic(iterErr.Error()) }

  // get a range of key-value pairs from a minimum version
  // if opts is nil, version is set to the earliest version and transform will not be used
  var rangekvPairs []*mari.KeyValuePair
  rangeErr := mariInst.ReadTx(func(tx *mari.MariTx) error {
    var rangeTxErr error
    rangekvPairs, rangeTxErr = tx.Range([]("hello"), []("world"), nil)
    if rangeTxErr != nil { return rangeTxErr }

    return nil
  })

  if rangeErr != nil { panic(rangeErr.Error()) }

  // create a transformer to process results before being returned
  transform := func(kvPair *mari.KeyValuePair) *mari.KeyValuePair {
    kvPair.Value = append(kvPair.Value, kvPair.Value...)
    return kvPair
  }

  // opts for range + iteration functions
  // can also contain MinVersion for the minimum version
  rangeOpts := &mari.MariRangeOpts{
    Transform: &transform
  }

  // get a transformed key-value in mari
  var transformedKvPair *mari.KeyValuePair
  getTransformedErr := mariInst.ReadTx(func(tx *mari.MariTx) error {
    var getTxErr error
    transformedKvPair, getTxErr = tx.Get(key, transform)
    if getTxErr != nil { return getTxErr }

    return nil
  })

  if getErr != nil { panic(getErr.Error()) }

  // get a range of key value pairs with transformed values
  var transformedRangePairs []*mari.KeyValuePair
  transformedRangeErr := mariInst.ReadTx(func(tx *mari.MariTx) error {
    var rangeTxErr error
    transformedRangePairs, rangeTxErr = tx.Range([]("hello"), []("world"), rangeOpts)
    if rangeTxErr != nil { return rangeTxErr }

    return nil
  })

  if rangeErr != nil { panic(rangeErr.Error()) }

  // get a set of ordered iterated key value pairs with transformed values
  var transformedIterPairs []*mari.KeyValuePair
  transformedIterErr := mariInst.ReadTx(func(tx *mari.MariTx) error {
    var iterTxErr error
    transformedIterPairs, iterTxErr = tx.Iterate([]("hello"), 10000, rangeOpts)
    if iterTxErr != nil { return iterTxErr }

    return nil
  })

  if transformedIterErr != nil { panic(iterErr.Error()) }

  // perform a mixed read-write transaction
  var mixedKvPair *mari.KeyValuePair
  mixedErr := mariInst.UpdateTx(func(tx *mari.MariTx) error {
    putTxErr := tx.Put([]byte("key1"), []byte("value1"))
    if putTxErr != nil { return putTxErr }

    putTxErr = tx.Put(]byte("key2"), []byte("value2"))
    if putTxErr != nil { return putTxErr }

    var getTxErr error
    mixedKvPair, getTxErr = tx.Get(key)
    if getTxErr != nil { return getTxErr }

    return nil
  })

  if mixedErr != nil { panic(mixedErr.Error()) }

  // delete a value in mari
  delErr := mariInst.UpdateTx(func(tx *mari.MariTx) error {
    delTxErr := tx.Delete(key)
    if delTxErr != nil { return delTxErr }

    return nil
  })

  if delErr != nil { panic(delErr.Error()) }

  // get mari filesize
  fSize, sizeErr := mariInst.FileSize()
  if sizeErr != nil { panic(sizeErr.Error()) }

  // close mari
  closeErr := mariInst.Close()
  if closeErr != nil { panic(closeErr.Error()) }

  // close mari and remove the associated file
  removeErr := mariInst.Remove()
  if removeErr != nil { panic(removeErr.Error()) }
}
```


## Tests

`mari`
```bash
go test -v ./tests
```


## godoc

For in depth definitions of types and functions, `godoc` can generate documentation from the formatted function comments. If `godoc` is not installed, it can be installed with the following:
```bash
go install golang.org/x/tools/cmd/godoc
```

To run the `godoc` server and view definitions for the package:
```bash
godoc -http=:6060
```

Then, in your browser, navigate to:
```
http://localhost:6060/pkg/github.com/sirgallo/mari/
```


## Note

**currently only tested on unix systems**

The `mmap` function utilizes `golang.org/x/sys/unix`, so the mmap functionality will only work on unix based systems. Builds for other operating systems can be done but have not been explored or implemented yet.


## Sources

[COMap](./docs/COMap.md)

[Compaction](./docs/Compaction.md)

[Concepts](./docs/Concepts.md)