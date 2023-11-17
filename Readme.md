# Mari

## A Concurrent, Persistent, Embedded Key-Value Store


## Mari?

`mari` is named after the ancient Library of Mari, which was located in Mari, Syria around `1900 BC`. The library contained over 22,000 documents at its peak. 


## Overview 

`mari` is a simple, embedded key-value store that utilzes a memory mapped file to back the contents of the data, implemented purely in `Go`. This project is an exploration of memory mapped files and taking a different approach to storing and retrieving data within a database.

Data is stored in a concurrent ordered array mapped trie that utilizes versioning and is serialized to an append-only data structure containing all versions within the store. Concurrent operations are lock free, so multiple writers and readers can operate on the data in parallel, utilizing a form of `MVCC`. Successful writes are immediately flushed to disk to preserve data integrity. 

Every operation on `mari` is a transaction. Transactions can be either read only (`ReadTx`) or read-write (`UpdateTx`). Write operations will only modify the current version supplied in the transaction and will be isolated from updates to the data. Transforms can be created for read operations to mutate results before being returned to the user. This can be useful for situations where data pre-processing is required. For more information on transactions, check out [Transactions](./docs/Transactions.md).

Since the trie is ordered, range operations and ordered iterations are supported, which are also concurrent and lock free. Ordered iterations and range operations will also perform better than sequential lookups of singular keys as entire paths do not need to be traversed for each, while a singular key lookup requires full path traversal. If a range of values is required for a lookup, consider using `tx.Iterate` or `tx.Range`.

A compaction strategy can also be implemented as well, which is passed in the instance options using the `CompactTrigger` option. [Compaction](./docs/Compaction.md) is explained further in depth here.

To alleviate pressure on the `Go` garbage collector, a node pool is also utilized, which is explained here [NodePool](./docs/NodePool.md).


## Usage

```go
package main

import "os"

import "github.com/sirgallo/mari"


const FILENAME = "<your-file-name>"


func main() {
  homedir, homedirErr := os.UserHomeDir()
  if homedirErr != nil { panic(homedirErr.Error()) }
  
  opts := mari.MariOpts{ Filepath: homedir, FileName: FILENAME }

  mariInst, openErr := mari.Open(opts)
  if openErr != nil { panic(openErr.Error()) }
  defer mariInst.Close()

  putErr := mariInst.UpdateTx(func(tx *mari.MariTx) error {
    putTxErr := tx.Put([]byte("hello"), []byte("world"))
    if putTxErr != nil { return putTxErr }

    return nil
  })

  if putErr != nil { panic(putErr.Error()) }

  var kvPair *mari.KeyValuePair
  getErr := mariInst.ReadTx(func(tx *mari.MariTx) error {
    var getTxErr error
    kvPair, getTxErr = tx.Get([]byte("hello"), nil)
    if getTxErr != nil { return getTxErr }

    return nil
  })

  if getErr != nil { panic(getErr.Error()) }

  fSize, sizeErr := mariInst.FileSize()
  if sizeErr != nil { panic(sizeErr.Error()) }

  closeErr := mariInst.Close()
  if closeErr != nil { panic(closeErr.Error()) }

  removeErr := mariInst.Remove()
  if removeErr != nil { panic(removeErr.Error()) }
}
```


## Tests

`mari`
```bash
go test -v ./tests
```

Tests are explained further in depth here [Tests](./docs/Tests.md)

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

[NodePool](./docs/NodePool.md)

[Tests](./docs/Tests.md)

[Transactions](./docs/Transactions.md)