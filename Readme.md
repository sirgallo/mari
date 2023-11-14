# Mari

## A Concurrent, Persistent, Embedded Key-Value Store


## Mari?

`mari` is named after the ancient Library of Mari, which was located in Mari, Syria around `1900 BC`. The library contained over 22,000 documents at its peak. 


## Overview 

`mari` is a simple, embedded key-value store that utilzes a memory mapped file to back the contents of the data. Data is stored in a concurrent ordered array mapped trie that utilizes versioning and is serialized to an append-only data structure containing all versions within the store. Concurrent operations are lock free and multiple writers and readers can be operate on the data in parallel. However, note that due to contention on writes, write performance may degrade as more writers attempt to write the memory map due to the nature of retries on atomic operations, while read performance will increase as more readers are added. Writers do not lock reads since reads operate on the current/previous version in the data. Since the trie is ordered, range operations are supported, which are also concurrent and lock free. Writes that succeed are immediately flushed to disk to preserve data integrity.

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

  // initialize mari filepath
  filepath := filepath.Join(homedir, FILENAME)
  opts := mari.MariOpts{ Filepath: filepath }

  // open mari
  mariInst, openErr := mari.Open(opts)
  if openErr != nil { panic(openErr.Error()) }

  key := []byte("hello")
  value := []byte("world")

  // put a value in mari
  _, putErr := mariInst.Put(key, value)
  if putErr != nil { panic(putErr.Error()) }

  // get a value in mari
  fetched, getErr := mariInst.Get(key)
  if getErr != nil { panic(getErr.Error()) }

  // get a range of key-value pairs from a minimum version
  // if minimum version is nil, version is set to the earliest version
  kvPairs, rangeErr := mariInst.Range([]("hello"), []("world"), nil)
  if rangeErr != nil { panic(rangeErr.Error()) }

  // delete a value in mari
  _, delErr := mariInst.Delete(key)
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

[Concepts](./docs/Concepts.md)