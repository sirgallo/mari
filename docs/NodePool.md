# NodePool


## Issue

Path copies can be an expensive operation and can put pressure on the go garbage collector when node copies are no longer being used.


## Solution

A NodePool is employed to recycle and pre-allocate internal/leaf nodes. When nodes are copied or created, pre-allocated nodes are fetched from the node pool. On successful serialization and failed `compare-and-swap` operations, instead of relying on the garbage collector, nodes are placed back into pool and reset to default values to be reused on following operations.


## Usage

The `NodePoolSize` option is used for defining the total number of internal/leaf nodes to be pre-allocated and recycled. If the option is not passed, then a default node pool size is utilized.
```go
package main

import "os"
import "path/filepath"

import "github.com/sirgallo/mari"


const FILENAME = "<your-file-name>"


func main() {
  homedir, homedirErr := os.UserHomeDir()
  if homedirErr != nil { panic(homedirErr.Error()) }
  
  nodePoolSize := int64(1000000)
  opts := mari.MariOpts{ 
    Filepath: filepath,
    FileName: FILENAME,
    NodePoolSize: &nodePoolSize,
  }

  // open mari
  mariInst, openErr := mari.Open(opts)
  if openErr != nil { panic(openErr.Error()) }
  
  // close mari
  defer mariInst.Close()
}
```