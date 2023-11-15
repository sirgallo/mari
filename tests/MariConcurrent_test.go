package maritests

import "bytes"
import "fmt"
import "os"
import "path/filepath"
import "sync"
import "sync/atomic"
import "testing"

import "github.com/sirgallo/mari"


var cTestPath = filepath.Join(os.TempDir(), "testconcurrent")
var concurrentMariInst *mari.Mari
var keyValPairs []KeyVal
var initMariErr error
var delWG, insertWG, iterWG, retrieveWG, rangeWG sync.WaitGroup


func init() {
	os.Remove(cTestPath)
	
	opts := mari.MariOpts{ Filepath: cTestPath }
	concurrentMariInst, initMariErr = mari.Open(opts)
	if initMariErr != nil {
		concurrentMariInst.Remove()
		panic(initMariErr.Error())
	}

	fmt.Println("concurrent test mari initialized")

	keyValPairs = make([]KeyVal, INPUT_SIZE)

	for idx := range keyValPairs {
		randomBytes, _ := GenerateRandomBytes(32)
		keyValPairs[idx] = KeyVal{ Key: randomBytes, Value: randomBytes }
	}
}


func TestMariConcurrentOperations(t *testing.T) {
	defer concurrentMariInst.Remove()

	t.Run("Test Write Operations", func(t *testing.T) {
		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			chunk := keyValPairs[i * WRITE_CHUNK_SIZE:(i + 1) * WRITE_CHUNK_SIZE]

			insertWG.Add(1)
			go func () {
				defer insertWG.Done()
					for _, val := range chunk {
						putErr := concurrentMariInst.UpdateTx(func(tx *mari.MariTx) error {
							putTxErr := tx.Put(val.Key, val.Value)
							if putTxErr != nil { return putTxErr }

							return nil
						})
						
						if putErr != nil { t.Errorf("error on mari put: %s", putErr.Error()) }
					}
			}()
		}

		insertWG.Wait()
	})

	t.Run("Test Read Operations", func(t *testing.T) {
		defer concurrentMariInst.Close()
		
		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := keyValPairs[i * READ_CHUNK_SIZE:(i + 1) * READ_CHUNK_SIZE]
			
			retrieveWG.Add(1)
			go func() {
				defer retrieveWG.Done()

				for _, val := range chunk {
					var kvPair *mari.KeyValuePair
					getErr := concurrentMariInst.ViewTx(func(tx *mari.MariTx) error {
						var getTxErr error
						kvPair, getTxErr = tx.Get(val.Key, nil)
						if getTxErr != nil { return getTxErr }

						return nil
					})
					
					if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }

					if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
						t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
					}
				}
			}()
		}

		retrieveWG.Wait()
	})

	t.Run("Test Read Operations After Reopen", func(t *testing.T) {
		opts := mari.MariOpts{ Filepath: cTestPath }
		
		concurrentMariInst, initMariErr = mari.Open(opts)
		if initMariErr != nil {
			concurrentMariInst.Remove()
			t.Error("unable to open file")
		}

		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := keyValPairs[i * READ_CHUNK_SIZE:(i + 1) * READ_CHUNK_SIZE]

			retrieveWG.Add(1)
			go func() {
				defer retrieveWG.Done()

				for _, val := range chunk {
					var kvPair *mari.KeyValuePair
					getErr := concurrentMariInst.ViewTx(func(tx *mari.MariTx) error {
						var getTxErr error
						kvPair, getTxErr = tx.Get(val.Key, nil)
						if getTxErr != nil { return getTxErr }

						return nil
					})
					
					if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }

					if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
						t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
					}
				}
			}()
		}

		retrieveWG.Wait()
	})

	t.Run("Test Iterate Operation", func(t *testing.T) {
		totalElements := uint64(0)

		for range make([]int, NUM_READER_GO_ROUTINES) {
			first, _, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
			if randomErr != nil { t.Error("error generating random min max") }
	
			start := keyValPairs[first].Key

			iterWG.Add(1)
			go func() {
				defer iterWG.Done()

				var kvPairs []*mari.KeyValuePair
				iterErr := concurrentMariInst.ViewTx(func(tx *mari.MariTx) error {
					var iterTxErr error
					kvPairs, iterTxErr = tx.Iterate(start, ITERATE_SIZE, nil)
					if iterTxErr != nil { return iterTxErr }

					return nil
				})

				if iterErr != nil { t.Errorf("error on mari get: %s", iterErr.Error()) }
				
				atomic.AddUint64(&totalElements, uint64(len(kvPairs)))

				isSorted := IsSorted(kvPairs)
				if ! isSorted { t.Errorf("key value pairs are not in sorted order: %t", isSorted) }
			}()
		}

		iterWG.Wait()

		t.Log("total elements returned:", totalElements)
	})

	t.Run("Test Range Operation", func(t *testing.T) {
		totalElements := uint64(0)

		for range make([]int, NUM_READER_GO_ROUTINES) {
			first, second, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
			if randomErr != nil { t.Error("error generating random min max") }

			var start, end []byte
			switch {
				case bytes.Compare(keyValPairs[first].Key, keyValPairs[second].Key) == 1:
					start = keyValPairs[second].Key
					end = keyValPairs[first].Key
				default:
					start = keyValPairs[first].Key
					end = keyValPairs[second].Key
			}

			rangeWG.Add(1)
			go func() {
				defer rangeWG.Done()

				var kvPairs []*mari.KeyValuePair
				rangeErr := concurrentMariInst.ViewTx(func(tx *mari.MariTx) error {
					var rangeTxErr error
					kvPairs, rangeTxErr = tx.Range(start, end, nil)
					if rangeTxErr != nil { return rangeTxErr }

					return nil
				})

				if rangeErr != nil { t.Errorf("error on mari get: %s", rangeErr.Error()) }
				
				atomic.AddUint64(&totalElements, uint64(len(kvPairs)))
				
				isSorted := IsSorted(kvPairs)
				if ! isSorted { t.Errorf("key value pairs are not in sorted order: %t", isSorted) }
			}()
		}
		
		rangeWG.Wait()

		t.Log("total elements returned:", totalElements)
	})

	t.Run("Test Delete Operations", func(t *testing.T) {
		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			chunk := keyValPairs[i * WRITE_CHUNK_SIZE:(i + 1) * WRITE_CHUNK_SIZE]

			delWG.Add(1)
			go func() {
				defer delWG.Done()

				for _, val := range chunk {
					delErr := concurrentMariInst.UpdateTx(func(tx *mari.MariTx) error {
						delTxErr := tx.Delete(val.Key)
						if delTxErr != nil { return delTxErr }

						return nil
					})

					if delErr != nil { t.Errorf("error on mari delete: %s", delErr.Error()) }
				}
			}()
		}

		delWG.Wait()
	})

	t.Run("Mari File Size", func(t *testing.T) {
		fSize, sizeErr := concurrentMariInst.FileSize()
		if sizeErr != nil { t.Errorf("error getting file size: %s", sizeErr.Error()) }

		t.Log("File Size In Bytes:", fSize)
	})

	t.Log("Done")
}