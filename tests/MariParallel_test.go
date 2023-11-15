package maritests

import "bytes"
import "fmt"
import "os"
import "path/filepath"
import "sync"
import "testing"

import "github.com/sirgallo/mari"


var pTestPath = filepath.Join(os.TempDir(), "testparallel")
var parallelMariInst *mari.Mari
var initKeyValPairs []KeyVal
var pKeyValPairs []KeyVal
var pInitMariErr error
var pInsertWG, pRetrieveWG sync.WaitGroup


func setup() {
	os.Remove(pTestPath)
	
	opts := mari.MariOpts{ 
		Filepath: pTestPath,
		NodePoolSize: NODEPOOL_SIZE, 
	}
	
	parallelMariInst, pInitMariErr = mari.Open(opts)
	if pInitMariErr != nil {
		parallelMariInst.Remove()
		panic(pInitMariErr.Error())
	}

	fmt.Println("parallel test mari initialized")

	initKeyValPairs = make([]KeyVal, INPUT_SIZE)
	pKeyValPairs = make([]KeyVal, PWRITE_INPUT_SIZE)

	for idx := range initKeyValPairs {
		iRandomBytes, _ := GenerateRandomBytes(32)
		initKeyValPairs[idx] = KeyVal{ Key: iRandomBytes, Value: iRandomBytes }
	}

	for idx := range pKeyValPairs {
		pRandomBytes, _ := GenerateRandomBytes(32)
		pKeyValPairs[idx] = KeyVal{ Key: pRandomBytes, Value: pRandomBytes }
	}

	fmt.Println("seeding parallel test mari")

	for _, val := range initKeyValPairs {
		putErr := parallelMariInst.UpdateTx(func(tx *mari.MariTx) error {
			putTxErr := tx.Put(val.Key, val.Value)
			if putTxErr != nil { return putTxErr }

			return nil
		})
		
		if putErr != nil { panic(putErr.Error()) }
	}

	fmt.Println("finished seeding parallel test mari")
}

func cleanup() {
	parallelMariInst.Remove()
}


func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	cleanup()

	os.Exit(code)
}

func TestMariParallelReadWrites(t *testing.T) {
	t.Run("Test Read Init Key Vals In MMap", func(t *testing.T) {
		t.Parallel()

		readData := initKeyValPairs[:len(initKeyValPairs) - PWRITE_INPUT_SIZE]

		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := readData[i * PCHUNK_SIZE_READ:(i + 1) * PCHUNK_SIZE_READ]

			pRetrieveWG.Add(1)
			go func() {
				defer pRetrieveWG.Done()

				for _, val := range chunk {
					var kvPair *mari.KeyValuePair
					getErr := parallelMariInst.ReadTx(func(tx *mari.MariTx) error {
						var getTxErr error
						kvPair, getTxErr = tx.Get(val.Key, nil)
						if getTxErr != nil { return getTxErr }

						return nil
					})

					if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }

					if ! bytes.Equal(kvPair.Value, val.Value) {
						t.Errorf("actual value not equal to expected: actual(%s), expected(%s)", kvPair.Value, val.Value)
					}
				}
			}()
		}

		pRetrieveWG.Wait()
	})

	t.Run("Test Write New Key Vals In MMap", func(t *testing.T) {
		t.Parallel()

		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			chunk := pKeyValPairs[i * PCHUNK_SIZE_WRITE:(i + 1) * PCHUNK_SIZE_WRITE]

			pInsertWG.Add(1)
			go func() {
				defer pInsertWG.Done()

				for _, val := range chunk {
					putErr := parallelMariInst.UpdateTx(func(tx *mari.MariTx) error {
						putTxErr := tx.Put(val.Key, val.Value)
						if putTxErr != nil { return putTxErr }

						return nil
					})

					if putErr != nil { t.Errorf("error on mari put: %s", putErr.Error()) }
				}
			}()
		}

		pInsertWG.Wait()
	})
}