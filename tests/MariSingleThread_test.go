package maritests

import "bytes"
import "fmt"
import "os"
import "path/filepath"
import "testing"

import "github.com/sirgallo/mari"


var stTestPath = filepath.Join(os.TempDir(), "testsinglethread")
var singleThreadTestMap *mari.Mari
var stkeyValPairs []KeyVal
var stInitMariErr error


func init() {
	os.Remove(stTestPath)
	
	opts := mari.MariOpts{ Filepath: stTestPath }
	singleThreadTestMap, stInitMariErr = mari.Open(opts)
	if stInitMariErr != nil {
		singleThreadTestMap.Remove()
		panic(stInitMariErr.Error())
	}

	fmt.Println("concurrent test mari initialized")

	stkeyValPairs = make([]KeyVal, INPUT_SIZE)

	for idx := range stkeyValPairs {
		randomBytes, _ := GenerateRandomBytes(32)
		stkeyValPairs[idx] = KeyVal{ Key: randomBytes, Value: randomBytes }
	}
}


func TestMariSingleThreadOperations(t *testing.T) {
	defer singleThreadTestMap.Remove()

	t.Run("Test Write Operations", func(t *testing.T) {
		for _, val := range stkeyValPairs {
			_, putErr := singleThreadTestMap.Put(val.Key, val.Value)
			if putErr != nil { t.Errorf("error on mari put: %s", putErr.Error()) }
		}

		// singleThreadTestMap.PrintChildren()
	})

	t.Run("Test Read Operations", func(t *testing.T) {
		defer singleThreadTestMap.Close()
		
		for _, val := range stkeyValPairs {
			kvPair, getErr := singleThreadTestMap.Get(val.Key)
			if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }
			
			if ! bytes.Equal(kvPair.Value, val.Value) {
				t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
			}
		}
	})

	t.Run("Test Read Operations After Reopen", func(t *testing.T) {
		opts := mari.MariOpts{ Filepath: stTestPath }
		
		singleThreadTestMap, stInitMariErr = mari.Open(opts)
		if stInitMariErr != nil {
			singleThreadTestMap.Remove()
			t.Error("unable to open file")
		}

		for _, val := range stkeyValPairs {
			kvPair, getErr := singleThreadTestMap.Get(val.Key)
			if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }

			if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
				t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
			}
		}
	})

	t.Run("Test Iterate Operation", func(t *testing.T) {
		first, _, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
		if randomErr != nil { t.Error("error generating random min max") }

		start := stkeyValPairs[first].Key

		kvPairs, rangeErr := singleThreadTestMap.Iterate(start, ITERATE_SIZE, nil)
		if rangeErr != nil { t.Errorf("error on mari get: %s", rangeErr.Error()) }
		
		t.Log("len kvPairs", len(kvPairs))
		
		isSorted := IsSorted(kvPairs)
		if ! isSorted { t.Errorf("key value pairs are not in sorted order1: %t", isSorted) }
	})

	t.Run("Test Range Operation", func(t *testing.T) {
		first, second, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
		if randomErr != nil { t.Error("error generating random min max") }

		var start, end []byte
		switch {
			case bytes.Compare(stkeyValPairs[first].Key, stkeyValPairs[second].Key) == 1:
				start = stkeyValPairs[second].Key
				end = stkeyValPairs[first].Key
			default:
				start = stkeyValPairs[first].Key
				end = stkeyValPairs[second].Key
		}

		kvPairs, rangeErr := singleThreadTestMap.Range(start, end, nil)
		if rangeErr != nil { t.Errorf("error on mari get: %s", rangeErr.Error()) }
		
		t.Log("len kvPairs", len(kvPairs))
		
		isSorted := IsSorted(kvPairs)
		if ! isSorted { t.Errorf("key value pairs are not in sorted order1: %t", isSorted) }
	})

	t.Run("Test Delete Operations", func(t *testing.T) {
		for _, val := range stkeyValPairs {
			_, delErr := singleThreadTestMap.Delete(val.Key)
			if delErr != nil { t.Errorf("error on mari delete: %s", delErr.Error()) }
		}
	})

	t.Run("Mari File Size", func(t *testing.T) {
		fSize, sizeErr := singleThreadTestMap.FileSize()
		if sizeErr != nil { t.Errorf("error getting file size: %s", sizeErr.Error()) }

		t.Log("File Size In Bytes:", fSize)
	})

	t.Log("Done")
}