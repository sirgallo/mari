package maritests

import "os"
import "fmt"
import "path/filepath"
import "testing"

import "github.com/sirgallo/mari"


var TestPath = filepath.Join(os.TempDir(), "testmari")
var mariInst *mari.Mari


func init() {
	var initPCMapErr error
	os.Remove(TestPath)
	
	opts := mari.MariOpts{ Filepath: TestPath }
	mariInst, initPCMapErr = mari.Open(opts)
	if initPCMapErr != nil { panic(initPCMapErr.Error()) }

	fmt.Println("op test mari initialized")
}


func TestMari(t *testing.T) {
	defer mariInst.Remove()

	var delErr, getErr, putErr error
	var val1, val2, val3, val4 *mari.KeyValuePair

	t.Run("Test Mari Put", func(t *testing.T) {
		_, putErr = mariInst.Put([]byte("hello"), []byte("world"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("new"), []byte("wow!"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("again"), []byte("test!"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("woah"), []byte("random entry"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("key"), []byte("Saturday!"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("sup"), []byte("6"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("final"), []byte("the!"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("asdfasdf"), []byte("add 10"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("asdfasdf"), []byte("123123"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("asd"), []byte("queue!"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("fasdf"), []byte("interesting"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("yup"), []byte("random again!"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("asdf"), []byte("hello"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("asdffasd"), []byte("uh oh!"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("fasdfasdfasdfasdf"), []byte("error message"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("fasdfasdf"), []byte("info!"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		_, putErr = mariInst.Put([]byte("woah"), []byte("done"))
		if putErr != nil { t.Errorf("error putting key in mari: %s", putErr.Error()) }

		t.Logf("mariInst after inserts")
		mariInst.PrintChildren()
	})

	t.Run("Test Mari Get", func(t *testing.T) {
		expVal1 := "world"
		val1, getErr = mariInst.Get([]byte("hello"))
		if getErr != nil { t.Errorf("error getting val1: %s", getErr.Error()) }
		if val1 == nil { t.Error("val actually nil") }

		t.Logf("actual: %s, expected: %s", string(val1.Value), expVal1)
		if string(val1.Value) != expVal1 { t.Errorf("val 1 does not match expected val 1: actual(%s), expected(%s)\n", val1.Value, expVal1) }

		expVal2 := "wow!"
		val2, getErr = mariInst.Get([]byte("new"))
		if getErr != nil { t.Errorf("error getting val2: %s", getErr.Error()) }
		if val2 == nil { t.Error("val actually nil") }

		t.Logf("actual: %s, expected: %s", val2.Value, expVal2)
		if string(val2.Value) != expVal2 { t.Errorf("val 2 does not match expected val 2: actual(%s), expected(%s)\n", val2.Value, expVal2) }

		expVal3 := "hello"
		val3, getErr = mariInst.Get([]byte("asdf"))
		if getErr != nil { t.Errorf("error getting val3: %s", getErr.Error()) }
		if val3 == nil { t.Error("val actually nil") }
		
		t.Logf("actual: %s, expected: %s", val3.Value, expVal3)
		if string(val3.Value) != expVal3 { t.Errorf("val 3 does not match expected val 3: actual(%s), expected(%s)", val3.Value, expVal3) }

		expVal4 := "123123"
		val4, getErr = mariInst.Get([]byte("asdfasdf"))
		if getErr != nil { t.Errorf("error getting val4: %s", getErr.Error()) }
		if val4 == nil { t.Error("val actually nil") }

		t.Logf("actual: %s, expected: %s", val4.Value, expVal4)
		if string(val4.Value) != expVal4 { t.Errorf("val 4 does not match expected val 4: actual(%s), expected(%s)", val4.Value, expVal4) }
	})

	t.Run("Test Range Operation", func(t *testing.T) {
		kvPairs, getErr := mariInst.Range([]byte("hello"), []byte("yup"), nil)
		if getErr != nil { t.Errorf("error on mari range: %s", getErr.Error()) }

		t.Log("keys in kv pairs", func() []string{
			var keys []string
			for _, kv := range kvPairs { 
				keys = append(keys, string(kv.Key))
			}

			return keys
		}())

		isSorted := IsSorted(kvPairs)
		t.Logf("is sorted: %t", isSorted)

		if ! isSorted {
			t.Errorf("key value pairs are not in sorted order: %t", isSorted)
		}
	})

	t.Run("Test Mari Delete", func(t *testing.T) {
		_, delErr = mariInst.Delete([]byte("hello"))
		if delErr != nil { t.Errorf("error deleting key from mari: %s", delErr.Error()) }

		_, delErr = mariInst.Delete([]byte("yup"))
		if delErr != nil { t.Errorf("error deleting key from mari: %s", delErr.Error()) }

		_, delErr = mariInst.Delete([]byte("asdf"))
		if delErr != nil { t.Errorf("error deleting key from mari: %s", delErr.Error()) }

		_, delErr = mariInst.Delete([]byte("asdfasdf"))
		if delErr != nil { t.Errorf("error deleting key from mari: %s", delErr.Error()) }

		_, delErr = mariInst.Delete([]byte("new"))
		if delErr != nil { t.Errorf("error deleting key from mari: %s", delErr.Error()) }

		t.Log("mari after deletes")
		mariInst.PrintChildren()
	})

	t.Log("Done")
}