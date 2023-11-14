package maritests

import "bytes"
import "crypto/rand"
import "errors"
import mrand "math/rand"

import "github.com/sirgallo/mari"


const NUM_WRITER_GO_ROUTINES = 10
const NUM_READER_GO_ROUTINES = 100
const INPUT_SIZE = 1000000
const ITERATE_SIZE = 50000
const PWRITE_INPUT_SIZE = INPUT_SIZE / 5
const WRITE_CHUNK_SIZE = INPUT_SIZE / NUM_WRITER_GO_ROUTINES
const READ_CHUNK_SIZE = INPUT_SIZE / NUM_READER_GO_ROUTINES
const PCHUNK_SIZE_READ = (INPUT_SIZE - PWRITE_INPUT_SIZE) / NUM_READER_GO_ROUTINES
const PCHUNK_SIZE_WRITE = PWRITE_INPUT_SIZE / NUM_WRITER_GO_ROUTINES


type KeyVal struct {
	Key   []byte
	Value []byte
}


func GenerateRandomBytes(length int) ([]byte, error) {
	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil { return nil, err }

	for i := 0; i < length; i++ {
		randomBytes[i] = 'a' + (randomBytes[i] % 26)
	}

	return randomBytes, nil
}

func TwoRandomDistinctValues(min, max int) (int, int, error) {
	if min >= max { return 0, 0, errors.New("min cannot be greater than max") }

	first := mrand.Intn(max - min) + min
	var second int
	for {
		second = mrand.Intn(max - min) + min
		if second != first { break }
	}

	return first, second, nil
}

func IsSorted(s []*mari.KeyValuePair) bool {
	for i := 1; i < len(s); i++ {
		if bytes.Compare(s[i - 1].Key, s[i].Key) > 0 { return false }
	}

	return true
}
