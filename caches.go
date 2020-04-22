package styx

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
)

type unaryCache map[ID]*[6]uint32

// newUnaryCache creates a new IndexCache
func newUnaryCache() unaryCache {
	return unaryCache{}
}

// getUnaryIndex returns the 6-tuple of counts from an item
func getUnaryIndex(item *badger.Item) (*[6]uint32, error) {
	result := &[6]uint32{}
	return result, item.Value(func(val []byte) error {
		if len(val) != 24 {
			return fmt.Errorf("Unexpected index value: %v", val)
		}
		for i := 0; i < 6; i++ {
			result[i] = binary.BigEndian.Uint32(val[i*4 : (i+1)*4])
		}
		return nil
	})
}

func (uc unaryCache) getIndex(a ID, txn *badger.Txn) (*[6]uint32, error) {
	index, has := uc[a]
	if has {
		return index, nil
	}

	key := assembleKey(UnaryPrefix, false, a)
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	uc[a] = &[6]uint32{}
	err = item.Value(func(val []byte) error {
		if len(val) != 24 {
			return fmt.Errorf("Unexpected index value: %v", val)
		}
		for i := 0; i < 6; i++ {
			uc[a][i] = binary.BigEndian.Uint32(val[i*4 : (i+1)*4])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return uc[a], nil
}

func (uc unaryCache) Get(p Permutation, a ID, txn *badger.Txn) (uint32, error) {
	index, err := uc.getIndex(a, txn)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return index[p], nil
}

func (uc unaryCache) Increment(p Permutation, a ID, txn *badger.Txn) error {
	index, err := uc.getIndex(a, txn)
	if err == badger.ErrKeyNotFound {
		index = &[6]uint32{}
		uc[a] = index
	} else if err != nil {
		return err
	}
	uc[a][p]++
	return nil
}

func (uc unaryCache) Decrement(p Permutation, a ID, txn *badger.Txn) error {
	index, err := uc.getIndex(a, txn)
	if err == badger.ErrKeyNotFound {
		index = &[6]uint32{}
		uc[a] = index
	} else if err != nil {
		return err
	}
	if uc[a][p] > 0 {
		uc[a][p]--
	}
	return nil
}

// Commit writes the contents of the index map to badger
func (uc unaryCache) Commit(db *badger.DB, t *badger.Txn) (txn *badger.Txn, err error) {
	txn = t
	for term, index := range uc {
		key := assembleKey(UnaryPrefix, false, term)
		zero := true
		for _, c := range index {
			if c > 0 {
				zero = false
				break
			}
		}
		if zero {
			txn, err = deleteSafe(key, txn, db)
			if err == badger.ErrKeyNotFound {
				return txn, nil
			}
		} else {
			val := make([]byte, 24)
			for i, c := range index {
				binary.BigEndian.PutUint32(val[i*4:(i+1)*4], c)
			}
			txn, err = setSafe(key, val, txn, db)
			if err != nil {
				return
			}
		}
	}
	return
}

type binaryCache map[string]uint32

// newBinaryCache returns a new binary cache
func newBinaryCache() binaryCache {
	return binaryCache{}
}

func (bc binaryCache) Get(p Permutation, a, b ID, txn *badger.Txn) (uint32, error) {
	key := assembleKey(BinaryPrefixes[p], false, a, b)
	s := string(key)
	count, has := bc[s]
	if has {
		return count, nil
	}

	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	err = item.Value(func(val []byte) error {
		bc[s] = binary.BigEndian.Uint32(val)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return bc[s], nil
}

func (bc binaryCache) delta(p Permutation, a, b ID, increment bool, uc unaryCache, txn *badger.Txn) error {
	key := assembleKey(BinaryPrefixes[p], false, a, b)
	s := string(key)
	_, has := bc[s]
	if has {
		if increment {
			bc[s]++
			if bc[s] == 1 {
				return uc.Increment(p, a, txn)
			}
		} else if bc[s] > 0 {
			bc[s]--
			if bc[s] == 0 {
				return uc.Decrement(p, a, txn)
			}
		} else {
			// ??
		}
		return nil
	}

	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound && increment { // Hmm
		bc[s] = 1
		return uc.Increment(p, a, txn)
	} else if err != nil {
		return err
	}

	err = item.Value(func(val []byte) error {
		if len(val) != 4 {
			return fmt.Errorf("Unexpected binary value: %v", val)
		}
		bc[s] = binary.BigEndian.Uint32(val)
		return nil
	})
	if err != nil {
		return err
	}

	if increment {
		bc[s]++
	} else if bc[s] == 1 {
		bc[s] = 0
		return uc.Decrement(p, a, txn)
	} else if bc[s] > 0 {
		bc[s]--
	} else {
		// ???
	}
	return nil
}

func (bc binaryCache) Increment(p Permutation, a, b ID, uc unaryCache, txn *badger.Txn) error {
	return bc.delta(p, a, b, true, uc, txn)
}

func (bc binaryCache) Decrement(p Permutation, a, b ID, uc unaryCache, txn *badger.Txn) error {
	return bc.delta(p, a, b, false, uc, txn)
}

// Commit writes the contents of the index map to badger
func (bc binaryCache) Commit(db *badger.DB, t *badger.Txn) (txn *badger.Txn, err error) {
	txn = t
	for key, count := range bc {
		if count == 0 {
			txn, err = deleteSafe([]byte(key), txn, db)
			if err == badger.ErrKeyNotFound {
			} else if err != nil {
				return
			}
		} else {
			val := make([]byte, 4)
			binary.BigEndian.PutUint32(val, count)
			txn, err = setSafe([]byte(key), val, txn, db)
			if err != nil {
				return
			}
		}
	}
	return
}
