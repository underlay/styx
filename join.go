package styx

import (
	"github.com/dgraph-io/badger"
)

// PrefetchSize of 128 as recommended in https://github.com/dgraph-io/badger
const PrefetchSize = 128

// Naive nested-loop join
func join(pivot []byte, iterator []byte, size int, joins [][]byte, txn *badger.Txn) ([]byte, []byte, [][]byte, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.PrefetchSize = PrefetchSize
	inner := txn.NewIterator(opts)
	defer inner.Close()

	if size < PrefetchSize {
		opts.PrefetchSize = size
	}

	outer := txn.NewIterator(opts)
	defer outer.Close()

	var err error
	var outerValue, innerValue []byte
	var outerStart, innerStart int
	sources := make([][]byte, len(joins)+1)

	if iterator == nil {
		outer.Seek(pivot)
	} else {
		seek := append(pivot, tab)
		seek = append(pivot, iterator...)
		outer.Seek(seek)
		outer.Next()
	}

	for ; outer.ValidForPrefix(pivot); outer.Next() {
		outerItem := outer.Item()
		outerValue, err = outerItem.ValueCopy(outerValue)
		if err != nil {
			return nil, nil, nil, err
		}

		// This is why we used \n for the delimiter before the actual value,
		// since \t is used a variable number of times before it.
		for i, char := range outerValue {
			if char == '\n' {
				outerStart = i
				break
			}
		}

		var conflict bool

		// This is the loops of joins for the inner loop.
		// We need every join to have *a* value that matches.
		for i, join := range joins {
			// This is inner loop for one specific join.
			// We just need one of the values to match.
			// Python's for...else syntax would be so nice here :-/
			passed := false
			for inner.Seek(join); inner.ValidForPrefix(join); inner.Next() {
				innerItem := inner.Item()
				innerValue, err = innerItem.ValueCopy(innerValue)
				if err != nil {
					return nil, nil, nil, err
				}

				innerStart = 0
				var failed bool
				for i, char := range innerValue {
					if innerStart > 0 {
						if char != outerValue[i-innerStart+outerStart] {
							failed = true
							break
						}
					} else if char == '\n' {
						innerStart = i
					}
				}

				if !failed {
					passed = true
					sources[i+1] = innerValue[0:innerStart]
					break
				}
			}
			if !passed {
				conflict = true
				break
			}
		}
		if !conflict {
			iterator := outerItem.Key()[len(pivot)+1:]
			sources[0] = outerValue[0:outerStart]
			return outerValue[outerStart+1:], iterator, sources, nil
		}
	}
	return nil, nil, nil, nil
}
