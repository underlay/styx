package styx

import (
	"fmt"

	"github.com/dgraph-io/badger"
)

// PrefetchSize of 128 as recommended in https://github.com/dgraph-io/badger
const PrefetchSize = 128

// Naive nested-loop join
func join(pivot []byte, iterator []byte, size int, joins [][]byte, txn *badger.Txn) ([]byte, []byte, [][]byte, error) {
	fmt.Println("joining!")
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

	fmt.Println("seeking to", string(pivot))
	if iterator == nil {
		fmt.Println("no existing iterator, so we seek directly to pivot")
		outer.Seek(pivot)
	} else {
		fmt.Println("there is an existing iterator, so we compute that shit", iterator)
		seek := append(pivot, tab)
		seek = append(pivot, iterator...)
		outer.Seek(seek)
		outer.Next()
	}

	fmt.Println("starting the outer loop iterator")

	for ; outer.ValidForPrefix(pivot); outer.Next() {
		fmt.Println("getting an outer item")
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
		fmt.Println("total joins:", len(joins))
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
		fmt.Println("checking for conflict")
		if !conflict {
			iterator := outerItem.Key()[len(pivot)+1:]
			fmt.Println("returning from join", string(outerValue))
			sources[0] = outerValue[0:outerStart]
			return outerValue[outerStart+1:], iterator, sources, nil
		}
	}
	fmt.Println("returning with nilllll")
	return nil, nil, nil, nil
}
