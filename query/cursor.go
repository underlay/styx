package query

import (
	"bytes"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"

	types "github.com/underlay/styx/types"
)

// A Cursor is an Iterator and a Prefix
type Cursor struct {
	ID       string
	Index    int
	Iterator *badger.Iterator
	Prefix   []byte
	Count    uint64
}

func (cursor *Cursor) String() string {
	return fmt.Sprintf("%s/%d", cursor.ID, cursor.Index)
}

func (cursor *Cursor) value() []byte {
	if cursor.Iterator.ValidForPrefix(cursor.Prefix) {
		item := cursor.Iterator.Item()
		key := item.Key()
		prefix := key[0]
		if _, has := types.TriplePrefixMap[prefix]; has {
			return key[17:25]
		} else if _, has := types.MajorPrefixMap[prefix]; has {
			return key[9:17]
		} else if _, has := types.MinorPrefixMap[prefix]; has {
			return key[9:17]
		} else {
			return key[1:9] // Should never happen?
		}
	}
	return nil
}

// Next advances the cursor and returns the next value
func (cursor *Cursor) Next() []byte {
	cursor.Iterator.Next()
	return cursor.value()
}

// Seek advances the cursor to the first value equal to
// or greater than given byte slice. Both full values
// (for dyanmic and static present cursors) and partial
// prefixes (for static future cursors) can be given
func (cursor *Cursor) Seek(value []byte) []byte {
	key := append(cursor.Prefix, value...)
	cursor.Iterator.Seek(key)
	return cursor.value()
}

// A CursorSet is just a slice of Cursors
type CursorSet []*Cursor

func (cs CursorSet) String() string {
	val := "[ "
	for i, cursor := range cs {
		if i > 0 {
			val += ", "
		}
		val += cursor.String()
	}
	return val + " ]"
}

// Sort interface for CursorSet
func (cs CursorSet) Len() int { return len(cs) }
func (cs CursorSet) Swap(a, b int) {
	cs[a], cs[b] = cs[b], cs[a]
}

func (cs CursorSet) Less(a, b int) bool { return cs[a].Count < cs[b].Count }

// Seek to the next intersect value
func (cs CursorSet) Seek(value []byte) []byte {
	var count int
	l := cs.Len()
	for i := 0; count < l; i = (i + 1) % l {
		cursor := cs[i]
		next := cursor.Seek(value)
		if next == nil {
			return nil
		} else if bytes.Equal(next, value) {
			count++
		} else {
			count = 1
			value = next
		}
	}
	return value
}

// Next value (could be improved to not double-check cursor[0])
func (cs CursorSet) Next() []byte {
	cursor := cs[0]
	cursor.Iterator.Next()
	next := cursor.value()
	return cs.Seek(next)
}
