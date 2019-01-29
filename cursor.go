package main

import (
	fmt "fmt"

	"github.com/dgraph-io/badger"
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
	// return fmt.Sprintf("%s/%d: %s | %d", cursor.ID, cursor.Index, string(cursor.Prefix), cursor.Count)
}

func (cursor *Cursor) Value() []byte {
	if cursor.Iterator.ValidForPrefix(cursor.Prefix) {
		item := cursor.Iterator.Item()
		key, index := item.Key(), int(item.UserMeta())
		return key[len(key)-index:]
	}
	return nil
}

func (cursor *Cursor) Next() []byte {
	cursor.Iterator.Next()
	return cursor.Value()
}

func (cursor *Cursor) Seek(value []byte) []byte {
	key := append(cursor.Prefix, value...)
	cursor.Iterator.Seek(key)
	return cursor.Value()
}

type Seekable interface {
	getCursor(i int) *Cursor
	Len() int
}

// Seek to the next intersect value
func Seek(s Seekable, value []byte) []byte {
	var count int
	l := s.Len()
	fmt.Println("seekable length", l)
	for i := 0; count < l; i = (i + 1) % l {
		cursor := s.getCursor(i)
		fmt.Println("got cursor", cursor, string(cursor.Prefix), string(value))
		next := cursor.Seek(value)
		fmt.Println("got cursor next", string(next))
		if next == nil {
			return nil
		} else if string(next) == string(value) {
			count++
		} else {
			count = 1
			value = next
		}
	}
	return value
}

// Next value (could be improved to not double-check cursor[0])
func Next(s Seekable) []byte {
	cursor := s.getCursor(0)
	cursor.Iterator.Next()
	item := cursor.Iterator.Item()
	key, index := item.Key(), int(item.UserMeta())
	next := key[len(key)-index:]
	return Seek(s, next)
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
func (cs CursorSet) Less(a, b int) bool      { return cs[a].Count < cs[b].Count }
func (cs CursorSet) getCursor(i int) *Cursor { return cs[i] }
