package styx

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"regexp"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
)

var patternInteger = regexp.MustCompile("^[\\-+]?[0-9]+$")
var patternDouble = regexp.MustCompile("^(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee](\\+|-)?[0-9]+)?$")

var patternLiteral = regexp.MustCompile("^\"([^\"\\\\]*(?:\\\\.[^\"\\\\]*)*)\"")
var patternNext = regexp.MustCompile("\t|\n|$")

func readValue(val []byte) (Value, int) {
	ti := patternNext.FindIndex(val)
	li := patternLiteral.FindIndex(val)
	if li != nil {
		r := ti[0] - li[1]
		l := &literal{v: unescape(string(val[1 : li[1]-1]))}
		if r == 0 {
		} else if val[li[1]] == ':' && r > 1 {
			// Datatype
			l.d = iri(val[li[1]+1 : ti[0]])
		} else if val[li[1]] == '@' {
			// Language
			l.l = string(val[li[1]+1 : ti[0]])
			l.d = RDFLangString
		} else {
			return nil, -1
		}
		return l, ti[1]
	}

	v := val[:ti[0]]
	t := bytes.IndexByte(v, '#')
	if t == -1 {
		return iri(v), ti[1]
	}
	return &blank{origin: iri(val[:t]), id: string(val[t+1 : ti[0]])}, ti[1]
}

const maxValue uint64 = 16777215 // ////

func fromUint64(id uint64) iri {
	i := uint64(id)
	var res []byte
	if i <= maxValue {
		res = make([]byte, 4)
		binary.BigEndian.PutUint32(res, uint32(id))
		base64.StdEncoding.Encode(res, res[1:])
	} else {
		res = make([]byte, 8)
		binary.BigEndian.PutUint64(res, uint64(id))
		base64.StdEncoding.Encode(res, res[2:])
	}
	l := len(res)
	if res[l-2] == '=' {
		return iri(res[:l-2])
	} else if res[l-1] == '=' {
		return iri(res[:l-1])
	} else {
		return iri(res)
	}
}

func escape(str string) string {
	str = strings.Replace(str, "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "\n", "\\n", -1)
	str = strings.Replace(str, "\r", "\\r", -1)
	str = strings.Replace(str, "\t", "\\t", -1)
	return str
}

func unescape(str string) string {
	str = strings.Replace(str, "\\\\", "\\", -1)
	str = strings.Replace(str, "\\\"", "\"", -1)
	str = strings.Replace(str, "\\n", "\n", -1)
	str = strings.Replace(str, "\\r", "\r", -1)
	str = strings.Replace(str, "\\t", "\t", -1)
	return str
}

// assembleKey concatenates the passed slices
func assembleKey(prefix byte, tail bool, terms ...Term) []byte {
	l := 0
	for _, term := range terms {
		l += 1 + len(term)
	}
	if tail {
		l++
	}
	key := make([]byte, l)
	key[0] = prefix
	i := 1
	for _, term := range terms {
		copy(key[i:i+len(term)], term)
		i += len(term)
		if i < l {
			key[i] = '\t'
			i++
		}
	}
	return key
}

// setSafe writes the entry and returns a new transaction if the old one was full.
func setSafe(key, val []byte, txn *badger.Txn, db *badger.DB) (*badger.Txn, error) {
	e := badger.NewEntry(key, val).WithMeta(key[0])
	err := txn.SetEntry(e)
	if err == badger.ErrTxnTooBig {
		err = txn.Commit()
		if err != nil {
			return nil, err
		}
		txn = db.NewTransaction(true)
		err = txn.SetEntry(e)
	}
	return txn, err
}

// deleteSafe deletes the entry and returns a new transaction if the old one was full.
func deleteSafe(key []byte, txn *badger.Txn, db *badger.DB) (*badger.Txn, error) {
	err := txn.Delete(key)
	if err == badger.ErrTxnTooBig {
		err = txn.Commit()
		if err != nil {
			return nil, err
		}
		txn = db.NewTransaction(true)
		err = txn.Delete(key)
	}
	return txn, err
}

// matrix is a type for 3x3 permutators
type matrix [3][3]uint8

// permute permutes the given ids by the specified permutation
func (m matrix) permute(permutation Permutation, ids [3]Term) (Term, Term, Term) {
	row := m[permutation]
	return ids[row[0]], ids[row[1]], ids[row[2]]
}

// permuteNode permutes the given ids by the specified permutation
func (m matrix) permuteNode(permutation Permutation, nodes [3]term) (term, term, term) {
	row := m[permutation]
	return nodes[row[0]], nodes[row[1]], nodes[row[2]]
}

// major indexes the major permutations
var major = matrix{
	[3]uint8{0, 1, 2},
	[3]uint8{1, 2, 0},
	[3]uint8{2, 0, 1},
}

// minor indexes the minor permutations
var minor = matrix{
	[3]uint8{0, 2, 1},
	[3]uint8{1, 0, 2},
	[3]uint8{2, 1, 0},
}
