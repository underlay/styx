package styx

import (
	rdf "github.com/underlay/go-rdfjs"
)

type list struct {
	dictionary Dictionary
	idlist     interface {
		Close()
		Next() (id ID, valid bool)
	}
}

func (l *list) Close() { l.idlist.Close() }

func (l *list) Next() (node rdf.Term) {
	id, valid := l.idlist.Next()
	if valid {
		node, _ = l.dictionary.GetTerm(id, rdf.Default)
	}
	return
}

// List lists the datasets in the database
func (s *Store) List(node rdf.Term) interface {
	Close()
	Next() rdf.Term
} {

	dictionary := s.Config.Dictionary.Open(false)
	id, _ := dictionary.GetID(node, rdf.Default)

	l := s.Config.QuadStore.List(id)
	return &list{dictionary, l}
}
