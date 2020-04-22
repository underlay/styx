package styx

import (
	rdf "github.com/underlay/go-rdfjs"
)

// Get a dataset from the database
func (s *Store) Get(node rdf.Term) ([]*rdf.Quad, error) {
	dictionary := s.Config.Dictionary.Open(false)
	defer func() { dictionary.Commit() }()

	id, err := dictionary.GetID(node, rdf.Default)
	if err != nil {
		return nil, err
	}

	quads, err := s.Config.QuadStore.Get(id)
	if err != nil {
		return nil, err
	}

	dataset := make([]*rdf.Quad, len(quads))
	for i, quad := range quads {
		s, err := dictionary.GetTerm(quad[0], node)
		if err != nil {
			return nil, err
		}
		p, err := dictionary.GetTerm(quad[1], node)
		if err != nil {
			return nil, err
		}
		o, err := dictionary.GetTerm(quad[2], node)
		if err != nil {
			return nil, err
		}
		g, err := dictionary.GetTerm(quad[3], node)
		if err != nil {
			return nil, err
		}
		dataset[i] = rdf.NewQuad(s, p, o, g)
	}

	return dataset, nil
}
