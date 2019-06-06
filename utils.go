package main

import (
	"fmt"
	"log"

	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	"./types"
)

func permuteMajor(permutation uint8, s, p, o []byte) ([]byte, []byte, []byte) {
	if permutation == 0 {
		return s, p, o
	} else if permutation == 1 {
		return p, o, s
	} else if permutation == 2 {
		return o, s, p
	}
	log.Fatalln("Invalid major permutation")
	return nil, nil, nil
}

func permuteMinor(permutation uint8, s, p, o []byte) ([]byte, []byte, []byte) {
	if permutation == 0 {
		return s, o, p
	} else if permutation == 1 {
		return p, s, o
	} else if permutation == 2 {
		return o, p, s
	}
	log.Fatalln("Invalid minor permutation")
	return nil, nil, nil
}

func nodeToValue(node ld.Node, origin *cid.Cid) *types.Value {
	if iri, isIri := node.(*ld.IRI); isIri {
		return &types.Value{Node: &types.Value_Iri{Iri: iri.Value}}
	} else if literal, isLiteral := node.(*ld.Literal); isLiteral {
		return &types.Value{
			Node: &types.Value_Literal{
				Literal: &types.Literal{
					Value:    literal.Value,
					Language: literal.Language,
					Datatype: literal.Datatype,
				},
			},
		}
	} else if blank, isBlank := node.(*ld.BlankNode); isBlank {
		return &types.Value{
			Node: &types.Value_Blank{
				Blank: &types.Blank{
					Cid: origin.Bytes(),
					Id:  blank.Attribute,
				},
			},
		}
	}
	return nil
}

func valueToNode(value *types.Value) (ld.Node, error) {
	if blank, isBlank := value.Node.(*types.Value_Blank); isBlank {
		c, err := cid.Cast(blank.Blank.Cid)
		if err != nil {
			return nil, err
		}
		iri := fmt.Sprintf("ul:/ipfs/%s#%s", c.String(), blank.Blank.Id)
		return &ld.IRI{Value: iri}, nil
	} else if iri, isIri := value.Node.(*types.Value_Iri); isIri {
		return &ld.IRI{Value: iri.Iri}, nil
	} else if literal, isLiteral := value.Node.(*types.Value_Literal); isLiteral {
		return &ld.Literal{
			Value:    literal.Literal.Value,
			Datatype: literal.Literal.Datatype,
			Language: literal.Literal.Language,
		}, nil
	}
	return nil, nil
}
