# styx

Home-grown quadstore inspired by [Hexastore](https://dl.acm.org/citation.cfm?id=1453965), tailored for use in the Underlay. 

```go
store := OpenStore(path)

triple := Triple{"alice", "likes", "pizza"}
cid := "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM"
quad := Quad{Triple: triple, Cid: cid}

// insert!
Insert(quad, store)

// yay! now query.
fmt.Println(IndexTriple(Triple{"alice", "likes", ""}, store))
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
fmt.Println(IndexTriple(Triple{"alice", "", "pizza"}, store))
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
fmt.Println(IndexTriple(Triple{"", "likes", "pizza"}, store))
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
```
