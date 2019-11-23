rapper -i rdfxml -o nquads -q $1 > $1.nt
curl --data-binary @$1.nt -H 'Content-Type: application/n-quads' localhost:8086/
