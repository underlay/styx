rapper -i rdfxml -o nquads -q $1 > $1.nt
./canonize $1.nt > data/$1.c.nt
rm $1 $1.nt
