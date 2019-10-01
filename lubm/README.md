# LUBM Benchmarks

- [LUBM homepage](http://swat.cse.lehigh.edu/projects/lubm/)
- [Queries in SVG format](http://swat.cse.lehigh.edu/projects/lubm/lubm.svg)
- [AllegroGraph results (archive)](https://web.archive.org/web/20090208165243/http://agraph.franz.com/allegrograph/agraph_bench_lubm50.lhtml)
- [Stardog results](https://docs.google.com/spreadsheets/d/1oHSWX_0ChZ61ofipZ1CMsW7OhyujioR28AfHzU9d56k/pubhtml#)

Generate data

```
java edu.lehigh.swat.bench.uba.Generator -univ 1 -index 0 -seed 0 -onto http://swat.cse.lehigh.edu/onto/univ-bench.owl
```

`convert.sh`

```
rapper -i rdfxml -o nquads -q $1  > $1.nt
curl --data-binary @$1.nt -H 'Content-Type: application/n-quads' localhost:8086/
```

Ingest data

```
find *.owl -maxdepth 1 -type f -exec ./convert.sh {} \;
```

Count triples

```
find *.owl.nt | xargs wc -l
```

# LUBM1

103,104 total triples

|        | 1       | 2       | 3       | 4       | 5       | 6       | 7       | 8       | 9       | 10      | avg       |
| ------ | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | --------- |
| query1 | 362.919 | 288.016 | 540.329 | 415.967 | 392.153 | 340.841 | 333.268 | 558.645 | 306.82  | 404.513 | 394.347µs |
| query2 | 558.941 | 469.040 | 462.761 | 387.005 | 643.515 | 456.333 | 415.154 | 472.529 | 381.791 | 387.346 | 463.441µs |
| query3 | 214.564 | 166.385 | 110.453 | 141.024 | 126.236 | 209.382 | 177.337 | 135.050 | 132.056 | 174.203 | 158.669µs |

## LUBM50

6,890,640 total triples

|        | 1        | 2        | 3        | 4        | 5        | 6        | 7        | 8       | 9        | 10       | avg        |
| ------ | -------- | -------- | -------- | -------- | -------- | -------- | -------- | ------- | -------- | -------- | ---------- |
| query1 | 559.499  | 581.531  | 534.952  | 560.218  | 587.980  | 736.740  | 536.250  | 548.940 | 652.013  | 593.672  | 589.180µs  |
| query2 | 2.382153 | 3.141992 | 2.337339 | 2.658253 | 2.695001 | 2.360368 | 2.193803 | 2.70467 | 3.040395 | 2.975206 | 2.648918ms |
| query3 | 128.307  | 166.007  | 169.277  | 149.976  | 158.24   | 155.821  | 207.412  | 187.419 | 173.274  | 194.018  | 168.975µs  |

## LUBM100

13,879,970 total triples

|        | 1        | 2        | 3        | 4        | 5        | 6        | 7        | 8        | 9        | 10       | avg         |
| ------ | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | ----------- |
| query1 | 0.445630 | 0.576025 | 0.408285 | 0.621839 | 0.484363 | 0.570337 | 0.477194 | 0.592727 | 0.499838 | 0.533649 | 0.5209887ms |
| query2 | 1.645319 | 2.145748 | 2.054479 | 1.500583 | 1.436753 | 1.585419 | 1.886467 | 2.120749 | 2.381642 | 1.437658 | 1.8194817ms |
| query3 | 0.181701 | 0.187435 | 0.221821 | 0.159671 | 0.156800 | 0.169522 | 0.196278 | 0.179919 | 0.159784 | 0.152532 | 0.1765463ms |
