# SimpleDB

[![Rust](https://github.com/cutsea110/simpledb/actions/workflows/rust.yml/badge.svg)](https://github.com/cutsea110/simpledb/actions/workflows/rust.yml)

This is a project to develop SimpleDB in Rust.

text: [Database Design and Implementation: Second Edition](https://www.amazon.co.jp/gp/product/3030338355/)

## Demo movie

<img src="./movie/demo-x2-resized.gif" width="800" alt="Demo movie">
[youtube]: https://www.youtube.com/watch?v=vr0wQq7cvHQ

## Status

Done to implement all of book contents, but any exercise.

## Build

You need to install capnproto for building this project.

``` shell
sudo apt install capnproto
```

``` shell
cargo build
```

## How to run on embedded version

How to connect and run sql for a database named dbname on embedded version is like below.

``` shell
cargo run --bin esql -- -d <dbname>
```

## How to run on server/client version

How to run simpledb-server.

``` shell
cargo run --bin simpledb server program.
```

How to run sql as simpledb client program.

``` shell
cargo run --bin sql -- -d <dbname>
```

## Benchmarking & Visualize

take benchmarking data.

``` shell
./benchmarks.sh
```

and then run http-server.
You must install http-server on npm, if you view the results on your local.

``` shell
cd benchmarks
http-server -p 3000
```

and then open browser http://localhost:3000?scale=tiny.
At this url, query parameter scale can has tiny/small/medium/large.

## Query tips

use rlwrap in order to edit query prettier.

```bash
$ rlwrap cargo run --bin esql -- -d <dbname>
```

check table catalogs.

```sql
SQL> :t tblcat
* table: tblcat has 2 fields.

#   name             type
--------------------------------------
   1 tblname          varchar(16)
   2 slotsize         integer

SQL> SELECT tblname FROM tblcat;
tblname
------------------
tblcat
fldcat
viewcat
idxcat
student
dept
course
section
enroll
sex
transaction 6 committed
Rows 10 (0.000s)

SQL>
```

check field catalogs.

```sql
SQL> :t fldcat
* table: fldcat has 5 fields.

#   name             type
--------------------------------------
   1 tblname          varchar(16)
   2 fldname          varchar(16)
   3 type             integer
   4 length           integer
   5 offset           integer

SQL> SELECT tblname, fldname, type, length FROM fldcat;
tblname           fldname           type    length
----------------------------------------------------
...
student           sid                     2       0
student           sname                   3      10
student           grad_year               1       0
student           major_id                2       0
student           birth                   5       0
student           sex                     4       0
transaction 9 committed
Rows 33 (0.001s)
```

check view catalog

```sql
SQL> SELECT viewname, viewdef FROM viewcat;
viewname          viewdef
------------------------------------------------------------------------------------------------------------------------
einstein          select sect_id from section where prof='einstein'
transaction 3 committed
Rows 1 (0.000s)

SQL>
```

### Benchmark results

- [tiny](https://cutsea110.github.io/simpledb/?scale=tiny)
- [small](https://cutsea110.github.io/simpledb/?scale=small)
- [medium](https://cutsea110.github.io/simpledb/?scale=medium)
