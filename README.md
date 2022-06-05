# SimpleDB

This is a project to develop SimpleDB in Rust.

text: [Database Design and Implementation: Second Edition](https://www.amazon.co.jp/gp/product/3030338355/)

## Demo movie

<img src="./movie/demo-x2-resized.gif" width="800" alt="Demo movie">

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
