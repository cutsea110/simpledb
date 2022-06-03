# SimpleDB

This is a project to develop SimpleDB in Rust.

text: [Database Design and Implementation: Second Edition](https://www.amazon.co.jp/gp/product/3030338355/)

## Demo

<img src="./movie/demo-x2-resized.gif" width="800">

## Status

Done to implement all of book contents, except for exercises and chap 11 remote server.

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

How to run server.

``` shell
cargo run --bin simpledb-server
```

How to run client.

``` shell
cargo run --bin sql -- -d <dbname>
```
