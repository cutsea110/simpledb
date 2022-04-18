# SimpleDB

This is a project to develop SimpleDB in Rust.

text: [Database Design and Implementation: Second Edition](https://www.amazon.co.jp/gp/product/3030338355/)

## Status

Done to implement all of book contents, except for exercises and chap 11 remote server.

## Build

``` shell
cargo build
```

## Run

How to connect and run sql for a database named dbname on embedded version is like below.

``` shell
cargo run -- -d <dbname>
```
