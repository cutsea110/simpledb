#[macro_use]
extern crate capnp_rpc;
pub mod remote_capnp {
    include!(concat!(
        env!("OUT_DIR"),
        "/src/rdbc/network/remote_capnp.rs"
    ));
}

pub mod buffer;
pub mod file;
pub mod index;
pub mod log;
pub mod materialize;
pub mod metadata;
pub mod multibuffer;
pub mod opt;
pub mod parser;
pub mod plan;
pub mod query;
pub mod rdbc;
pub mod record;
pub mod server;
pub mod tx;

// my own extends
pub mod repr;
