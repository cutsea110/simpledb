use capnp::capability::Promise;
use std::error::Error;

use crate::test_capnp::ping;

struct PingImpl;

impl ping::Server for PingImpl {
    fn ping(
        &mut self,
        _params: ping::PingParams,
        mut _results: ping::PingResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub async fn main() -> Result<(), Box<dyn Error>> {
    panic!("TODO")
}
