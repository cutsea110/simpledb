use anyhow::Result;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use core::fmt;
use futures::AsyncReadExt;
use std::net::{SocketAddr, ToSocketAddrs};

use super::connection::NetworkConnection;
use crate::{rdbc::driveradapter::DriverAdapter, remote_capnp::remote_driver};

#[derive(Debug)]
pub enum NetworkDriverError {
    InvalidUrl,
}
impl std::error::Error for NetworkDriverError {}
impl fmt::Display for NetworkDriverError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NetworkDriverError::InvalidUrl => {
                write!(f, "invalid url")
            }
        }
    }
}

struct Config<'a> {
    addr: SocketAddr,
    dbname: &'a str,
}

impl<'a> Config<'a> {
    // url is like as "rdbc:simpledb://127.0.0.1:1099/sampledb".
    pub fn from_str(url: &'a str) -> Result<Self> {
        if let Some(trimed) = url.strip_prefix("rdbc:simpledb://") {
            let elems: Vec<&str> = trimed.split('/').collect();
            if elems.len() == 2 {
                let addr = elems[0].to_socket_addrs()?.next().expect("socket addr");
                let dbname = elems[1];

                return Ok(Self { addr, dbname });
            }
            return Err(From::from(NetworkDriverError::InvalidUrl));
        }
        Err(From::from(NetworkDriverError::InvalidUrl))
    }
}

pub struct NetworkDriver;

impl<'a> DriverAdapter<'a> for NetworkDriver {
    type Con = NetworkConnection;

    fn connect(&self, url: &str) -> Result<Self::Con> {
        let cfg = Config::from_str(url)?;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let con = rt.block_on(async {
            let stream = tokio::net::TcpStream::connect(&cfg.addr).await.unwrap();
            stream.set_nodelay(true).unwrap();
            let (reader, writer) =
                tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
            let rpc_network = Box::new(twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Client,
                Default::default(),
            ));
            let mut rpc_system = RpcSystem::new(rpc_network, None);
            let drvc: remote_driver::Client =
                rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

            let mut request = drvc.connect_request();
            request.get().set_dbname(cfg.dbname);
            request.send().pipeline.get_conn()
        });

        Ok(NetworkConnection::new(con))
    }
    fn get_major_version(&self) -> i32 {
        0
    }
    fn get_minor_version(&self) -> i32 {
        1
    }
}
