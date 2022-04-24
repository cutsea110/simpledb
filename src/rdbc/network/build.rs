extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .file("src/rdbc/network/remote.capnp")
        .run()
        .unwrap();
}
