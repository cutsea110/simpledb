extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .file("capnp/remote.capnp")
        .run()
        .unwrap();
}
