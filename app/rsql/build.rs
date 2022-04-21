extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .file("app/rsql/test.capnp")
        .run()
        .unwrap();
}
