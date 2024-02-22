pub use capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .file("capnp/remote.capnp")
        .run()
        .expect("compiling schema");
}
