use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=schema/ferrosa/internode/envelope.capnp");
    println!("cargo:rerun-if-env-changed=CAPNP");

    let mut compiler = capnpc::CompilerCommand::new();
    compiler
        .src_prefix("schema/ferrosa/internode")
        .default_parent_module(vec!["protocol".to_owned()])
        .file("schema/ferrosa/internode/envelope.capnp");

    if let Ok(capnp) = env::var("CAPNP") {
        compiler.capnp_executable(PathBuf::from(capnp));
    }

    compiler
        .run()
        .expect("compile ferrosa internode Cap'n Proto schemas with capnp");
}
