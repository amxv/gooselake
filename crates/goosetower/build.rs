use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../../proto");
    let proto_files = [
        proto_root.join("goosetower/v1/common.proto"),
        proto_root.join("goosetower/v1/view.proto"),
        proto_root.join("goosetower/v1/commands.proto"),
        proto_root.join("goosetower/v1/realtime.proto"),
    ];

    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let mut config = prost_build::Config::new();
    config.bytes([".goosetower.v1.Snapshot.body", ".goosetower.v1.Patch.body"]);
    config.compile_protos(&proto_files, &[proto_root])?;

    Ok(())
}
