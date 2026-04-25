use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "../../protobufs/compute_engine_commands.proto";
    let proto_file = fs::canonicalize(proto_file)?;
    let proto_file_str = proto_file.to_string_lossy().to_string();
    let proto_dir = proto_file.parent().unwrap();
    let proto_dir_str = proto_dir.to_string_lossy().to_string();

    let mut config = prost_build::Config::new();
    config.out_dir("src/generated");
    config.compile_protos(&[proto_file_str.as_str()], &[proto_dir_str.as_str()])?;

    println!("Generated protobuf files in src/generated/");
    Ok(())
}
