// This build.rs is now optional - the protobuf files are pre-generated
// and included in the src/generated directory.
//
// To regenerate the protobuf files when operator_commands.proto changes,
// uncomment the code below and run `cargo build` (requires protoc).
//
// Alternatively, use the script: cargo run --bin generate_proto

/*
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: this needs to be dynamic from env variables or live with the project.
    let proto_file = "../../protobufs/operator_commands.proto";
    let proto_path = Path::new(proto_file);

    if !proto_path.exists() {
        println!("Warning: Proto file not found: {}", proto_file);
        println!("Using pre-generated protobuf files from src/generated/");
        return Ok(());
    }

    let protoc_check = Command::new("protoc").arg("--version").output();
    match protoc_check {
        Ok(output) => {
            if !output.status.success() {
                println!(
                    "protoc is in PATH but returned an error: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                println!("Using pre-generated protobuf files from src/generated/");
                return Ok(());
            }
        }
        Err(_) => {
            println!("protoc is not in PATH - using pre-generated protobuf files from src/generated/");
            return Ok(());
        }
    }

    let proto_file = fs::canonicalize(proto_file)?;
    let proto_file_str = proto_file.to_string_lossy().to_string();
    let proto_dir = proto_file.parent().unwrap();
    let proto_dir_str = proto_dir.to_string_lossy().to_string();

    // Configure prost-build to output to src/generated
    let mut config = prost_build::Config::new();
    config.out_dir("src/generated");
    
    config.compile_protos(&[proto_file_str.as_str()], &[proto_dir_str.as_str()])?;

    println!("Updated protobuf files in src/generated/");

    Ok(())
}
*/

fn main() {
    // No-op build script - protobuf files are pre-generated
    println!("Using pre-generated protobuf files from src/generated/");
}
