#!/bin/bash

# Script to regenerate protobuf files for the Flezha Rust SDK
# This script needs to be run when the protobuf files change
# Users of the library don't need to run this - the generated files are committed

set -e

echo "Regenerating protobuf files..."

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc is not installed or not in PATH"
    echo "Please install protoc and make sure it's in your PATH"
    echo "On Ubuntu/Debian: sudo apt install protobuf-compiler"
    echo "On macOS: brew install protobuf"
    exit 1
fi

# Check if we have the proto file
PROTO_FILE="../../protobufs/executor_commands.proto"
if [ ! -f "$PROTO_FILE" ]; then
    echo "Error: Proto file not found: $PROTO_FILE"
    exit 1
fi

# Create generated directory if it doesn't exist
mkdir -p src/generated

# Temporary build script to generate the files
cat > temp_build.rs << 'EOF'
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "../../protobufs/executor_commands.proto";
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
EOF

# Temporarily add prost-build to Cargo.toml
cp Cargo.toml Cargo.toml.backup
cat >> Cargo.toml << 'EOF'

[build-dependencies]
prost-build = "0.13.5"
EOF

# Set the temporary build script
mv build.rs build.rs.backup
mv temp_build.rs build.rs

# Build to generate the files
cargo build

# Restore original files
mv build.rs.backup build.rs
mv Cargo.toml.backup Cargo.toml

echo "Successfully regenerated protobuf files!"
echo "The generated files are in src/generated/"
echo "Make sure to commit these files to your repository."
