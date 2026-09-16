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
PROTO_FILE="../../protobufs/runner_commands.proto"
if [ ! -f "$PROTO_FILE" ]; then
    echo "Error: Proto file not found: $PROTO_FILE"
    exit 1
fi

# The generated module is compiled into the shared kernel (`flz_core/src/lib.rs`
# `include!`s it), not into this crate — this script only owns the toolchain.
OUT_DIR="../../flz_core/src/generated"
mkdir -p "$OUT_DIR"

# Temporary build script to generate the files
cat > temp_build.rs << 'EOF'
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "../../protobufs/runner_commands.proto";
    let proto_file = fs::canonicalize(proto_file)?;
    let proto_file_str = proto_file.to_string_lossy().to_string();
    let proto_dir = proto_file.parent().unwrap();
    let proto_dir_str = proto_dir.to_string_lossy().to_string();

    let mut config = prost_build::Config::new();
    config.out_dir("../../flz_core/src/generated");
    config.compile_protos(&[proto_file_str.as_str()], &[proto_dir_str.as_str()])?;

    println!("Generated protobuf files in flz_core/src/generated/");
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
HAD_BUILD_RS=0
if [ -f build.rs ]; then
    HAD_BUILD_RS=1
    mv build.rs build.rs.backup
fi
mv temp_build.rs build.rs

# Build to generate the files
cargo build

# Restore original files
if [ "$HAD_BUILD_RS" -eq 1 ]; then
    mv build.rs.backup build.rs
else
    rm build.rs
fi
mv Cargo.toml.backup Cargo.toml

echo "Successfully regenerated protobuf files!"
echo "The generated files are in flz_core/src/generated/"
echo "Make sure to commit these files to your repository."
