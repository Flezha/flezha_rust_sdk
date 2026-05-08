# Flezha Rust SDK - Protobuf Generation

This document explains how protobuf files are handled in the Flezha Rust SDK.

## For Users (Library Consumers)

**Good news!** You don't need to install `protoc` (Protocol Buffer compiler) to use this library. The generated Rust code from the protobuf files is already included in the repository at `src/generated/`.

The library will compile and work out of the box without any additional dependencies.

## For Developers (Library Maintainers)

### Pre-generated Files

The protobuf files are pre-generated and stored in `src/generated/`. This approach has several benefits:

1. **No build-time dependencies**: Users don't need `protoc` installed
2. **Faster builds**: No need to compile protobuf files during build
3. **Consistent builds**: Same generated code across all environments
4. **Easier CI/CD**: No need to install protobuf compiler in CI environments

### When to Regenerate

You only need to regenerate the protobuf files when:
- The `../../protobufs/executor_commands.proto` file changes
- You want to update to a newer version of `prost`

### How to Regenerate

#### Method 1: Using the Script (Recommended)

```bash
./regen_proto.sh
```

This script will:
1. Check if `protoc` is installed
2. Temporarily modify the build configuration
3. Generate the new protobuf files
4. Restore the original configuration
5. Place the generated files in `src/generated/`

#### Method 2: Manual Process

1. Install `protoc` if not already installed:
   ```bash
   # Ubuntu/Debian
   sudo apt install protobuf-compiler
   
   # macOS
   brew install protobuf
   ```

2. Temporarily add `prost-build` to your `Cargo.toml`:
   ```toml
   [build-dependencies]
   prost-build = "0.13.5"
   ```

3. Modify `build.rs` to generate files in `src/generated/` (uncomment the commented code)

4. Run `cargo build`

5. Restore the original `Cargo.toml` and `build.rs`

### Important Notes

- **Always commit the generated files** to the repository
- The generated files are in `src/generated/executor_commands.rs`
- The library includes these files using `include!("generated/executor_commands.rs")`
- The current `build.rs` is essentially a no-op for normal builds

### Project Structure

```
sdk/rust/
├── src/
│   ├── generated/
│   │   └── executor_commands.rs  # Generated protobuf code (committed)
│   ├── lib.rs                    # Includes the generated code
│   └── ...
├── build.rs                      # No-op build script
├── regen_proto.sh               # Script to regenerate protobuf files
└── Cargo.toml                   # No build dependencies
```

### Troubleshooting

If you encounter issues:

1. **Build fails with protobuf errors**: Make sure the generated files are present in `src/generated/`
2. **Generated files are outdated**: Run `./regen_proto.sh` to regenerate
3. **Missing protoc**: Only needed for regeneration, not for normal builds

### Alternative Approaches Considered

1. **Build-time generation**: Requires users to install `protoc`
2. **Optional feature**: Complex and confusing for users
3. **CI-generated files**: Chosen approach - simple and user-friendly
