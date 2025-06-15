use std::path::Path;
use std::process::Command;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: this needs to be dynamic from env variables or live with the project.
    let proto_file = "../../protobufs/operator_commands.proto";
    let proto_path = Path::new(proto_file);

    if !proto_path.exists() {
        println!("Error: Proto file not found: {}", proto_file);
    }

    let protoc_check = Command::new("protoc").arg("--version").output();
    match protoc_check {
        Ok(output) => {
            if output.status.success() {} 
            else {
                println!("protoc is in PATH but returned an error: {}", String::from_utf8_lossy(&output.stderr));
                println!("Make sure protoc version is >= 3");
            }
        }
        Err(e) => {
            println!("protoc is not in PATH or not executable: {}", e);
            println!("Install protoc and make sure it's in your PATH.");
            return Err(e.into());
        }
    }
    let proto_file = fs::canonicalize(proto_file)?;
    let proto_file_str = proto_file.to_string_lossy().to_string();
    let proto_dir = proto_file.parent().unwrap();
    let proto_dir_str = proto_dir.to_string_lossy().to_string();

    prost_build::compile_protos(&[proto_file_str.as_str()], &[proto_dir_str.as_str()])?;

    Ok(())
}
