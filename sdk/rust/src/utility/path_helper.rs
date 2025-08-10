use std::env;

pub fn stream_socket_path(compute_engine_id: &str, stream_type: String) -> String {
    let base_path: String = sockets_base_path();

    format!(
        "{}/{}_stream_{}.sock",
        base_path, stream_type, compute_engine_id
    )
}

pub fn control_socket_path() -> String {
    let base_path: String = sockets_base_path();

    format!("{}/compute_engine_control_channel.sock", base_path)
}

fn sockets_base_path() -> String {
    let key = "FLEZHA_SOCKET_FILES_PATH";
    match env::var(key) {
        Ok(val) => val.trim_end_matches('/').to_string(),
        Err(_) => "app_data/sockets".to_string(),
    }
}
