pub enum WorkerInput {
    Text(String),
    Binary(Vec<u8>),
}

impl WorkerInput {
    pub fn as_string(self) -> String {
        match self {
            WorkerInput::Text(text) => text,
            WorkerInput::Binary(binary) => String::from_utf8(binary).unwrap(),
        }
    }

    pub fn as_binary(self) -> Vec<u8> {
        match self {
            WorkerInput::Text(_) => vec![],
            WorkerInput::Binary(binary) => binary,
        }
    }
}
