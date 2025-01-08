pub enum ComputationInput {
    Text(String),
    Binary(Vec<u8>),
}

impl ComputationInput {
    pub fn as_string(self) -> String {
        match self {
            ComputationInput::Text(text) => text,
            ComputationInput::Binary(binary) => String::from_utf8(binary).unwrap(),
        }
    }

    pub fn as_binary(self) -> Vec<u8> {
        match self {
            ComputationInput::Text(_) => vec![],
            ComputationInput::Binary(binary) => binary,
        }
    }
}
