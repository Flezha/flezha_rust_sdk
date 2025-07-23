use crate::node::OperatorNode;
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, CommandResponse};
use crate::service::payloader;
use prost::Message;
use std::sync::{Arc, Mutex};

mod migration_deserialization;
mod migration_serialization;
mod operator_assign;
mod operator_disconnect;
mod pipeline_start;
mod pipeline_state;
mod pipeline_stop;
mod streams_connect;

pub trait CommandAction {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        command_struct: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>);
}

pub enum CommandActionType {
    Regular(Box<dyn CommandAction>),
    OperatorDisconnect(Box<dyn CommandAction>),
}

pub fn get_handler(_payload_id: &String, message: &String) -> (CommandActionType, String) {
    let wrapper_command =
        CommandRequest::decode(message.as_bytes()).expect("Failed to decode message");

    let reference_id = wrapper_command.reference_id;

    let command_handler = match wrapper_command.command.unwrap() {
        Command::AssignOperatorId(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::ConnectSocket(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::AddComputation(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::RemComputation(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::MigrationSerialization(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::MigrationDeserialization(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        }
        Command::ComputationState(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::OperatorDisconnect(proto_ch) => {
            CommandActionType::OperatorDisconnect(Box::new(proto_ch))
        }
    };

    (command_handler, reference_id)
}

pub fn handle_action(
    operator: &Arc<Mutex<OperatorNode>>,
    handler: Box<dyn CommandAction>,
    payload_id: &String,
    message: &String,
    reference_id: String,
) -> Vec<u8> {
    let mut errors: Vec<String> = vec![];
    let mut status = String::from("success");

    let (_payload_id, raw_data) = handler.handle(operator, message, &mut errors);

    if errors.len() > 0 {
        status = "error".to_string();
    }

    let command_response: CommandResponse = CommandResponse {
        reference_id,
        status,
        errors,
        raw_data,
    };

    let command_response_bytes = command_response.encode_to_vec();

    // TODO: hardcoded virtual channel.
    payloader::build_from_binary(&payload_id, 1, &command_response_bytes)
}
