use crate::command;
use crate::operator::OperatorNode;
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, CommandResponse};
use crate::service::payloader;
use prost::Message;
use std::collections::HashMap;
use std::io::Write;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;

mod computation_start;
mod computation_state;
mod computation_stop;
mod migration_deserialization;
mod migration_serialization;
mod operator_assign;
mod operator_disconnect;
mod streams_connect;

pub trait CommandAction {
    fn handle(
        &self,
        operator: &mut OperatorNode,
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

pub fn handle_command(
    operator: &mut OperatorNode,
    stream: &UnixStream,
    payload_id: &String,
    message: &Vec<u8>,
) {
    let payload_message: String = String::from_utf8_lossy(&message).to_string();

    let (handler_type, reference_id) = command::get_handler(&payload_id, &payload_message);

    match handler_type {
        CommandActionType::Regular(handler) => {
            let reply = command::exec_control(
                operator,
                handler,
                &payload_id,
                &payload_message,
                reference_id,
            );
            (&*stream).write_all(&reply).unwrap();
        }

        CommandActionType::OperatorDisconnect(handler) => {
            let reply = command::exec_control(
                operator,
                handler,
                &payload_id,
                &payload_message,
                reference_id,
            );
            (&*stream).write_all(&reply).unwrap();

            // Close the socket.
            stream.shutdown(Shutdown::Both).unwrap();
        }
    }
}

fn exec_control(
    operator: &mut OperatorNode,
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

    payloader::build_from_binary(&payload_id, &command_response_bytes, HashMap::new())
}
