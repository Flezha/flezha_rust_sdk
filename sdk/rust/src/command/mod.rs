use crate::command;
use crate::compute_engine::ComputeEngineNode;
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, CommandResponse};
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;
use std::collections::HashMap;
use std::io::Write;
use std::net::Shutdown;

mod compute_engine_assign;
mod compute_engine_disconnect;
mod migration_deserialization;
mod migration_serialization;
mod processor_start;
mod processor_state;
mod processor_stop;
mod streams_connect;

pub trait CommandAction {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        command_struct: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>);
}

pub enum CommandActionType {
    Regular(Box<dyn CommandAction>),
    ComputeEngineDisconnect(Box<dyn CommandAction>),
}

pub fn get_handler(_payload_id: &String, message: &String) -> (CommandActionType, String) {
    let wrapper_command =
        CommandRequest::decode(message.as_bytes()).expect("Failed to decode message");

    let reference_id = wrapper_command.reference_id;

    let command_handler = match wrapper_command.command.unwrap() {
        Command::AssignComputeEngineId(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::ConnectSocket(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::AddProcessor(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::RemProcessor(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::MigrationSerialization(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::MigrationDeserialization(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        }
        Command::ProcessorState(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::ComputeEngineDisconnect(proto_ch) => {
            CommandActionType::ComputeEngineDisconnect(Box::new(proto_ch))
        }
    };

    (command_handler, reference_id)
}

pub fn handle_command(
    compute_engine: &mut ComputeEngineNode,
    stream: &FlezhaStream,
    payload_id: &String,
    message: &Vec<u8>,
) {
    let payload_message: String = String::from_utf8_lossy(&message).to_string();

    let (handler_type, reference_id) = command::get_handler(&payload_id, &payload_message);

    match handler_type {
        CommandActionType::Regular(handler) => {
            let reply = command::exec_control(
                compute_engine,
                handler,
                &payload_id,
                &payload_message,
                reference_id,
            );
            if let Err(e) = (&*stream).write_all(&reply) {
                eprintln!("[flezha] Failed to write command response: {}", e);
            }
        }

        CommandActionType::ComputeEngineDisconnect(handler) => {
            let reply = command::exec_control(
                compute_engine,
                handler,
                &payload_id,
                &payload_message,
                reference_id,
            );
            if let Err(e) = (&*stream).write_all(&reply) {
                eprintln!("[flezha] Failed to write disconnect response: {}", e);
            }

            // Close the socket.
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

fn exec_control(
    compute_engine: &mut ComputeEngineNode,
    handler: Box<dyn CommandAction>,
    payload_id: &String,
    message: &String,
    reference_id: String,
) -> Vec<u8> {
    let mut errors: Vec<String> = vec![];
    let mut status = String::from("success");

    let (_payload_id, raw_data) = handler.handle(compute_engine, message, &mut errors);

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
