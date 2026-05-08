use crate::command;
use crate::executor::ExecutorNode;
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, CommandResponse};
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;
use std::collections::HashMap;
use std::io::Write;
use std::net::Shutdown;

mod compact;
mod executor_assign;
mod executor_disconnect;
mod migration_deserialization;
mod migration_serialization;
mod partition_status;
mod partition_apply_state;
mod relay_message;
mod relay_start;
pub(crate) mod partition_state;
mod relay_stop;
mod restore;
mod streams_connect;

pub trait CommandAction {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        command_struct: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>);
}

pub enum CommandActionType {
    Regular(Box<dyn CommandAction>),
    ExecutorDisconnect(Box<dyn CommandAction>),
    /// Agent -> SDK `ExecutorShutdownAck`. Wake the corresponding
    /// per-relay wait channel registered by the SDK's graceful-
    /// shutdown routine. No `CommandResponse` is written — the controller
    /// sent this as its own `CommandRequest`, not in reply to one we
    /// sent. See ship-fresh-state-on-sdk-shutdown.
    AckSignal,
    /// Defensive: SDK should never receive `ExecutorShutdownNotification`
    /// (it's the SDK->controller direction). Logged and dropped if a buggy
    /// or future controller ever emits one.
    NotificationIgnore,
}

pub fn get_handler(_payload_id: &String, message: &String) -> (CommandActionType, String) {
    let wrapper_command =
        CommandRequest::decode(message.as_bytes()).expect("Failed to decode message");

    let reference_id = wrapper_command.reference_id;

    let command_handler = match wrapper_command.command.unwrap() {
        Command::AssignExecutorId(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::ConnectSocket(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::AddRelay(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::RemRelay(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::MigrationSerialization(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::MigrationDeserialization(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        }
        Command::PartitionState(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::ExecutorDisconnect(proto_ch) => {
            CommandActionType::ExecutorDisconnect(Box::new(proto_ch))
        }
        Command::RelayMessage(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::PartitionApplyState(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::ExecutorShutdownNotification(_) => CommandActionType::NotificationIgnore,
        Command::ExecutorShutdownAck(_) => CommandActionType::AckSignal,
        Command::RestoreInitState(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        // SDK is the SENDER for the response variants — receiving them
        // is a protocol bug. Drop the message, log it like the other
        // direction-mistake (`ExecutorShutdownNotification`).
        Command::RestoreInitStateResponse(_) => CommandActionType::NotificationIgnore,
        Command::CompactPartition(proto_ch) => CommandActionType::Regular(Box::new(proto_ch)),
        Command::CompactPartitionResponse(_) => CommandActionType::NotificationIgnore,
        Command::PartitionMailboxStatus(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        }
    };

    (command_handler, reference_id)
}

pub fn handle_command(
    executor: &mut ExecutorNode,
    stream: &FlezhaStream,
    payload_id: &String,
    message: &Vec<u8>,
) {
    let payload_message: String = String::from_utf8_lossy(&message).to_string();

    let (handler_type, reference_id) = command::get_handler(&payload_id, &payload_message);

    match handler_type {
        CommandActionType::Regular(handler) => {
            let reply = command::exec_control(
                executor,
                handler,
                &payload_id,
                &payload_message,
                reference_id,
            );
            if let Err(e) = (&*stream).write_all(&reply) {
                eprintln!("[flezha] Failed to write command response: {}", e);
            }
        }

        CommandActionType::ExecutorDisconnect(handler) => {
            let reply = command::exec_control(
                executor,
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

        CommandActionType::AckSignal => {
            // Suppress unused-variable warnings for unused params.
            let _ = executor;
            crate::shutdown::signal_ack(&reference_id);
        }

        CommandActionType::NotificationIgnore => {
            let _ = executor;
            eprintln!(
                "[flezha] Received unexpected ExecutorShutdownNotification (ref={}); \
                 ignoring — SDK is the SENDER, not the receiver.",
                reference_id
            );
        }
    }
}

fn exec_control(
    executor: &mut ExecutorNode,
    handler: Box<dyn CommandAction>,
    payload_id: &String,
    message: &String,
    reference_id: String,
) -> Vec<u8> {
    let mut errors: Vec<String> = vec![];
    let mut status = String::from("success");

    let (_payload_id, raw_data) = handler.handle(executor, message, &mut errors);

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
