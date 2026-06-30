use std::collections::HashMap;
use std::io::Write;
use std::net::Shutdown;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::command;
use crate::executor::ExecutorNode;
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, CommandResponse};

mod compact;
mod executor_assign;
mod executor_disconnect;
mod migration_deserialization;
mod migration_serialization;
mod operation_message;
mod operative_start;
mod operative_stop;
mod ping;
mod restore;
mod segment_apply_state;
pub mod segment_state;
mod segment_status;
mod streams_connect;

pub trait CommandAction {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        command_struct: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>);
}

pub enum CommandActionType {
    Regular(Box<dyn CommandAction>),
    ExecutorDisconnect(Box<dyn CommandAction>),
    /// Controller -> SDK `ExecutorShutdownAck`. Wake the corresponding
    /// per-operative wait channel registered by the SDK's graceful-shutdown
    /// routine. No `CommandResponse` is written — the controller sent
    /// this as its own `CommandRequest`, not in reply to one we sent.
    AckSignal,
    /// Defensive: SDK should never receive `ExecutorShutdownNotification`
    /// (it's the SDK->controller direction). Logged and dropped if a buggy
    /// or future controller ever emits one.
    NotificationIgnore,
}

/// Surface of `get_handler` so callers can distinguish a transport-edge
/// event (peer closed the socket cleanly → reconnect) from a decoded
/// command frame.
pub enum FrameOutcome {
    PeerClosed,
    Command(CommandActionType, String),
}

impl std::fmt::Debug for FrameOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameOutcome::PeerClosed => write!(f, "FrameOutcome::PeerClosed"),
            FrameOutcome::Command(_, ref_id) => {
                write!(f, "FrameOutcome::Command(<handler>, {:?})", ref_id)
            }
        }
    }
}

/// Surface of `get_handler` failure cases. The control-channel reader
/// MUST drive the existing reconnect loop on either variant and MUST NOT
/// panic. `Malformed` is for non-empty bytes that fail `prost::decode`;
/// `EmptyCommand` is for a well-formed `CommandRequest` whose `command`
/// oneof is `None`.
#[derive(Debug)]
pub enum CommandDecodeError {
    Malformed { stack: String },
    EmptyCommand,
}

pub fn get_handler(_payload_id: &str, message: &[u8]) -> Result<FrameOutcome, CommandDecodeError> {
    if message.is_empty() {
        return Ok(FrameOutcome::PeerClosed);
    }

    let wrapper_command =
        CommandRequest::decode(message).map_err(|e| CommandDecodeError::Malformed {
            stack: format!("{:?}", e),
        })?;

    let reference_id = wrapper_command.reference_id;

    let command_handler = match wrapper_command.command {
        Some(Command::AssignExecutorId(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        Some(Command::ConnectSocket(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        Some(Command::AddOperative(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        Some(Command::RemOperative(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        Some(Command::MigrationSerialization(proto_ch)) => {
            CommandActionType::Regular(Box::new(proto_ch))
        }
        Some(Command::MigrationDeserialization(proto_ch)) => {
            CommandActionType::Regular(Box::new(proto_ch))
        }
        Some(Command::SegmentState(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        Some(Command::ExecutorDisconnect(proto_ch)) => {
            CommandActionType::ExecutorDisconnect(Box::new(proto_ch))
        }
        Some(Command::OperationMessage(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        Some(Command::SegmentApplyState(proto_ch)) => {
            CommandActionType::Regular(Box::new(proto_ch))
        }
        Some(Command::ExecutorShutdownNotification(_)) => CommandActionType::NotificationIgnore,
        Some(Command::ExecutorShutdownAck(_)) => CommandActionType::AckSignal,
        Some(Command::RestoreInitState(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        // SDK is the SENDER for the response variants — receiving them
        // is a protocol bug. Drop the message, log it like the other
        // direction-mistake (`ExecutorShutdownNotification`).
        Some(Command::RestoreInitStateResponse(_)) => CommandActionType::NotificationIgnore,
        Some(Command::CompactSegment(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        Some(Command::CompactSegmentResponse(_)) => CommandActionType::NotificationIgnore,
        Some(Command::SegmentMailboxStatus(proto_ch)) => {
            CommandActionType::Regular(Box::new(proto_ch))
        }
        Some(Command::Ping(proto_ch)) => CommandActionType::Regular(Box::new(proto_ch)),
        // Retired reconnect machinery. The SDK no longer sends
        // `ExecutorReconnected` (it re-presents its stable id every connect) and
        // the controller no longer mints-and-reassigns ids (reconnect is a
        // controller-side binding swap), so neither is handled — drop and log.
        Some(Command::ExecutorReconnected(_)) => CommandActionType::NotificationIgnore,
        Some(Command::ReassignExecutorId(_)) => CommandActionType::NotificationIgnore,
        // SDK is the SENDER for BoundedCompleted — receiving it is a protocol
        // bug. Drop and log, like `ExecutorShutdownNotification`.
        Some(Command::BoundedCompleted(_)) => CommandActionType::NotificationIgnore,
        // Cooperative external stop for a bounded operative (Controller -> SDK).
        Some(Command::BoundedStop(_)) => CommandActionType::NotificationIgnore,
        None => return Err(CommandDecodeError::EmptyCommand),
    };

    Ok(FrameOutcome::Command(command_handler, reference_id))
}

pub fn handle_command(
    executor: &mut ExecutorNode,
    stream: &FlezhaStream,
    payload_id: &str,
    message: &[u8],
) {
    // Controller's keepalive echo (see flz_transport liveness): the inbound
    // read already re-armed the steady-state idle watchdog, and the frame
    // carries a bare PingResponse on its own payload — nothing to dispatch.
    if payload_id == flz_transport::transport::liveness::CONTROL_PING_PAYLOAD_ID {
        return;
    }

    let outcome = match command::get_handler(payload_id, message) {
        Ok(o) => o,
        Err(CommandDecodeError::Malformed { stack }) => {
            eprintln!(
                "[flezha] control-channel frame failed to decode (stack={}); \
                 dropping and waiting for the read loop to drive reconnect.",
                stack
            );
            return;
        }
        Err(CommandDecodeError::EmptyCommand) => {
            eprintln!(
                "[flezha] control-channel frame had `command: None`; dropping. \
                 The next frame on this connection will be processed normally."
            );
            return;
        }
    };

    let (handler_type, reference_id) = match outcome {
        FrameOutcome::PeerClosed => {
            // The control-channel reader (`payloader::extract_payload`)
            // is the canonical detector of EOF and emits
            // `ExecutorMessage::Reconnect` on its own io::Error path.
            // Dropping this frame here is a defence-in-depth no-op in
            // case a 0-byte payload ever flows in band.
            return;
        }
        FrameOutcome::Command(handler, reference_id) => (handler, reference_id),
    };

    match handler_type {
        CommandActionType::Regular(handler) => {
            let reply = command::exec_control(executor, handler, payload_id, message, reference_id);
            if let Err(e) = (&*stream).write_all(&reply) {
                eprintln!("[flezha] Failed to write command response: {}", e);
            }
        }

        CommandActionType::ExecutorDisconnect(handler) => {
            let reply = command::exec_control(executor, handler, payload_id, message, reference_id);
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
                "[flezha] Received an unexpected controller->SDK command the SDK does \
                 not handle (ref={}); ignoring.",
                reference_id
            );
        }
    }
}

fn exec_control(
    executor: &mut ExecutorNode,
    handler: Box<dyn CommandAction>,
    payload_id: &str,
    message: &[u8],
    reference_id: String,
) -> Vec<u8> {
    let mut errors: Vec<String> = vec![];
    let mut status = String::from("success");

    let message_str = std::str::from_utf8(message).unwrap_or("");
    let (_payload_id, raw_data) = handler.handle(executor, message_str, &mut errors);

    if !errors.is_empty() {
        status = "error".to_string();
    }

    let command_response: CommandResponse = CommandResponse {
        reference_id,
        status,
        errors,
        raw_data,
    };

    let command_response_bytes = command_response.encode_to_vec();

    payloader::build_from_binary(payload_id, &command_response_bytes, HashMap::new())
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn empty_bytes_signal_peer_closed() {
        let out = get_handler("payload", &[]).expect("empty input is not an error");
        assert!(matches!(out, FrameOutcome::PeerClosed));
    }

    #[test]
    fn truncated_prefix_returns_malformed_error() {
        // Three bytes: a valid prost varint prefix that promises more data
        // than is present. Decode MUST fail with a typed error rather
        // than panic.
        let err = get_handler("payload", &[0x08, 0x01, 0x12]).expect_err("truncation must error");
        match err {
            CommandDecodeError::Malformed { stack } => {
                assert!(
                    !stack.is_empty(),
                    "malformed-error stack must carry the prost diagnostic"
                );
            }
            other => panic!("expected Malformed, got {:?}", other),
        }
    }

    #[test]
    fn empty_command_oneof_returns_typed_error() {
        let request = CommandRequest {
            reference_id: "ref-empty".to_string(),
            command: None,
        };

        let bytes = request.encode_to_vec();
        let err = get_handler("payload", &bytes).expect_err("None command must error");
        assert!(matches!(err, CommandDecodeError::EmptyCommand));
    }

    #[test]
    fn well_formed_request_returns_command_variant() {
        let request = CommandRequest {
            reference_id: "ref-shutdown".to_string(),
            command: Some(Command::ExecutorShutdownAck(
                crate::pb::ExecutorShutdownAck {
                    operative_id: "".to_string(),
                },
            )),
        };

        let bytes = request.encode_to_vec();
        let out = get_handler("payload", &bytes).expect("well-formed request must decode");

        match out {
            FrameOutcome::Command(_, ref_id) => assert_eq!(ref_id, "ref-shutdown"),
            FrameOutcome::PeerClosed => panic!("well-formed request must not be PeerClosed"),
        }
    }

    #[test]
    fn no_get_handler_input_causes_panic() {
        // Final guard: catch_unwind across every error-edge input proves
        // the path no longer panics.
        let empty_command = CommandRequest {
            reference_id: "x".to_string(),
            command: None,
        }
        .encode_to_vec();

        let valid_request = CommandRequest {
            reference_id: "y".to_string(),
            command: Some(Command::ExecutorShutdownAck(
                crate::pb::ExecutorShutdownAck {
                    operative_id: "".to_string(),
                },
            )),
        }
        .encode_to_vec();

        let inputs: Vec<Vec<u8>> = vec![
            Vec::new(),
            vec![0x08, 0x01, 0x12],
            empty_command,
            valid_request,
        ];

        for input in inputs {
            let owned = input.clone();
            let result = std::panic::catch_unwind(|| {
                let _ = get_handler("payload", &owned);
            });
            assert!(
                result.is_ok(),
                "get_handler must not panic on input {:?}",
                input
            );
        }
    }
}
