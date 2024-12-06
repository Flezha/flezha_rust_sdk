use std::thread;
use std::sync::{mpsc, Arc, Mutex};

use crate::command::action;

use super::{ComputationOperator, OperatorAction};


pub fn start_action_receiver(
    operator: Arc<Mutex<ComputationOperator>>,
    rx: Arc<Mutex<mpsc::Receiver<OperatorAction>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            let rx = rx.lock().unwrap();
            match rx.recv() {
                Ok(received) => {
                    match received {
                        OperatorAction::ControlCommand(payload_id, payload) => {
                            let payload_message: String = String::from_utf8_lossy(&payload).to_string();
                            action::handle_action(&operator, &payload_id, &payload_message);
                        }
                        _ => panic!("Unexpected message received on worker channel"),
                    }
                }
                Err(_) => {
                    eprint!("Action receiver channel closed");
                    
                    // TODO: we should probably break here and try to reconnect.
                    return;
                }
            }
        }
    })
}
