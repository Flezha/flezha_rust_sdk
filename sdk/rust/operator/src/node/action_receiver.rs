use std::thread;
use std::sync::{mpsc, Arc, Mutex};

use crate::command::action;

use super::{WorkerNode, NodeAction};


pub fn start_action_receiver(
    worker_node: Arc<Mutex<WorkerNode>>,
    rx: Arc<Mutex<mpsc::Receiver<NodeAction>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            let rx = rx.lock().unwrap();
            match rx.recv() {
                Ok(received) => {
                    match received {
                        NodeAction::ControlCommand(payload_id, payload) => {
                            let payload_message: String = String::from_utf8_lossy(&payload).to_string();
                            action::handle_action(&worker_node, &payload_id, &payload_message);
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
