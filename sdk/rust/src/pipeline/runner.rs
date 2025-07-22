use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, os::unix::net::UnixStream};

use crate::pipeline::factory_registry::PipelineBox;
use crate::pipeline::{PipelineInput, PipelineOutput, PipelineResult, VirtualChannel};
use crate::service::payloader;

pub fn exec(
    pipeline_id: &String,
    pipelines: &Arc<RwLock<HashMap<String, Arc<Mutex<PipelineBox>>>>>,
    input: PipelineInput,
) -> PipelineResult {
    let pipelines_read = pipelines.read().unwrap();

    if let Some(c) = pipelines_read.get(pipeline_id.as_str()) {
        let mut pipeline = c.lock().unwrap();

        pipeline.run(input)
    } else {
        panic!("Pipeline with given ID not found {}", pipeline_id);
    }
}

pub fn exec_agent(
    mut stream: UnixStream,
    pipeline_id: &String,
    pipelines: &Arc<RwLock<HashMap<String, Arc<Mutex<PipelineBox>>>>>,
    input: PipelineInput,
) {
    let pipeline_result = exec(pipeline_id, pipelines, input);

    match pipeline_result {
        PipelineResult::None => {
            return;
        }
        PipelineResult::Forward(outputs) => {
            for (output, virtual_channel) in outputs {
                pipe_to_agent(&mut stream, pipeline_id, &output, virtual_channel);
            }
        }
    }
}

pub fn pipe_to_agent(
    stream: &mut UnixStream,
    pipeline_id: &String,
    result: &PipelineOutput,
    virtual_channel: VirtualChannel,
) {
    // TODO: Hardcoded virtual channel.
    let payload = match result {
        PipelineOutput::Text(text) => {
            payloader::build_from_str(&pipeline_id, virtual_channel, text)
        }
        PipelineOutput::Binary(binary) => {
            payloader::build_from_binary(&pipeline_id, virtual_channel, binary)
        }
        // TODO: Implement custom type serialization
        PipelineOutput::Custom(_) => {
            panic!("Unsupported pipeline output type");
        }
    };

    match stream.write_all(&payload) {
        Ok(_) => {}
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // The stream is not ready to be written to, we need to wait.
            // For simplicity, we'll do a busy wait with a small sleep.
            // A more advanced implementation would use mio's event loop to wait for the stream to become writable.
            loop {
                std::thread::sleep(std::time::Duration::from_micros(100));
                match stream.write_all(&payload) {
                    Ok(_) => break,
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(err) => {
                        eprintln!("Error writing to stream: {}", err);
                        break;
                    }
                }
            }
        }
        Err(err) => {
            eprintln!("Error writing to stream: {}", err);
        }
    }
}
