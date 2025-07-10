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
    stream: &mut UnixStream,
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
                pipe_to_agent(stream, pipeline_id, &output, virtual_channel);

                match output {
                    PipelineOutput::Binary(bin) => {
                        let string = String::from_utf8(bin).unwrap();

                        println!("Handled message: {}", string);
                    }
                    _ => {
                        println!("Handled something else");
                    }
                }
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

    if let Err(err) = stream.write_all(&payload) {
        eprintln!("Error writing to stream: {}", err);
    }
}
