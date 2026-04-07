use std::collections::HashMap;

use tonic::transport::Channel;

use crate::proto;
use crate::proto::runner_client::RunnerClient;

#[derive(Debug, Clone)]
pub struct RunResult {
    pub status: String,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub elapsed: f64,
    pub files: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct StepResult {
    pub status: String,
    pub exit_code: i32,
    pub elapsed: f64,
    pub files: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct PipelineResult {
    pub all_succeeded: bool,
    pub elapsed: f64,
    pub steps: HashMap<String, StepResult>,
    pub skipped_steps: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub id: String,
    pub model: String,
    pub file_root: String,
    pub inputs: Vec<(String, Vec<u8>)>,
    pub depends_on: Vec<String>,
    pub timeout: Option<u32>,
}

impl Step {
    pub fn new(id: &str, model: &str, file_root: &str) -> Self {
        Self {
            id: id.to_string(),
            model: model.to_string(),
            file_root: file_root.to_string(),
            inputs: Vec::new(),
            depends_on: Vec::new(),
            timeout: None,
        }
    }

    pub fn with_input(mut self, name: &str, data: &[u8]) -> Self {
        self.inputs.push((name.to_string(), data.to_vec()));
        self
    }

    pub fn depends_on(mut self, deps: &[&str]) -> Self {
        self.depends_on = deps.iter().map(|s| s.to_string()).collect();
        self
    }
}

pub struct ATSession {
    client: RunnerClient<Channel>,
    chunk_size: usize,
}

impl ATSession {
    pub async fn connect(target: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = RunnerClient::connect(format!("http://{target}")).await?;
        Ok(Self {
            client,
            chunk_size: 65536,
        })
    }

    pub async fn upload(
        &mut self,
        name: &str,
        data: &[u8],
    ) -> Result<(String, u64), Box<dyn std::error::Error>> {
        let chunks = build_upload_chunks(name, data, self.chunk_size);
        let resp = self
            .client
            .upload_file(tokio_stream::iter(chunks))
            .await?
            .into_inner();
        Ok((resp.name, resp.size_bytes))
    }

    pub async fn download(&mut self, name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .get_file(proto::GetFileRequest {
                name: name.to_string(),
            })
            .await?;

        let mut stream = resp.into_inner();
        let mut buf = Vec::new();
        while let Some(chunk) = stream.message().await? {
            buf.extend_from_slice(&chunk.data);
        }
        Ok(buf)
    }

    pub async fn delete(&mut self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .delete_file(proto::DeleteFileRequest {
                name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    pub async fn list_files(&mut self) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .list_files(proto::ListFilesRequest {})
            .await?
            .into_inner();
        Ok(resp
            .files
            .into_iter()
            .map(|f| (f.name, f.size_bytes))
            .collect())
    }

    pub async fn run(
        &mut self,
        model: &str,
        file_root: &str,
    ) -> Result<RunResult, Box<dyn std::error::Error>> {
        self.run_with_timeout(model, file_root, None).await
    }

    pub async fn run_with_timeout(
        &mut self,
        model: &str,
        file_root: &str,
        timeout: Option<u32>,
    ) -> Result<RunResult, Box<dyn std::error::Error>> {
        let req = proto::RunRequest {
            model: model.to_string(),
            file_root: file_root.to_string(),
            timeout_seconds: timeout,
        };

        let mut stream = self.client.run(req).await?.into_inner();
        let mut stdout_parts: Vec<Vec<u8>> = Vec::new();
        let mut stderr_parts: Vec<Vec<u8>> = Vec::new();
        let mut status = "unspecified".to_string();
        let mut exit_code = -1i32;
        let mut elapsed = 0.0f64;
        let mut files: HashMap<String, Vec<u8>> = HashMap::new();
        let mut current_file: Option<String> = None;
        let mut current_data: Vec<Vec<u8>> = Vec::new();

        while let Some(msg) = stream.message().await? {
            if let Some(payload) = msg.payload {
                match payload {
                    proto::run_output::Payload::Started(_) => {}
                    proto::run_output::Payload::Output(chunk) => {
                        if chunk.stream == proto::OutputStream::Stdout as i32 {
                            stdout_parts.push(chunk.data);
                        } else {
                            stderr_parts.push(chunk.data);
                        }
                    }
                    proto::run_output::Payload::Completed(c) => {
                        status = crate::status_str(c.status);
                        exit_code = c.exit_code;
                        elapsed = c.elapsed_seconds;
                    }
                    proto::run_output::Payload::File(fc) => {
                        if !fc.name.is_empty() {
                            if let Some(name) = current_file.take() {
                                files.insert(name, current_data.concat());
                                current_data.clear();
                            }
                            current_file = Some(fc.name);
                        }
                        current_data.push(fc.data);
                    }
                }
            }
        }

        if let Some(name) = current_file {
            files.insert(name, current_data.concat());
        }

        Ok(RunResult {
            status,
            exit_code,
            stdout: String::from_utf8_lossy(&stdout_parts.concat()).to_string(),
            stderr: String::from_utf8_lossy(&stderr_parts.concat()).to_string(),
            elapsed,
            files,
        })
    }

    pub async fn run_pipeline(
        &mut self,
        steps: &[Step],
    ) -> Result<PipelineResult, Box<dyn std::error::Error>> {
        self.run_pipeline_with_timeout(steps, None).await
    }

    pub async fn run_pipeline_with_timeout(
        &mut self,
        steps: &[Step],
        timeout: Option<u32>,
    ) -> Result<PipelineResult, Box<dyn std::error::Error>> {
        let pb_steps: Vec<proto::PipelineStep> = steps
            .iter()
            .map(|s| proto::PipelineStep {
                id: s.id.clone(),
                model: s.model.clone(),
                file_root: s.file_root.clone(),
                inputs: s
                    .inputs
                    .iter()
                    .map(|(name, data)| proto::File {
                        name: name.clone(),
                        content: data.clone(),
                    })
                    .collect(),
                depends_on: s.depends_on.clone(),
                timeout_seconds: s.timeout,
            })
            .collect();

        let req = proto::RunPipelineRequest {
            steps: pb_steps,
            timeout_seconds: timeout,
        };

        let mut stream = self.client.run_pipeline(req).await?.into_inner();

        let mut step_results: HashMap<String, StepResult> = HashMap::new();
        let mut current_files: HashMap<String, (String, Vec<Vec<u8>>)> = HashMap::new();
        let mut all_succeeded = false;
        let mut elapsed = 0.0f64;
        let mut skipped_steps = Vec::new();

        while let Some(msg) = stream.message().await? {
            if let Some(payload) = msg.payload {
                match payload {
                    proto::pipeline_output::Payload::PipelineStarted(_) => {}
                    proto::pipeline_output::Payload::Step(ev) => {
                        let sid = ev.step_id.clone();
                        if let Some(detail) = ev.detail {
                            match detail {
                                proto::step_event::Detail::Started(_) => {
                                    step_results.entry(sid).or_insert_with(|| StepResult {
                                        status: "unspecified".to_string(),
                                        exit_code: -1,
                                        elapsed: 0.0,
                                        files: HashMap::new(),
                                    });
                                }
                                proto::step_event::Detail::Output(_) => {}
                                proto::step_event::Detail::Completed(c) => {
                                    finalize_file(&sid, &mut current_files, &mut step_results);
                                    let sr =
                                        step_results.entry(sid).or_insert_with(|| StepResult {
                                            status: "unspecified".to_string(),
                                            exit_code: -1,
                                            elapsed: 0.0,
                                            files: HashMap::new(),
                                        });
                                    sr.status = crate::status_str(c.status);
                                    sr.exit_code = c.exit_code;
                                    sr.elapsed = c.elapsed_seconds;
                                }
                                proto::step_event::Detail::File(fc) => {
                                    if !fc.name.is_empty() {
                                        finalize_file(&sid, &mut current_files, &mut step_results);
                                        current_files.insert(sid, (fc.name, vec![fc.data]));
                                    } else if let Some((_, parts)) = current_files.get_mut(&sid) {
                                        parts.push(fc.data);
                                    }
                                }
                            }
                        }
                    }
                    proto::pipeline_output::Payload::PipelineCompleted(c) => {
                        for sid in current_files.keys().cloned().collect::<Vec<_>>() {
                            finalize_file(&sid, &mut current_files, &mut step_results);
                        }
                        all_succeeded = c.all_succeeded;
                        elapsed = c.elapsed_seconds;
                        skipped_steps = c.skipped_steps;
                    }
                }
            }
        }

        Ok(PipelineResult {
            all_succeeded,
            elapsed,
            steps: step_results,
            skipped_steps,
        })
    }
}

fn build_upload_chunks(name: &str, data: &[u8], chunk_size: usize) -> Vec<proto::FileChunk> {
    let mut chunks = Vec::new();
    let mut offset = 0;
    let mut first = true;
    while offset < data.len() || first {
        let end = (offset + chunk_size).min(data.len());
        chunks.push(proto::FileChunk {
            name: if first {
                name.to_string()
            } else {
                String::new()
            },
            data: data[offset..end].to_vec(),
        });
        first = false;
        offset = end;
    }
    chunks
}

fn finalize_file(
    sid: &str,
    current_files: &mut HashMap<String, (String, Vec<Vec<u8>>)>,
    step_results: &mut HashMap<String, StepResult>,
) {
    if let Some((fname, parts)) = current_files.remove(sid) {
        let sr = step_results
            .entry(sid.to_string())
            .or_insert_with(|| StepResult {
                status: "unspecified".to_string(),
                exit_code: -1,
                elapsed: 0.0,
                files: HashMap::new(),
            });
        sr.files.insert(fname, parts.concat());
    }
}
