use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;
use tracing::{info, warn};

use crate::executor::{self, ExecRequest, StreamEvent};
use crate::proto::{self, runner_server::Runner, FileChunk};
use crate::session::new_session_id;
use crate::{pipeline, workspace, AppState};

pub struct RunnerService {
    state: Arc<AppState>,
}

impl RunnerService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    fn resolve_executable(&self, model: &str) -> Result<std::path::PathBuf, Status> {
        if !self.state.allowlist.contains(model) {
            return Err(Status::invalid_argument(format!(
                "unknown model: {model:?} (available: {:?})",
                self.state.allowlist
            )));
        }
        Ok(self.state.bin_dir.join(format!("{model}.exe")))
    }

    fn resolve_timeout(&self, requested: Option<u32>) -> Duration {
        Duration::from_secs(
            requested
                .map(|t| t as u64)
                .unwrap_or(self.state.default_timeout),
        )
    }
}

#[tonic::async_trait]
impl Runner for RunnerService {
    // ================================================================
    // Tier 1: RunSync
    // ================================================================

    async fn run_sync(
        &self,
        request: Request<proto::RunSyncRequest>,
    ) -> Result<Response<proto::RunSyncResponse>, Status> {
        let session_id = new_session_id();
        let req = request.into_inner();
        let executable = self.resolve_executable(&req.model)?;
        let timeout = self.resolve_timeout(req.timeout_seconds);

        let _guard = self.state.exec_lock.read().await;

        let tmp = tempfile::tempdir().map_err(|e| Status::internal(format!("tmpdir: {e}")))?;
        let run_dir = tmp.path().to_path_buf();

        let inputs: Vec<(String, Vec<u8>)> = req
            .inputs
            .into_iter()
            .map(|f| (f.name, f.content))
            .collect();

        let input_summary: Vec<(&str, usize)> =
            inputs.iter().map(|(n, d)| (n.as_str(), d.len())).collect();

        info!(
            session_id = %session_id,
            model = %req.model,
            file_root = %req.file_root,
            timeout_secs = timeout.as_secs(),
            inline_input_count = inputs.len(),
            inline_inputs = ?input_summary,
            "RunSync started"
        );

        executor::populate_run_dir(&session_id, &run_dir, &self.state.workspace, &inputs)
            .await
            .map_err(|e| Status::internal(format!("populate: {e}")))?;

        let result = executor::run_buffered(ExecRequest {
            session_id: session_id.clone(),
            executable,
            file_root: req.file_root.clone(),
            run_dir: run_dir.clone(),
            timeout,
        })
        .await
        .map_err(|e| Status::internal(format!("exec: {e}")))?;

        let mut outputs = Vec::new();
        let mut total_output_bytes: u64 = 0;
        let max_response = 256 * 1024 * 1024_u64; // match server encoding limit

        for (name, path) in &result.output_files {
            let meta = tokio::fs::metadata(path).await.ok();
            let file_size = meta.map(|m| m.len()).unwrap_or(0);
            total_output_bytes += file_size;

            if total_output_bytes > max_response {
                return Err(Status::out_of_range(format!(
                    "RunSync output too large ({total_output_bytes} bytes). \
                     Use Run (Tier 2) for large outputs."
                )));
            }

            let content = tokio::fs::read(path)
                .await
                .map_err(|e| Status::internal(format!("read output {name}: {e}")))?;
            outputs.push(proto::File {
                name: name.clone(),
                content,
            });
        }

        let returned: Vec<(&str, usize)> = outputs
            .iter()
            .map(|f| (f.name.as_str(), f.content.len()))
            .collect();

        info!(
            session_id = %session_id,
            model = %req.model,
            status = ?result.status,
            exit_code = result.exit_code,
            elapsed_secs = result.elapsed.as_secs_f64(),
            response_output_count = outputs.len(),
            response_output_bytes_total = total_output_bytes,
            response_outputs = ?returned,
            "RunSync completed"
        );

        Ok(Response::new(proto::RunSyncResponse {
            status: result.status.into(),
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
            elapsed_seconds: result.elapsed.as_secs_f64(),
            outputs,
        }))
    }

    // ================================================================
    // Tier 2: Workspace RPCs
    // ================================================================

    async fn upload_file(
        &self,
        request: Request<Streaming<FileChunk>>,
    ) -> Result<Response<proto::UploadResponse>, Status> {
        workspace::upload_file(&self.state, request).await
    }

    type GetFileStream = ReceiverStream<Result<FileChunk, Status>>;

    async fn get_file(
        &self,
        request: Request<proto::GetFileRequest>,
    ) -> Result<Response<Self::GetFileStream>, Status> {
        workspace::get_file(&self.state, request).await
    }

    async fn delete_file(
        &self,
        request: Request<proto::DeleteFileRequest>,
    ) -> Result<Response<proto::DeleteFileResponse>, Status> {
        workspace::delete_file(&self.state, request).await
    }

    async fn list_files(
        &self,
        request: Request<proto::ListFilesRequest>,
    ) -> Result<Response<proto::ListFilesResponse>, Status> {
        workspace::list_files(&self.state, request).await
    }

    // ================================================================
    // Tier 2: Run (streaming)
    // ================================================================

    type RunStream = ReceiverStream<Result<proto::RunOutput, Status>>;

    async fn run(
        &self,
        request: Request<proto::RunRequest>,
    ) -> Result<Response<Self::RunStream>, Status> {
        let session_id = new_session_id();
        let req = request.into_inner();
        let executable = self.resolve_executable(&req.model)?;
        let timeout = self.resolve_timeout(req.timeout_seconds);

        let _write_guard = self
            .state
            .exec_lock
            .clone()
            .try_write_owned()
            .map_err(|_| Status::failed_precondition("another Run or RunPipeline is active"))?;

        let workspace = self.state.workspace.clone();
        let chunk_size = self.state.chunk_size;
        let model = req.model.clone();
        let file_root = req.file_root.clone();

        info!(
            session_id = %session_id,
            model = %model,
            file_root = %file_root,
            timeout_secs = timeout.as_secs(),
            run_dir = %workspace.display(),
            "Run started"
        );

        let run_span = tracing::info_span!("Run", session_id = %session_id);
        let (out_tx, out_rx) = mpsc::channel(32);
        let sid = session_id.clone();

        tokio::spawn(
            async move {
                let _guard = _write_guard;
                let send = |msg: proto::RunOutput| {
                    let tx = out_tx.clone();
                    async move { tx.send(Ok(msg)).await }
                };

                // Phase 1: RunStarted
                let _ = send(proto::RunOutput {
                    payload: Some(proto::run_output::Payload::Started(proto::RunStarted {
                        model: model.clone(),
                        file_root: file_root.clone(),
                    })),
                })
                .await;

                // Phase 2 + 3: Stream output, then completed
                let (event_tx, mut event_rx) = mpsc::channel(32);
                let exec_req = ExecRequest {
                    session_id: sid.clone(),
                    executable,
                    file_root,
                    run_dir: workspace.clone(),
                    timeout,
                };

                let exec_span = tracing::Span::current();
                let sid_exec = sid.clone();
                tokio::spawn(async move {
                    if let Err(e) = executor::run_streaming(exec_req, event_tx)
                        .instrument(exec_span)
                        .await
                    {
                        tracing::error!(session_id = %sid_exec, error = %e, "run_streaming failed");
                    }
                });

                let mut output_files: Vec<(String, std::path::PathBuf)> = Vec::new();
                let mut last_status: Option<i32> = None;
                let mut last_exit: Option<i32> = None;
                let mut last_elapsed: Option<f64> = None;

                while let Some(event) = event_rx.recv().await {
                    match event {
                        StreamEvent::Output(chunk) => {
                            let _ = send(proto::RunOutput {
                                payload: Some(proto::run_output::Payload::Output(chunk)),
                            })
                            .await;
                        }
                        StreamEvent::Completed(completed, files) => {
                            last_status = Some(completed.status);
                            last_exit = Some(completed.exit_code);
                            last_elapsed = Some(completed.elapsed_seconds);
                            output_files = files;
                            let _ = send(proto::RunOutput {
                                payload: Some(proto::run_output::Payload::Completed(completed)),
                            })
                            .await;
                        }
                    }
                }

                // Phase 4: Stream output files
                let mut streamed: Vec<(String, u64)> = Vec::new();
                for (name, path) in &output_files {
                    let mut file = match tokio::fs::File::open(path).await {
                        Ok(f) => f,
                        Err(e) => {
                            warn!(
                                session_id = %sid,
                                file = %name,
                                error = %e,
                                "skip output file (open failed)"
                            );
                            continue;
                        }
                    };

                    let len = match file.metadata().await {
                        Ok(m) => m.len(),
                        Err(_) => 0,
                    };
                    let num_chunks = len.div_ceil(chunk_size as u64);

                    let mut buf = vec![0u8; chunk_size];
                    let mut first = true;

                    loop {
                        match file.read(&mut buf).await {
                            Ok(0) => {
                                if first {
                                    let chunk = FileChunk {
                                        name: name.clone(),
                                        data: Vec::new(),
                                    };
                                    let _ = send(proto::RunOutput {
                                        payload: Some(proto::run_output::Payload::File(chunk)),
                                    })
                                    .await;
                                }
                                break;
                            }
                            Ok(n) => {
                                let chunk = FileChunk {
                                    name: if first { name.clone() } else { String::new() },
                                    data: buf[..n].to_vec(),
                                };
                                first = false;
                                let _ = send(proto::RunOutput {
                                    payload: Some(proto::run_output::Payload::File(chunk)),
                                })
                                .await;
                            }
                            Err(e) => {
                                warn!(
                                    session_id = %sid,
                                    file = %name,
                                    error = %e,
                                    "error reading output file"
                                );
                                break;
                            }
                        }
                    }

                    streamed.push((name.clone(), len));
                    info!(
                        session_id = %sid,
                        file = %name,
                        bytes = len,
                        chunks = num_chunks,
                        chunk_size,
                        "streamed output file to client"
                    );
                }

                info!(
                    session_id = %sid,
                    model = %model,
                    run_status = ?last_status,
                    exit_code = ?last_exit,
                    elapsed_secs = ?last_elapsed,
                    files_streamed = streamed.len(),
                    streamed_files = ?streamed,
                    "Run finished"
                );
            }
            .instrument(run_span),
        );

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }

    // ================================================================
    // Tier 3: RunPipeline
    // ================================================================

    type RunPipelineStream = ReceiverStream<Result<proto::PipelineOutput, Status>>;

    async fn run_pipeline(
        &self,
        request: Request<proto::RunPipelineRequest>,
    ) -> Result<Response<Self::RunPipelineStream>, Status> {
        pipeline::run_pipeline(&self.state, request).await
    }
}
