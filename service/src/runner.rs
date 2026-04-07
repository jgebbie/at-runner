use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::executor::{self, ExecRequest, StreamEvent};
use crate::proto::{self, runner_server::Runner, FileChunk};
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

        executor::populate_run_dir(&run_dir, &self.state.workspace, &inputs)
            .await
            .map_err(|e| Status::internal(format!("populate: {e}")))?;

        info!(model = %req.model, file_root = %req.file_root, "RunSync");

        let result = executor::run_buffered(ExecRequest {
            executable,
            file_root: req.file_root,
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
        let req = request.into_inner();
        let executable = self.resolve_executable(&req.model)?;
        let timeout = self.resolve_timeout(req.timeout_seconds);

        let _write_guard = self
            .state
            .exec_lock
            .try_write()
            .map_err(|_| Status::failed_precondition("another Run or RunPipeline is active"))?;

        let workspace = self.state.workspace.clone();
        let chunk_size = self.state.chunk_size;
        let model = req.model.clone();
        let file_root = req.file_root.clone();

        info!(model = %model, file_root = %file_root, "Run");

        let (out_tx, out_rx) = mpsc::channel(32);

        tokio::spawn(async move {
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
                executable,
                file_root,
                run_dir: workspace.clone(),
                timeout,
            };

            tokio::spawn(async move {
                if let Err(e) = executor::run_streaming(exec_req, event_tx).await {
                    tracing::error!("run_streaming error: {e}");
                }
            });

            let mut output_files: Vec<(String, std::path::PathBuf)> = Vec::new();

            while let Some(event) = event_rx.recv().await {
                match event {
                    StreamEvent::Output(chunk) => {
                        let _ = send(proto::RunOutput {
                            payload: Some(proto::run_output::Payload::Output(chunk)),
                        })
                        .await;
                    }
                    StreamEvent::Completed(completed, files) => {
                        output_files = files;
                        let _ = send(proto::RunOutput {
                            payload: Some(proto::run_output::Payload::Completed(completed)),
                        })
                        .await;
                    }
                }
            }

            // Phase 4: Stream output files
            for (name, path) in &output_files {
                let data = match tokio::fs::read(path).await {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let mut offset = 0;
                let mut first = true;
                while offset < data.len() {
                    let end = (offset + chunk_size).min(data.len());
                    let chunk = FileChunk {
                        name: if first { name.clone() } else { String::new() },
                        data: data[offset..end].to_vec(),
                    };
                    first = false;
                    let _ = send(proto::RunOutput {
                        payload: Some(proto::run_output::Payload::File(chunk)),
                    })
                    .await;
                    offset = end;
                }
            }
        });

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
