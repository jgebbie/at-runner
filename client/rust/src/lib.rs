pub mod session;

pub mod proto {
    tonic::include_proto!("at.runner.v1");
}

pub use session::{ATSession, PipelineResult, RunResult, Step, StepResult};

use std::collections::HashMap;

/// Tier 1: Blocking one-shot run. Creates a temporary tokio runtime internally.
pub fn run_sync(
    target: &str,
    model: &str,
    file_root: &str,
    inputs: &[(&str, &[u8])],
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_sync_async(target, model, file_root, inputs))
}

/// Async version of run_sync.
pub async fn run_sync_async(
    target: &str,
    model: &str,
    file_root: &str,
    inputs: &[(&str, &[u8])],
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let mut client =
        proto::runner_client::RunnerClient::connect(format!("http://{target}")).await?;

    let req = proto::RunSyncRequest {
        model: model.to_string(),
        file_root: file_root.to_string(),
        inputs: inputs
            .iter()
            .map(|(name, content)| proto::File {
                name: name.to_string(),
                content: content.to_vec(),
            })
            .collect(),
        timeout_seconds: None,
    };

    let resp = client.run_sync(req).await?.into_inner();

    let files: HashMap<String, Vec<u8>> = resp
        .outputs
        .into_iter()
        .map(|f| (f.name, f.content))
        .collect();

    Ok(RunResult {
        status: status_str(resp.status),
        exit_code: resp.exit_code,
        stdout: String::from_utf8_lossy(&resp.stdout).to_string(),
        stderr: String::from_utf8_lossy(&resp.stderr).to_string(),
        elapsed: resp.elapsed_seconds,
        files,
    })
}

fn status_str(code: i32) -> String {
    match code {
        0 => "unspecified",
        1 => "completed",
        2 => "timed_out",
        3 => "signaled",
        4 => "skipped",
        _ => "unknown",
    }
    .to_string()
}
