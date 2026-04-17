use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::proto::{self, OutputChunk, RunCompleted, RunStatus};
use crate::validation;

pub struct ExecRequest {
    pub session_id: String,
    pub executable: PathBuf,
    pub file_root: String,
    pub run_dir: PathBuf,
    pub timeout: Duration,
}

pub struct BufferedResult {
    pub status: RunStatus,
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub elapsed: Duration,
    pub output_files: Vec<(String, PathBuf)>,
}

pub enum StreamEvent {
    Output(OutputChunk),
    Completed(RunCompleted, Vec<(String, PathBuf)>),
}

/// Populate `run_dir` with workspace files and inline inputs.
/// If `use_symlinks` is true, workspace files are symlinked (for Tier 2 Run
/// which executes in-place in the workspace — this parameter is unused there).
/// If false, workspace files are copied so the workspace is never modified
/// by the subprocess (required for RunSync temp dirs and pipeline steps).
pub async fn populate_run_dir(
    session_id: &str,
    run_dir: &Path,
    workspace: &Path,
    inputs: &[(String, Vec<u8>)],
) -> io::Result<()> {
    let mut workspace_copied = 0u32;
    let mut entries = tokio::fs::read_dir(workspace).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() && !file_type.is_symlink() {
            debug!(
                session_id = %session_id,
                path = %entry.path().display(),
                "skipping non-file workspace entry"
            );
            continue;
        }
        let fname = entry.file_name();
        let dest = run_dir.join(&fname);
        if !dest.exists() {
            tokio::fs::copy(entry.path(), &dest).await?;
            workspace_copied += 1;
        }
    }

    for (name, data) in inputs {
        validate_filename_for_io(name)?;
        let dest = run_dir.join(name);
        tokio::fs::write(&dest, data).await?;
    }

    let inline: Vec<(&str, usize)> = inputs.iter().map(|(n, d)| (n.as_str(), d.len())).collect();

    info!(
        session_id = %session_id,
        run_dir = %run_dir.display(),
        workspace = %workspace.display(),
        workspace_files_copied = workspace_copied,
        inline_input_count = inputs.len(),
        inline_inputs = ?inline,
        "run directory populated"
    );

    Ok(())
}

/// Snapshot directory: capture filename -> modification time.
pub async fn snapshot_dir(dir: &Path) -> io::Result<HashMap<String, SystemTime>> {
    let mut map = HashMap::new();
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(meta) = entry.metadata().await {
                if let Ok(mtime) = meta.modified() {
                    map.insert(name.to_string(), mtime);
                }
            }
        }
    }
    Ok(map)
}

/// Detect output files: new files or files whose mtime changed since the snapshot.
pub async fn detect_outputs(
    dir: &Path,
    before: &HashMap<String, SystemTime>,
) -> io::Result<Vec<(String, PathBuf)>> {
    let mut outputs = Vec::new();
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let meta = entry.metadata().await?;
        if !meta.is_file() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            let is_output = match (before.get(name), meta.modified()) {
                (None, _) => true,
                (Some(old_mtime), Ok(new_mtime)) => new_mtime > *old_mtime,
                _ => false,
            };
            if is_output {
                outputs.push((name.to_string(), entry.path()));
            }
        }
    }
    outputs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(outputs)
}

/// Run a model, buffering all stdout/stderr. Used by RunSync.
pub async fn run_buffered(req: ExecRequest) -> io::Result<BufferedResult> {
    let sid = req.session_id.as_str();
    info!(
        session_id = %sid,
        executable = %req.executable.display(),
        file_root = %req.file_root,
        run_dir = %req.run_dir.display(),
        timeout_secs = req.timeout.as_secs(),
        "spawning process (buffered)"
    );

    let before = snapshot_dir(&req.run_dir).await?;
    let start = Instant::now();

    let mut child = Command::new(&req.executable)
        .arg(&req.file_root)
        .current_dir(&req.run_dir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let mut stdout_handle = child.stdout.take().unwrap();
    let mut stderr_handle = child.stderr.take().unwrap();

    let stdout_fut = async {
        let mut buf = Vec::new();
        stdout_handle.read_to_end(&mut buf).await.ok();
        buf
    };
    let stderr_fut = async {
        let mut buf = Vec::new();
        stderr_handle.read_to_end(&mut buf).await.ok();
        buf
    };

    let timeout_result = tokio::time::timeout(req.timeout, async {
        let (stdout, stderr) = tokio::join!(stdout_fut, stderr_fut);
        let status = child.wait().await?;
        Ok::<_, io::Error>((status, stdout, stderr))
    })
    .await;

    let elapsed = start.elapsed();

    match timeout_result {
        Ok(Ok((exit_status, stdout, stderr))) => {
            let output_files = detect_outputs(&req.run_dir, &before).await?;
            let (status, exit_code) = status_from_exit(exit_status);
            let outputs_meta: Vec<(&str, u64)> = output_files
                .iter()
                .filter_map(|(n, p)| std::fs::metadata(p).ok().map(|m| (n.as_str(), m.len())))
                .collect();
            info!(
                session_id = %sid,
                status = ?status,
                exit_code,
                elapsed_secs = elapsed.as_secs_f64(),
                stdout_bytes = stdout.len(),
                stderr_bytes = stderr.len(),
                output_file_count = output_files.len(),
                output_files = ?outputs_meta,
                "process finished (buffered)"
            );
            Ok(BufferedResult {
                status,
                exit_code,
                stdout,
                stderr,
                elapsed,
                output_files,
            })
        }
        Ok(Err(e)) => {
            warn!(session_id = %sid, error = %e, "process wait/read failed (buffered)");
            Err(e)
        }
        Err(_) => {
            kill_with_grace(&mut child, sid).await;
            let output_files = detect_outputs(&req.run_dir, &before).await?;
            let outputs_meta: Vec<(&str, u64)> = output_files
                .iter()
                .filter_map(|(n, p)| std::fs::metadata(p).ok().map(|m| (n.as_str(), m.len())))
                .collect();
            warn!(
                session_id = %sid,
                status = ?RunStatus::TimedOut,
                exit_code = -1,
                elapsed_secs = elapsed.as_secs_f64(),
                output_file_count = output_files.len(),
                output_files = ?outputs_meta,
                "process timed out (buffered)"
            );
            Ok(BufferedResult {
                status: RunStatus::TimedOut,
                exit_code: -1,
                stdout: Vec::new(),
                stderr: Vec::new(),
                elapsed,
                output_files,
            })
        }
    }
}

/// Run a model, streaming stdout/stderr chunks over a channel. Used by Run and RunPipeline.
pub async fn run_streaming(req: ExecRequest, tx: mpsc::Sender<StreamEvent>) -> io::Result<()> {
    let sid = req.session_id.as_str();
    info!(
        session_id = %sid,
        executable = %req.executable.display(),
        file_root = %req.file_root,
        run_dir = %req.run_dir.display(),
        timeout_secs = req.timeout.as_secs(),
        "spawning process (streaming)"
    );

    let before = snapshot_dir(&req.run_dir).await?;
    let start = Instant::now();

    let mut child = Command::new(&req.executable)
        .arg(&req.file_root)
        .current_dir(&req.run_dir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();

    let mut stdout_buf = vec![0u8; 8192];
    let mut stderr_buf = vec![0u8; 8192];
    let mut stdout_done = false;
    let mut stderr_done = false;

    let deadline = tokio::time::Instant::now() + req.timeout;

    let (status, exit_code) = loop {
        if stdout_done && stderr_done {
            match tokio::time::timeout_at(deadline, child.wait()).await {
                Ok(Ok(exit)) => {
                    break status_from_exit(exit);
                }
                Ok(Err(_)) => {
                    break (RunStatus::Signaled, -1);
                }
                Err(_) => {
                    kill_with_grace(&mut child, sid).await;
                    break (RunStatus::TimedOut, -1);
                }
            }
        }

        tokio::select! {
            res = stdout.read(&mut stdout_buf), if !stdout_done => {
                match res {
                    Ok(0) => stdout_done = true,
                    Ok(n) => {
                        let _ = tx.send(StreamEvent::Output(OutputChunk {
                            stream: proto::OutputStream::Stdout.into(),
                            data: stdout_buf[..n].to_vec(),
                        })).await;
                    }
                    Err(_) => stdout_done = true,
                }
            }
            res = stderr.read(&mut stderr_buf), if !stderr_done => {
                match res {
                    Ok(0) => stderr_done = true,
                    Ok(n) => {
                        let _ = tx.send(StreamEvent::Output(OutputChunk {
                            stream: proto::OutputStream::Stderr.into(),
                            data: stderr_buf[..n].to_vec(),
                        })).await;
                    }
                    Err(_) => stderr_done = true,
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                kill_with_grace(&mut child, sid).await;
                break (RunStatus::TimedOut, -1);
            }
        }
    };

    let elapsed = start.elapsed();
    let output_files = detect_outputs(&req.run_dir, &before).await?;

    let outputs_meta: Vec<(&str, u64)> = output_files
        .iter()
        .filter_map(|(n, p)| std::fs::metadata(p).ok().map(|m| (n.as_str(), m.len())))
        .collect();

    info!(
        session_id = %sid,
        status = ?status,
        exit_code,
        elapsed_secs = elapsed.as_secs_f64(),
        output_file_count = output_files.len(),
        output_files = ?outputs_meta,
        "process finished (streaming)"
    );

    let file_infos: Vec<proto::FileInfo> = output_files
        .iter()
        .filter_map(|(name, path)| {
            std::fs::metadata(path).ok().map(|m| proto::FileInfo {
                name: name.clone(),
                size_bytes: m.len(),
            })
        })
        .collect();

    let _ = tx
        .send(StreamEvent::Completed(
            RunCompleted {
                status: status.into(),
                exit_code,
                elapsed_seconds: elapsed.as_secs_f64(),
                output_files: file_infos,
            },
            output_files,
        ))
        .await;

    Ok(())
}

fn validate_filename_for_io(name: &str) -> io::Result<()> {
    validation::validate_filename(name)
        .map_err(|status| validation::invalid_input(status.message().to_string()))
}

fn status_from_exit(exit_status: std::process::ExitStatus) -> (RunStatus, i32) {
    match exit_status.code() {
        Some(code) => (RunStatus::Completed, code),
        None => (RunStatus::Signaled, -1),
    }
}

async fn kill_with_grace(child: &mut tokio::process::Child, session_id: &str) {
    debug!(session_id = %session_id, "sending SIGTERM");
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }

    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(_) => {}
        Err(_) => {
            warn!(
                session_id = %session_id,
                "process did not exit after SIGTERM, sending SIGKILL"
            );
            let _ = child.kill().await;
        }
    }
}
