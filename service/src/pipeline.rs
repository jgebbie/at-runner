use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::executor::{self, ExecRequest, StreamEvent};
use crate::proto::{self, FileChunk};
use crate::session::new_session_id;
use crate::validation::{validate_file_root, validate_filename, validate_step_id};
use crate::AppState;

type StepFiles = Vec<(String, std::path::PathBuf)>;
type StepOutputs = Arc<tokio::sync::Mutex<HashMap<String, StepFiles>>>;

struct StepDone {
    id: String,
    success: bool,
    files: StepFiles,
}

pub async fn run_pipeline(
    state: &Arc<AppState>,
    request: Request<proto::RunPipelineRequest>,
) -> Result<Response<ReceiverStream<Result<proto::PipelineOutput, Status>>>, Status> {
    let _write_guard = state
        .exec_lock
        .clone()
        .try_write_owned()
        .map_err(|_| Status::failed_precondition("another Run or RunPipeline is active"))?;

    let req = request.into_inner();
    let steps = req.steps;

    if steps.is_empty() {
        return Err(Status::invalid_argument(
            "pipeline must have at least one step",
        ));
    }

    // Validate: unique IDs
    let mut id_set = HashSet::new();
    for step in &steps {
        validate_step_id(&step.id)?;
        validate_file_root(&step.file_root)?;
        for input in &step.inputs {
            validate_filename(&input.name)?;
        }
        if !id_set.insert(step.id.clone()) {
            return Err(Status::invalid_argument(format!(
                "duplicate step id: {:?}",
                step.id
            )));
        }
    }

    // Validate: model names
    for step in &steps {
        if !state.allowlist.contains(&step.model) {
            return Err(Status::invalid_argument(format!(
                "unknown model: {:?}",
                step.model
            )));
        }
    }

    // Validate: dependency references
    for step in &steps {
        for dep in &step.depends_on {
            if !id_set.contains(dep) {
                return Err(Status::invalid_argument(format!(
                    "step {:?} depends on nonexistent step {:?}",
                    step.id, dep
                )));
            }
        }
    }

    // Validate: no cycles (topological sort)
    let step_ids: Vec<String> = steps.iter().map(|s| s.id.clone()).collect();
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for id in &step_ids {
        adj.entry(id.clone()).or_default();
        in_degree.entry(id.clone()).or_insert(0);
    }
    for step in &steps {
        for dep in &step.depends_on {
            adj.entry(dep.clone()).or_default().push(step.id.clone());
            *in_degree.entry(step.id.clone()).or_default() += 1;
        }
    }

    let queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(id, _)| id.clone())
        .collect();
    let mut sorted_count = 0;
    let mut topo_queue = queue.clone();
    while let Some(node) = topo_queue.pop_front() {
        sorted_count += 1;
        if let Some(neighbors) = adj.get(&node) {
            for n in neighbors {
                let deg = in_degree.get_mut(n).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    topo_queue.push_back(n.clone());
                }
            }
        }
    }
    if sorted_count != step_ids.len() {
        return Err(Status::invalid_argument("cycle in dependency graph"));
    }

    let session_id = new_session_id();

    let overall_timeout = req
        .timeout_seconds
        .map(|t| Duration::from_secs(t as u64))
        .unwrap_or(Duration::from_secs(
            state.default_timeout * steps.len() as u64,
        ));

    let step_ids_out = step_ids.clone();
    let state = state.clone();
    let (out_tx, out_rx) = mpsc::channel(64);

    info!(
        session_id = %session_id,
        step_count = steps.len(),
        step_ids = ?step_ids,
        overall_timeout_secs = overall_timeout.as_secs(),
        "RunPipeline started"
    );

    let pipeline_sid: Arc<String> = Arc::new(session_id);
    tokio::spawn(async move {
        let _guard = _write_guard;
        let start = Instant::now();
        let deadline = tokio::time::Instant::now() + overall_timeout;

        let send = |msg: proto::PipelineOutput| {
            let tx = out_tx.clone();
            async move {
                let _ = tx.send(Ok(msg)).await;
            }
        };

        // PipelineStarted
        send(proto::PipelineOutput {
            payload: Some(proto::pipeline_output::Payload::PipelineStarted(
                proto::PipelineStarted {
                    step_ids: step_ids_out,
                },
            )),
        })
        .await;

        // Build step map
        let mut step_map: HashMap<String, proto::PipelineStep> = HashMap::new();
        let mut dep_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut rev_dep: HashMap<String, Vec<String>> = HashMap::new();
        let mut remaining_deps: HashMap<String, usize> = HashMap::new();

        for step in &steps {
            step_map.insert(step.id.clone(), step.clone());
            dep_map.insert(step.id.clone(), step.depends_on.clone());
            remaining_deps.insert(step.id.clone(), step.depends_on.len());
            for dep in &step.depends_on {
                rev_dep
                    .entry(dep.clone())
                    .or_default()
                    .push(step.id.clone());
            }
        }

        // Track outputs per step (step_id -> [(filename, path)])
        let step_outputs: StepOutputs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        let mut failed: HashSet<String> = HashSet::new();
        let mut skipped: HashSet<String> = HashSet::new();
        let mut completed: HashSet<String> = HashSet::new();

        // Create temp base dir
        let tmp_base = match tempfile::tempdir() {
            Ok(t) => t,
            Err(e) => {
                let _ = out_tx
                    .send(Err(Status::internal(format!("tmpdir: {e}"))))
                    .await;
                return;
            }
        };

        info!(
            session_id = pipeline_sid.as_str(),
            temp_dir = %tmp_base.path().display(),
            "pipeline workspace (temp) created"
        );

        // Find initially ready steps
        let mut ready: VecDeque<String> = steps
            .iter()
            .filter(|s| s.depends_on.is_empty())
            .map(|s| s.id.clone())
            .collect();

        let (done_tx, mut done_rx) = mpsc::channel::<StepDone>(32);

        let mut active_steps: HashSet<String> = HashSet::new();
        let mut cancel_steps: HashMap<String, oneshot::Sender<()>> = HashMap::new();
        let mut overall_timed_out = false;

        loop {
            // Launch ready steps
            while let Some(step_id) = ready.pop_front() {
                // Check if any dependency failed -> skip
                let deps = dep_map.get(&step_id).cloned().unwrap_or_default();
                let should_skip = deps
                    .iter()
                    .any(|d| failed.contains(d) || skipped.contains(d));

                if should_skip {
                    skipped.insert(step_id.clone());
                    send(proto::PipelineOutput {
                        payload: Some(proto::pipeline_output::Payload::Step(proto::StepEvent {
                            step_id: step_id.clone(),
                            detail: Some(proto::step_event::Detail::Completed(
                                proto::RunCompleted {
                                    status: proto::RunStatus::Skipped.into(),
                                    exit_code: -1,
                                    elapsed_seconds: 0.0,
                                    output_files: vec![],
                                },
                            )),
                        })),
                    })
                    .await;

                    // Propagate to dependents
                    if let Some(dependents) = rev_dep.get(&step_id) {
                        for dep_id in dependents {
                            let rem = remaining_deps.get_mut(dep_id).unwrap();
                            *rem -= 1;
                            if *rem == 0 {
                                ready.push_back(dep_id.clone());
                            }
                        }
                    }
                    continue;
                }

                let step = step_map.get(&step_id).unwrap().clone();
                let step_dir = tmp_base.path().join(&step_id);
                if let Err(e) = std::fs::create_dir_all(&step_dir) {
                    let _ = out_tx
                        .send(Err(Status::internal(format!("mkdir: {e}"))))
                        .await;
                    return;
                }

                // Populate: workspace symlinks
                let inputs: Vec<(String, Vec<u8>)> = step
                    .inputs
                    .iter()
                    .map(|f| (f.name.clone(), f.content.clone()))
                    .collect();

                let workspace = state.workspace.clone();
                if let Err(e) = executor::populate_run_dir(
                    pipeline_sid.as_str(),
                    &step_dir,
                    &workspace,
                    &inputs,
                )
                .await
                {
                    let _ = out_tx
                        .send(Err(Status::internal(format!("populate: {e}"))))
                        .await;
                    return;
                }

                // Copy outputs from dependency steps into this step's directory
                {
                    let outputs_lock = step_outputs.lock().await;
                    for dep_id in &deps {
                        if let Some(dep_outputs) = outputs_lock.get(dep_id) {
                            for (fname, src_path) in dep_outputs {
                                let dest = step_dir.join(fname);
                                if dest.exists() {
                                    let _ = tokio::fs::remove_file(&dest).await;
                                }
                                if let Err(e) = tokio::fs::copy(src_path, &dest).await {
                                    tracing::warn!(
                                        session_id = pipeline_sid.as_str(),
                                        step_id = %step_id,
                                        file = %fname,
                                        error = %e,
                                        "copy dependency output failed"
                                    );
                                }
                            }
                        }
                    }
                }

                let executable = state.bin_dir.join(format!("{}.exe", step.model));
                let step_timeout = step
                    .timeout_seconds
                    .map(|t| Duration::from_secs(t as u64))
                    .unwrap_or(Duration::from_secs(state.default_timeout));

                let out_tx2 = out_tx.clone();
                let done_tx2 = done_tx.clone();
                let sid = step_id.clone();
                let chunk_size = state.chunk_size;
                let exec_session_id = format!("{}:step:{}", pipeline_sid.as_str(), step_id);

                info!(
                    session_id = pipeline_sid.as_str(),
                    step_id = %step_id,
                    model = %step.model,
                    file_root = %step.file_root,
                    step_timeout_secs = step_timeout.as_secs(),
                    step_dir = %step_dir.display(),
                    "pipeline step executing"
                );

                // Send StepEvent::Started
                send(proto::PipelineOutput {
                    payload: Some(proto::pipeline_output::Payload::Step(proto::StepEvent {
                        step_id: step_id.clone(),
                        detail: Some(proto::step_event::Detail::Started(proto::RunStarted {
                            model: step.model.clone(),
                            file_root: step.file_root.clone(),
                        })),
                    })),
                })
                .await;

                let (cancel_tx, cancel_rx) = oneshot::channel();
                active_steps.insert(step_id.clone());
                cancel_steps.insert(step_id.clone(), cancel_tx);
                let pipeline_sid_task = pipeline_sid.clone();
                tokio::spawn(async move {
                    let (event_tx, mut event_rx) = mpsc::channel(32);
                    let exec_req = ExecRequest {
                        session_id: exec_session_id.clone(),
                        executable,
                        file_root: step.file_root,
                        run_dir: step_dir,
                        timeout: step_timeout,
                    };

                    let sid_inner = sid.clone();
                    let esid = exec_session_id.clone();
                    let mut exec_handle = Some(tokio::spawn(async move {
                        if let Err(e) = executor::run_streaming(exec_req, event_tx).await {
                            tracing::error!(
                                session_id = %esid,
                                step_id = %sid_inner,
                                error = %e,
                                "pipeline step exec error"
                            );
                        }
                    }));

                    let mut step_files: StepFiles = Vec::new();
                    let mut success = false;
                    let mut completed_sent = false;
                    let mut cancel_rx = cancel_rx;

                    let cancelled = loop {
                        tokio::select! {
                            _ = &mut cancel_rx => {
                                if completed_sent {
                                    break false;
                                }
                                if let Some(handle) = exec_handle.take() {
                                    handle.abort();
                                    let _ = handle.await;
                                }
                                let completed = proto::RunCompleted {
                                    status: proto::RunStatus::TimedOut.into(),
                                    exit_code: -1,
                                    elapsed_seconds: 0.0,
                                    output_files: vec![],
                                };
                                let _ = out_tx2
                                    .send(Ok(proto::PipelineOutput {
                                        payload: Some(proto::pipeline_output::Payload::Step(
                                            proto::StepEvent {
                                                step_id: sid.clone(),
                                                detail: Some(proto::step_event::Detail::Completed(
                                                    completed,
                                                )),
                                            },
                                        )),
                                    }))
                                    .await;
                                break true;
                            }
                            event = event_rx.recv() => {
                                let Some(event) = event else {
                                    break false;
                                };
                                match event {
                                    StreamEvent::Output(chunk) => {
                                        let _ = out_tx2
                                            .send(Ok(proto::PipelineOutput {
                                                payload: Some(proto::pipeline_output::Payload::Step(
                                                    proto::StepEvent {
                                                        step_id: sid.clone(),
                                                        detail: Some(proto::step_event::Detail::Output(
                                                            chunk,
                                                        )),
                                                    },
                                                )),
                                            }))
                                            .await;
                                    }
                                    StreamEvent::Completed(completed, files) => {
                                        success = completed.status
                                            == i32::from(proto::RunStatus::Completed)
                                            && completed.exit_code == 0;
                                        completed_sent = true;

                                        let file_meta: Vec<(&str, u64)> = files
                                            .iter()
                                            .filter_map(|(n, p)| {
                                                std::fs::metadata(p).ok().map(|m| (n.as_str(), m.len()))
                                            })
                                            .collect();

                                        tracing::info!(
                                            session_id = pipeline_sid_task.as_str(),
                                            step_id = %sid,
                                            status = completed.status,
                                            exit_code = completed.exit_code,
                                            elapsed_secs = completed.elapsed_seconds,
                                            output_file_count = files.len(),
                                            output_files = ?file_meta,
                                            success,
                                            "pipeline step completed"
                                        );

                                        let _ = out_tx2
                                            .send(Ok(proto::PipelineOutput {
                                                payload: Some(proto::pipeline_output::Payload::Step(
                                                    proto::StepEvent {
                                                        step_id: sid.clone(),
                                                        detail: Some(proto::step_event::Detail::Completed(
                                                            completed,
                                                        )),
                                                    },
                                                )),
                                            }))
                                            .await;

                                        // Stream file chunks
                                        for (name, path) in &files {
                                            let mut file = match tokio::fs::File::open(path).await {
                                                Ok(f) => f,
                                                Err(e) => {
                                                    tracing::warn!(
                                                        session_id = pipeline_sid_task.as_str(),
                                                        step_id = %sid,
                                                        file = %name,
                                                        error = %e,
                                                        "skip step output file (open failed)"
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
                                                            let _ = out_tx2.send(Ok(proto::PipelineOutput {
                                                                payload: Some(proto::pipeline_output::Payload::Step(
                                                                    proto::StepEvent {
                                                                        step_id: sid.clone(),
                                                                        detail: Some(proto::step_event::Detail::File(
                                                                            FileChunk {
                                                                                name: name.clone(),
                                                                                data: Vec::new(),
                                                                            }
                                                                        )),
                                                                    }
                                                                ))
                                                            })).await;
                                                        }
                                                        break;
                                                    }
                                                    Ok(n) => {
                                                        let _ = out_tx2.send(Ok(proto::PipelineOutput {
                                                            payload: Some(proto::pipeline_output::Payload::Step(
                                                                proto::StepEvent {
                                                                    step_id: sid.clone(),
                                                                    detail: Some(proto::step_event::Detail::File(
                                                                        FileChunk {
                                                                            name: if first { name.clone() } else { String::new() },
                                                                            data: buf[..n].to_vec(),
                                                                        }
                                                                    )),
                                                                }
                                                            ))
                                                        })).await;
                                                        first = false;
                                                    }
                                                    Err(e) => {
                                                        tracing::warn!(
                                                            session_id = pipeline_sid_task.as_str(),
                                                            step_id = %sid,
                                                            file = %name,
                                                            error = %e,
                                                            "error reading output file"
                                                        );
                                                        break;
                                                    }
                                                }
                                            }
                                            tracing::info!(
                                                session_id = pipeline_sid_task.as_str(),
                                                step_id = %sid,
                                                file = %name,
                                                bytes = len,
                                                chunks = num_chunks,
                                                chunk_size,
                                                "streamed pipeline step file to client"
                                            );
                                        }

                                        step_files = files;
                                    }
                                }
                            }
                        }
                    };

                    if !cancelled {
                        if let Some(handle) = exec_handle.take() {
                            let _ = handle.await;
                        }
                    }

                    let _ = done_tx2
                        .send(StepDone {
                            id: sid,
                            success,
                            files: step_files,
                        })
                        .await;
                });
            }

            // If no active steps and nothing ready, we're done
            if active_steps.is_empty() {
                break;
            }

            // Check overall timeout
            if tokio::time::Instant::now() >= deadline {
                overall_timed_out = true;
                break;
            }

            // Wait for a step to complete
            tokio::select! {
                Some(done) = done_rx.recv() => {
                    let sid = done.id;
                    active_steps.remove(&sid);
                    cancel_steps.remove(&sid);
                    completed.insert(sid.clone());

                    if !done.success {
                        failed.insert(sid.clone());
                    }

                    // Store outputs for dependent steps
                    step_outputs.lock().await.insert(sid.clone(), done.files);

                    // Unblock dependents
                    if let Some(dependents) = rev_dep.get(&sid) {
                        for dep_id in dependents {
                            if completed.contains(dep_id) || skipped.contains(dep_id) {
                                continue;
                            }
                            let rem = remaining_deps.get_mut(dep_id).unwrap();
                            *rem -= 1;
                            if *rem == 0 {
                                ready.push_back(dep_id.clone());
                            }
                        }
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    overall_timed_out = true;
                    break;
                }
            }
        }

        if overall_timed_out {
            for (_, cancel_tx) in cancel_steps.drain() {
                let _ = cancel_tx.send(());
            }

            while !active_steps.is_empty() {
                let Some(done) = done_rx.recv().await else {
                    break;
                };
                let sid = done.id;
                active_steps.remove(&sid);
                completed.insert(sid.clone());
                failed.insert(sid.clone());
                step_outputs.lock().await.insert(sid, done.files);
            }

            let unfinished: Vec<String> = step_ids
                .iter()
                .filter(|id| {
                    !completed.contains(*id) && !failed.contains(*id) && !skipped.contains(*id)
                })
                .cloned()
                .collect();

            for step_id in unfinished {
                failed.insert(step_id.clone());
                send(proto::PipelineOutput {
                    payload: Some(proto::pipeline_output::Payload::Step(proto::StepEvent {
                        step_id,
                        detail: Some(proto::step_event::Detail::Completed(proto::RunCompleted {
                            status: proto::RunStatus::TimedOut.into(),
                            exit_code: -1,
                            elapsed_seconds: 0.0,
                            output_files: vec![],
                        })),
                    })),
                })
                .await;
            }
        }

        let elapsed = start.elapsed();
        let all_succeeded =
            failed.is_empty() && skipped.is_empty() && completed.len() == step_ids.len();
        let skipped_list: Vec<String> = skipped.into_iter().collect();

        info!(
            session_id = pipeline_sid.as_str(),
            elapsed_secs = elapsed.as_secs_f64(),
            all_succeeded,
            failed_steps = ?failed.iter().cloned().collect::<Vec<_>>(),
            skipped = ?skipped_list,
            completed_steps = completed.len(),
            "RunPipeline completed"
        );

        send(proto::PipelineOutput {
            payload: Some(proto::pipeline_output::Payload::PipelineCompleted(
                proto::PipelineCompleted {
                    all_succeeded,
                    elapsed_seconds: elapsed.as_secs_f64(),
                    skipped_steps: skipped_list,
                },
            )),
        })
        .await;
    });

    Ok(Response::new(ReceiverStream::new(out_rx)))
}
