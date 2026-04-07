use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::executor::{self, ExecRequest, StreamEvent};
use crate::proto::{self, FileChunk};
use crate::AppState;

pub async fn run_pipeline(
    state: &Arc<AppState>,
    request: Request<proto::RunPipelineRequest>,
) -> Result<Response<ReceiverStream<Result<proto::PipelineOutput, Status>>>, Status> {
    let _write_guard = state
        .exec_lock
        .try_write()
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
        if step.id.is_empty() {
            return Err(Status::invalid_argument("step id must not be empty"));
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

    let overall_timeout = req
        .timeout_seconds
        .map(|t| Duration::from_secs(t as u64))
        .unwrap_or(Duration::from_secs(
            state.default_timeout * steps.len() as u64,
        ));

    let step_ids_out = step_ids.clone();
    let state = state.clone();
    let (out_tx, out_rx) = mpsc::channel(64);

    tokio::spawn(async move {
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
        let step_outputs: Arc<
            tokio::sync::Mutex<HashMap<String, Vec<(String, std::path::PathBuf)>>>,
        > = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

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

        // Find initially ready steps
        let mut ready: VecDeque<String> = steps
            .iter()
            .filter(|s| s.depends_on.is_empty())
            .map(|s| s.id.clone())
            .collect();

        let (done_tx, mut done_rx) =
            mpsc::channel::<(String, bool, Vec<(String, std::path::PathBuf)>)>(32);

        let mut active = 0usize;

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
                if let Err(e) = executor::populate_run_dir(&step_dir, &workspace, &inputs).await {
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
                                    tracing::warn!("copy dep output {fname}: {e}");
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

                active += 1;
                tokio::spawn(async move {
                    let (event_tx, mut event_rx) = mpsc::channel(32);
                    let exec_req = ExecRequest {
                        executable,
                        file_root: step.file_root,
                        run_dir: step_dir,
                        timeout: step_timeout,
                    };

                    let sid_inner = sid.clone();
                    tokio::spawn(async move {
                        if let Err(e) = executor::run_streaming(exec_req, event_tx).await {
                            tracing::error!("step {}: exec error: {e}", sid_inner);
                        }
                    });

                    let mut step_files: Vec<(String, std::path::PathBuf)> = Vec::new();
                    let mut success = false;

                    while let Some(event) = event_rx.recv().await {
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
                                    let data = match tokio::fs::read(path).await {
                                        Ok(d) => d,
                                        Err(_) => continue,
                                    };
                                    let mut offset = 0;
                                    let mut first = true;
                                    while offset < data.len() {
                                        let end = (offset + chunk_size).min(data.len());
                                        let _ = out_tx2
                                            .send(Ok(proto::PipelineOutput {
                                                payload: Some(
                                                    proto::pipeline_output::Payload::Step(
                                                        proto::StepEvent {
                                                            step_id: sid.clone(),
                                                            detail: Some(
                                                                proto::step_event::Detail::File(
                                                                    FileChunk {
                                                                        name: if first {
                                                                            name.clone()
                                                                        } else {
                                                                            String::new()
                                                                        },
                                                                        data: data[offset..end]
                                                                            .to_vec(),
                                                                    },
                                                                ),
                                                            ),
                                                        },
                                                    ),
                                                ),
                                            }))
                                            .await;
                                        first = false;
                                        offset = end;
                                    }
                                }

                                step_files = files;
                            }
                        }
                    }

                    let _ = done_tx2.send((sid, success, step_files)).await;
                });
            }

            // If no active steps and nothing ready, we're done
            if active == 0 {
                break;
            }

            // Check overall timeout
            if tokio::time::Instant::now() >= deadline {
                break;
            }

            // Wait for a step to complete
            tokio::select! {
                Some((sid, success, files)) = done_rx.recv() => {
                    active -= 1;
                    completed.insert(sid.clone());

                    if !success {
                        failed.insert(sid.clone());
                    }

                    // Store outputs for dependent steps
                    step_outputs.lock().await.insert(sid.clone(), files);

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
                    break;
                }
            }
        }

        let elapsed = start.elapsed();
        let all_succeeded = failed.is_empty() && skipped.is_empty();
        let skipped_list: Vec<String> = skipped.into_iter().collect();

        info!(
            elapsed = ?elapsed,
            all_succeeded,
            skipped = ?skipped_list,
            "pipeline completed"
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
