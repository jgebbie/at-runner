use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::executor::{self, ExecRequest, StreamEvent};
use crate::files::stream_file_chunks;
use crate::proto;
use crate::session::new_session_id;
use crate::validation::{validate_file_root, validate_filename, validate_step_id};
use crate::AppState;

type PipelineStream = ReceiverStream<Result<proto::PipelineOutput, Status>>;
type PipelineSender = mpsc::Sender<Result<proto::PipelineOutput, Status>>;
type StepFiles = Vec<(String, PathBuf)>;
type StepOutputs = Arc<tokio::sync::Mutex<HashMap<String, StepFiles>>>;

struct StepDone {
    id: String,
    success: bool,
    files: StepFiles,
}

// Immutable view of the requested DAG. Keeping this separate from SchedulerState
// makes it clear which data describes the pipeline and which data changes as it runs.
struct DependencyGraph {
    step_ids: Vec<String>,
    step_map: HashMap<String, proto::PipelineStep>,
    dep_map: HashMap<String, Vec<String>>,
    rev_dep: HashMap<String, Vec<String>>,
    remaining_deps: HashMap<String, usize>,
}

impl DependencyGraph {
    fn initial_ready(&self) -> VecDeque<String> {
        self.step_ids
            .iter()
            .filter(|id| self.remaining_deps.get(*id) == Some(&0))
            .cloned()
            .collect()
    }
}

// Mutable runtime state for the scheduler loop. Most fields are sets so each
// helper can ask simple questions: ready, active, completed, failed, or skipped.
struct SchedulerState {
    ready: VecDeque<String>,
    active_steps: HashSet<String>,
    cancel_steps: HashMap<String, oneshot::Sender<()>>,
    failed: HashSet<String>,
    skipped: HashSet<String>,
    completed: HashSet<String>,
    remaining_deps: HashMap<String, usize>,
}

impl SchedulerState {
    fn new(graph: &DependencyGraph) -> Self {
        Self {
            ready: graph.initial_ready(),
            active_steps: HashSet::new(),
            cancel_steps: HashMap::new(),
            failed: HashSet::new(),
            skipped: HashSet::new(),
            completed: HashSet::new(),
            remaining_deps: graph.remaining_deps.clone(),
        }
    }

    fn should_skip(&self, graph: &DependencyGraph, step_id: &str) -> bool {
        graph
            .dep_map
            .get(step_id)
            .into_iter()
            .flatten()
            .any(|dep| self.failed.contains(dep) || self.skipped.contains(dep))
    }

    fn unblock_dependents(&mut self, graph: &DependencyGraph, step_id: &str) {
        let Some(dependents) = graph.rev_dep.get(step_id) else {
            return;
        };

        for dep_id in dependents {
            if self.completed.contains(dep_id) || self.skipped.contains(dep_id) {
                continue;
            }

            let remaining = self.remaining_deps.get_mut(dep_id).unwrap();
            *remaining -= 1;
            if *remaining == 0 {
                self.ready.push_back(dep_id.clone());
            }
        }
    }

    fn all_succeeded(&self, total_steps: usize) -> bool {
        self.failed.is_empty() && self.skipped.is_empty() && self.completed.len() == total_steps
    }
}

// Owned state moved into the background pipeline task. The RPC returns a stream
// immediately, so the real work must own everything it needs after this point.
struct PipelineRun {
    state: Arc<AppState>,
    graph: DependencyGraph,
    session_id: String,
    overall_timeout: Duration,
    out_tx: PipelineSender,
}

// Per-step owned state moved into a spawned task. This avoids borrowing from the
// scheduler while the executable runs and lets cancellation use a simple channel.
struct StepTask {
    pipeline_id: Arc<String>,
    step_id: String,
    executable: PathBuf,
    file_root: String,
    step_dir: PathBuf,
    timeout: Duration,
    chunk_size: usize,
    out_tx: PipelineSender,
    done_tx: mpsc::Sender<StepDone>,
    cancel_rx: oneshot::Receiver<()>,
}

struct StepOutcome {
    files: StepFiles,
    success: bool,
    completed_sent: bool,
}

impl StepOutcome {
    fn new() -> Self {
        Self {
            files: Vec::new(),
            success: false,
            completed_sent: false,
        }
    }
}

pub async fn run_pipeline(
    state: &Arc<AppState>,
    request: Request<proto::RunPipelineRequest>,
) -> Result<Response<PipelineStream>, Status> {
    // Pipelines execute multiple steps and share intermediate artifacts; we enforce
    // the same "single active run" rule as Tier-2 `Run` to keep workspace state stable.
    let write_guard = state
        .exec_lock
        .clone()
        .try_write_owned()
        .map_err(|_| Status::failed_precondition("another Run or RunPipeline is active"))?;

    let req = request.into_inner();
    let graph = validate_pipeline(state, req.steps)?;
    let session_id = new_session_id();
    let overall_timeout = pipeline_timeout(state, req.timeout_seconds, graph.step_ids.len());

    let (out_tx, out_rx) = mpsc::channel(64);

    info!(
        session_id = %session_id,
        step_count = graph.step_ids.len(),
        step_ids = ?graph.step_ids,
        overall_timeout_secs = overall_timeout.as_secs(),
        "RunPipeline started"
    );

    let run = PipelineRun {
        state: state.clone(),
        graph,
        session_id,
        overall_timeout,
        out_tx,
    };

    tokio::spawn(async move {
        let _guard = write_guard;
        run_pipeline_task(run).await;
    });

    Ok(Response::new(ReceiverStream::new(out_rx)))
}

fn validate_pipeline(
    state: &Arc<AppState>,
    steps: Vec<proto::PipelineStep>,
) -> Result<DependencyGraph, Status> {
    if steps.is_empty() {
        return Err(Status::invalid_argument(
            "pipeline must have at least one step",
        ));
    }

    let mut id_set = HashSet::new();
    for step in &steps {
        validate_step_id(&step.id)?;
        validate_file_root(&step.file_root)?;
        state.resolve_executable(&step.model)?;

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

    let graph = build_dependency_graph(steps);
    ensure_acyclic(&graph)?;
    Ok(graph)
}

fn build_dependency_graph(steps: Vec<proto::PipelineStep>) -> DependencyGraph {
    let mut step_ids = Vec::with_capacity(steps.len());
    let mut step_map = HashMap::with_capacity(steps.len());
    let mut dep_map = HashMap::with_capacity(steps.len());
    let mut rev_dep: HashMap<String, Vec<String>> = HashMap::new();
    let mut remaining_deps = HashMap::with_capacity(steps.len());

    for step in steps {
        step_ids.push(step.id.clone());
        dep_map.insert(step.id.clone(), step.depends_on.clone());
        remaining_deps.insert(step.id.clone(), step.depends_on.len());

        for dep in &step.depends_on {
            rev_dep
                .entry(dep.clone())
                .or_default()
                .push(step.id.clone());
        }

        step_map.insert(step.id.clone(), step);
    }

    DependencyGraph {
        step_ids,
        step_map,
        dep_map,
        rev_dep,
        remaining_deps,
    }
}

fn ensure_acyclic(graph: &DependencyGraph) -> Result<(), Status> {
    // Kahn's algorithm is only used for validation. The runtime scheduler below
    // still picks steps dynamically as their dependencies finish.
    let mut remaining_deps = graph.remaining_deps.clone();
    let mut ready = graph.initial_ready();
    let mut sorted_count = 0;

    while let Some(step_id) = ready.pop_front() {
        sorted_count += 1;

        if let Some(dependents) = graph.rev_dep.get(&step_id) {
            for dep_id in dependents {
                let remaining = remaining_deps.get_mut(dep_id).unwrap();
                *remaining -= 1;
                if *remaining == 0 {
                    ready.push_back(dep_id.clone());
                }
            }
        }
    }

    if sorted_count != graph.step_ids.len() {
        return Err(Status::invalid_argument("cycle in dependency graph"));
    }

    Ok(())
}

fn pipeline_timeout(state: &AppState, requested: Option<u32>, step_count: usize) -> Duration {
    requested
        .map(|seconds| Duration::from_secs(seconds as u64))
        .unwrap_or(Duration::from_secs(
            state.default_timeout * step_count as u64,
        ))
}

// Each turn of this loop does three things: launch newly ready steps, wait for
// one active step to finish, and stop everything if the pipeline deadline hits.
async fn run_pipeline_task(run: PipelineRun) {
    let start = Instant::now();
    let deadline = tokio::time::Instant::now() + run.overall_timeout;
    let pipeline_id = Arc::new(run.session_id.clone());
    let step_outputs: StepOutputs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let mut scheduler = SchedulerState::new(&run.graph);
    let (done_tx, mut done_rx) = mpsc::channel::<StepDone>(32);
    let mut overall_timed_out = false;

    send_output(
        &run.out_tx,
        proto::PipelineOutput {
            payload: Some(proto::pipeline_output::Payload::PipelineStarted(
                proto::PipelineStarted {
                    step_ids: run.graph.step_ids.clone(),
                },
            )),
        },
    )
    .await;

    let tmp_base = match tempfile::tempdir() {
        Ok(tmp_base) => tmp_base,
        Err(e) => {
            send_error(&run.out_tx, Status::internal(format!("tmpdir: {e}"))).await;
            return;
        }
    };

    info!(
        session_id = pipeline_id.as_str(),
        temp_dir = %tmp_base.path().display(),
        "pipeline workspace (temp) created"
    );

    loop {
        while let Some(step_id) = scheduler.ready.pop_front() {
            if scheduler.should_skip(&run.graph, &step_id) {
                skip_step(&mut scheduler, &run.graph, &run.out_tx, step_id).await;
                continue;
            }

            if let Err(status) = launch_step(
                &run,
                &mut scheduler,
                &step_outputs,
                tmp_base.path(),
                &pipeline_id,
                &done_tx,
                step_id,
            )
            .await
            {
                send_error(&run.out_tx, status).await;
                return;
            }
        }

        if scheduler.active_steps.is_empty() {
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            overall_timed_out = true;
            break;
        }

        tokio::select! {
            Some(done) = done_rx.recv() => {
                record_step_done(&mut scheduler, &run.graph, &step_outputs, done).await;
            }
            _ = tokio::time::sleep_until(deadline) => {
                overall_timed_out = true;
                break;
            }
        }
    }

    if overall_timed_out {
        cancel_unfinished_steps(
            &mut scheduler,
            &run.graph,
            &run.out_tx,
            &step_outputs,
            &mut done_rx,
        )
        .await;
    }

    finish_pipeline(&run, &scheduler, start.elapsed()).await;
}

async fn skip_step(
    scheduler: &mut SchedulerState,
    graph: &DependencyGraph,
    out_tx: &PipelineSender,
    step_id: String,
) {
    scheduler.skipped.insert(step_id.clone());
    send_step_event(
        out_tx,
        &step_id,
        proto::step_event::Detail::Completed(status_completion(proto::RunStatus::Skipped)),
    )
    .await;
    scheduler.unblock_dependents(graph, &step_id);
}

async fn launch_step(
    run: &PipelineRun,
    scheduler: &mut SchedulerState,
    step_outputs: &StepOutputs,
    tmp_base: &Path,
    pipeline_id: &Arc<String>,
    done_tx: &mpsc::Sender<StepDone>,
    step_id: String,
) -> Result<(), Status> {
    let step = run.graph.step_map.get(&step_id).unwrap().clone();
    let deps = run.graph.dep_map.get(&step_id).cloned().unwrap_or_default();
    let step_dir = tmp_base.join(&step_id);

    tokio::fs::create_dir_all(&step_dir)
        .await
        .map_err(|e| Status::internal(format!("mkdir: {e}")))?;

    let inputs: Vec<(String, Vec<u8>)> = step
        .inputs
        .iter()
        .map(|file| (file.name.clone(), file.content.clone()))
        .collect();

    executor::populate_run_dir(
        pipeline_id.as_str(),
        &step_dir,
        &run.state.workspace,
        &inputs,
    )
    .await
    .map_err(|e| Status::internal(format!("populate: {e}")))?;

    materialize_dependency_outputs(
        pipeline_id.as_str(),
        &step_id,
        &deps,
        &step_dir,
        step_outputs,
    )
    .await;

    let executable = run.state.resolve_executable(&step.model)?;
    let step_timeout = run.state.timeout_or_default(step.timeout_seconds);

    info!(
        session_id = pipeline_id.as_str(),
        step_id = %step_id,
        model = %step.model,
        file_root = %step.file_root,
        step_timeout_secs = step_timeout.as_secs(),
        step_dir = %step_dir.display(),
        "pipeline step executing"
    );

    send_step_event(
        &run.out_tx,
        &step_id,
        proto::step_event::Detail::Started(proto::RunStarted {
            model: step.model,
            file_root: step.file_root.clone(),
        }),
    )
    .await;

    let (cancel_tx, cancel_rx) = oneshot::channel();
    scheduler.active_steps.insert(step_id.clone());
    scheduler.cancel_steps.insert(step_id.clone(), cancel_tx);

    let task = StepTask {
        pipeline_id: pipeline_id.clone(),
        step_id,
        executable,
        file_root: step.file_root,
        step_dir,
        timeout: step_timeout,
        chunk_size: run.state.chunk_size,
        out_tx: run.out_tx.clone(),
        done_tx: done_tx.clone(),
        cancel_rx,
    };

    tokio::spawn(run_step_task(task));
    Ok(())
}

async fn materialize_dependency_outputs(
    pipeline_id: &str,
    step_id: &str,
    deps: &[String],
    step_dir: &Path,
    step_outputs: &StepOutputs,
) {
    // Downstream steps receive upstream outputs as regular local files. That
    // keeps model executables simple: they only need to read their current dir.
    let outputs_lock = step_outputs.lock().await;

    for dep_id in deps {
        let Some(dep_outputs) = outputs_lock.get(dep_id) else {
            continue;
        };

        for (fname, src_path) in dep_outputs {
            let dest = step_dir.join(fname);
            if dest.exists() {
                let _ = tokio::fs::remove_file(&dest).await;
            }
            if let Err(e) = tokio::fs::copy(src_path, &dest).await {
                warn!(
                    session_id = pipeline_id,
                    step_id = %step_id,
                    file = %fname,
                    error = %e,
                    "copy dependency output failed"
                );
            }
        }
    }
}

async fn run_step_task(mut task: StepTask) {
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let exec_session_id = format!("{}:step:{}", task.pipeline_id.as_str(), task.step_id);
    let exec_req = ExecRequest {
        session_id: exec_session_id.clone(),
        executable: task.executable.clone(),
        file_root: task.file_root.clone(),
        run_dir: task.step_dir.clone(),
        timeout: task.timeout,
    };

    let step_id_for_log = task.step_id.clone();
    let mut exec_handle = Some(tokio::spawn(async move {
        if let Err(e) = executor::run_streaming(exec_req, event_tx).await {
            tracing::error!(
                session_id = %exec_session_id,
                step_id = %step_id_for_log,
                error = %e,
                "pipeline step exec error"
            );
        }
    }));

    let mut outcome = StepOutcome::new();
    let cancelled = loop {
        tokio::select! {
            _ = &mut task.cancel_rx => {
                if outcome.completed_sent {
                    break false;
                }
                abort_step(&mut exec_handle).await;
                send_step_event(
                    &task.out_tx,
                    &task.step_id,
                    proto::step_event::Detail::Completed(status_completion(proto::RunStatus::TimedOut)),
                )
                .await;
                break true;
            }
            event = event_rx.recv() => {
                let Some(event) = event else {
                    break false;
                };
                handle_step_event(&task, &mut outcome, event).await;
            }
        }
    };

    if !cancelled {
        if let Some(handle) = exec_handle.take() {
            let _ = handle.await;
        }
    }

    let _ = task
        .done_tx
        .send(StepDone {
            id: task.step_id,
            success: outcome.success,
            files: outcome.files,
        })
        .await;
}

async fn abort_step(exec_handle: &mut Option<tokio::task::JoinHandle<()>>) {
    if let Some(handle) = exec_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
}

async fn handle_step_event(task: &StepTask, outcome: &mut StepOutcome, event: StreamEvent) {
    match event {
        StreamEvent::Output(chunk) => {
            send_step_event(
                &task.out_tx,
                &task.step_id,
                proto::step_event::Detail::Output(chunk),
            )
            .await;
        }
        StreamEvent::Completed(completed, files) => {
            outcome.success = completed.status == i32::from(proto::RunStatus::Completed)
                && completed.exit_code == 0;
            outcome.completed_sent = true;

            log_step_completed(task, &completed, &files, outcome.success);

            send_step_event(
                &task.out_tx,
                &task.step_id,
                proto::step_event::Detail::Completed(completed),
            )
            .await;

            stream_step_files(task, &files).await;
            outcome.files = files;
        }
    }
}

fn log_step_completed(
    task: &StepTask,
    completed: &proto::RunCompleted,
    files: &StepFiles,
    success: bool,
) {
    let file_meta: Vec<(&str, u64)> = files
        .iter()
        .filter_map(|(name, path)| {
            std::fs::metadata(path)
                .ok()
                .map(|m| (name.as_str(), m.len()))
        })
        .collect();

    info!(
        session_id = task.pipeline_id.as_str(),
        step_id = %task.step_id,
        status = completed.status,
        exit_code = completed.exit_code,
        elapsed_secs = completed.elapsed_seconds,
        output_file_count = files.len(),
        output_files = ?file_meta,
        success,
        "pipeline step completed"
    );
}

async fn stream_step_files(task: &StepTask, files: &StepFiles) {
    for (name, path) in files {
        let streamed_file = stream_file_chunks(name, path, task.chunk_size, |chunk| {
            let out_tx = task.out_tx.clone();
            let step_id = task.step_id.clone();
            async move {
                send_step_event(&out_tx, &step_id, proto::step_event::Detail::File(chunk)).await;
            }
        })
        .await;

        match streamed_file {
            Ok(streamed_file) => {
                info!(
                    session_id = task.pipeline_id.as_str(),
                    step_id = %task.step_id,
                    file = %name,
                    bytes = streamed_file.bytes,
                    chunks = streamed_file.chunks,
                    chunk_size = task.chunk_size,
                    "streamed pipeline step file to client"
                );
            }
            Err(e) => {
                warn!(
                    session_id = task.pipeline_id.as_str(),
                    step_id = %task.step_id,
                    file = %name,
                    error = %e,
                    "skip step output file (stream failed)"
                );
            }
        }
    }
}

async fn record_step_done(
    scheduler: &mut SchedulerState,
    graph: &DependencyGraph,
    step_outputs: &StepOutputs,
    done: StepDone,
) {
    scheduler.active_steps.remove(&done.id);
    scheduler.cancel_steps.remove(&done.id);
    scheduler.completed.insert(done.id.clone());

    if !done.success {
        scheduler.failed.insert(done.id.clone());
    }

    step_outputs
        .lock()
        .await
        .insert(done.id.clone(), done.files);
    scheduler.unblock_dependents(graph, &done.id);
}

async fn cancel_unfinished_steps(
    scheduler: &mut SchedulerState,
    graph: &DependencyGraph,
    out_tx: &PipelineSender,
    step_outputs: &StepOutputs,
    done_rx: &mut mpsc::Receiver<StepDone>,
) {
    for (_, cancel_tx) in scheduler.cancel_steps.drain() {
        let _ = cancel_tx.send(());
    }

    while !scheduler.active_steps.is_empty() {
        let Some(done) = done_rx.recv().await else {
            break;
        };

        scheduler.active_steps.remove(&done.id);
        scheduler.completed.insert(done.id.clone());
        scheduler.failed.insert(done.id.clone());
        step_outputs.lock().await.insert(done.id, done.files);
    }

    let unfinished: Vec<String> = graph
        .step_ids
        .iter()
        .filter(|id| {
            !scheduler.completed.contains(*id)
                && !scheduler.failed.contains(*id)
                && !scheduler.skipped.contains(*id)
        })
        .cloned()
        .collect();

    for step_id in unfinished {
        scheduler.failed.insert(step_id.clone());
        send_step_event(
            out_tx,
            &step_id,
            proto::step_event::Detail::Completed(status_completion(proto::RunStatus::TimedOut)),
        )
        .await;
    }
}

async fn finish_pipeline(run: &PipelineRun, scheduler: &SchedulerState, elapsed: Duration) {
    let skipped_steps: Vec<String> = scheduler.skipped.iter().cloned().collect();
    let all_succeeded = scheduler.all_succeeded(run.graph.step_ids.len());

    info!(
        session_id = %run.session_id,
        elapsed_secs = elapsed.as_secs_f64(),
        all_succeeded,
        failed_steps = ?scheduler.failed.iter().cloned().collect::<Vec<_>>(),
        skipped = ?skipped_steps,
        completed_steps = scheduler.completed.len(),
        "RunPipeline completed"
    );

    send_output(
        &run.out_tx,
        proto::PipelineOutput {
            payload: Some(proto::pipeline_output::Payload::PipelineCompleted(
                proto::PipelineCompleted {
                    all_succeeded,
                    elapsed_seconds: elapsed.as_secs_f64(),
                    skipped_steps,
                },
            )),
        },
    )
    .await;
}

fn status_completion(status: proto::RunStatus) -> proto::RunCompleted {
    proto::RunCompleted {
        status: status.into(),
        exit_code: -1,
        elapsed_seconds: 0.0,
        output_files: vec![],
    }
}

async fn send_step_event(
    out_tx: &PipelineSender,
    step_id: &str,
    detail: proto::step_event::Detail,
) {
    send_output(
        out_tx,
        proto::PipelineOutput {
            payload: Some(proto::pipeline_output::Payload::Step(proto::StepEvent {
                step_id: step_id.to_string(),
                detail: Some(detail),
            })),
        },
    )
    .await;
}

async fn send_output(out_tx: &PipelineSender, output: proto::PipelineOutput) {
    let _ = out_tx.send(Ok(output)).await;
}

async fn send_error(out_tx: &PipelineSender, status: Status) {
    let _ = out_tx.send(Err(status)).await;
}
