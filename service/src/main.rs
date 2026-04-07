mod executor;
mod pipeline;
mod runner;
mod session;
mod workspace;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;
use tracing::{info, warn};

pub mod proto {
    tonic::include_proto!("at.runner.v1");
}

#[derive(Parser)]
#[command(
    name = "at-runner",
    about = "gRPC service wrapping Acoustics Toolbox executables"
)]
struct Cli {
    #[arg(long, default_value = "50051")]
    port: u16,

    #[arg(long, default_value = "/at/bin")]
    bin_dir: PathBuf,

    #[arg(long, default_value = "/workspace")]
    workspace: PathBuf,

    #[arg(long, default_value = "300")]
    default_timeout: u64,

    #[arg(long, default_value = "65536")]
    chunk_size: usize,
}

pub struct AppState {
    pub bin_dir: PathBuf,
    pub workspace: PathBuf,
    pub allowlist: HashSet<String>,
    pub default_timeout: u64,
    pub chunk_size: usize,
    pub exec_lock: tokio::sync::RwLock<()>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "at_runner=info,tonic=info".into()),
        )
        .init();

    let cli = Cli::parse();

    let allowlist = build_allowlist(&cli.bin_dir);
    if allowlist.is_empty() {
        warn!("no executables found in {:?}", cli.bin_dir);
    } else {
        info!(
            count = allowlist.len(),
            "discovered executables: {:?}", allowlist
        );
    }

    if !cli.workspace.exists() {
        std::fs::create_dir_all(&cli.workspace)?;
    }

    let state = Arc::new(AppState {
        bin_dir: cli.bin_dir,
        workspace: cli.workspace,
        allowlist,
        default_timeout: cli.default_timeout,
        chunk_size: cli.chunk_size,
        exec_lock: tokio::sync::RwLock::new(()),
    });

    let runner_svc = runner::RunnerService::new(state.clone());
    let svc = proto::runner_server::RunnerServer::new(runner_svc)
        .max_decoding_message_size(256 * 1024 * 1024)
        .max_encoding_message_size(256 * 1024 * 1024);

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<proto::runner_server::RunnerServer<runner::RunnerService>>()
        .await;

    let addr = format!("0.0.0.0:{}", cli.port).parse()?;
    info!(%addr, "starting at-runner");

    Server::builder()
        .add_service(health_service)
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}

fn build_allowlist(bin_dir: &PathBuf) -> HashSet<String> {
    let mut set = HashSet::new();
    let Ok(entries) = std::fs::read_dir(bin_dir) else {
        return set;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("exe") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                set.insert(stem.to_string());
            }
        }
    }
    set
}
