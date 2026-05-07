use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::proto::{
    DeleteFileRequest, DeleteFileResponse, FileChunk, FileInfo, GetFileRequest, ListFilesRequest,
    ListFilesResponse, UploadResponse,
};
use crate::session::new_session_id;
use crate::validation::validate_filename;
use crate::{files::stream_file_chunks, AppState};

pub async fn upload_file(
    state: &Arc<AppState>,
    request: Request<Streaming<FileChunk>>,
) -> Result<Response<UploadResponse>, Status> {
    let session_id = new_session_id();
    let _guard = state.exec_lock.read().await;

    // Streaming upload protocol:
    // - first chunk must include `name`
    // - subsequent chunks must leave `name` empty (saves bytes and prevents ambiguity)
    // The server writes to a temp file and renames at the end for atomic "publish".
    let mut stream = request.into_inner();
    let mut filename: Option<String> = None;
    let mut final_path: Option<std::path::PathBuf> = None;
    let mut tmp_path: Option<std::path::PathBuf> = None;
    let mut file: Option<tokio::fs::File> = None;
    let mut total_size = 0u64;

    while let Some(chunk) = stream.message().await? {
        if !chunk.name.is_empty() {
            if filename.is_some() {
                return Err(Status::invalid_argument(
                    "filename may only be set in the first chunk",
                ));
            }
            validate_filename(&chunk.name)?;
            filename = Some(chunk.name.clone());

            let path = state.workspace.join(&chunk.name);
            let t_path = state
                .workspace
                .join(format!("{}.{}.tmp_upload", chunk.name, session_id));
            final_path = Some(path);
            tmp_path = Some(t_path.clone());

            let mut f = tokio::fs::File::create(&t_path)
                .await
                .map_err(|e| Status::internal(format!("create failed: {e}")))?;

            f.write_all(&chunk.data)
                .await
                .map_err(|e| Status::internal(format!("write failed: {e}")))?;
            total_size += chunk.data.len() as u64;
            file = Some(f);
        } else {
            if let Some(f) = file.as_mut() {
                f.write_all(&chunk.data)
                    .await
                    .map_err(|e| Status::internal(format!("write failed: {e}")))?;
                total_size += chunk.data.len() as u64;
            } else {
                return Err(Status::invalid_argument(
                    "no filename provided in first chunk",
                ));
            }
        }
    }

    if let (Some(f), Some(p), Some(tp)) = (file, final_path, tmp_path) {
        f.sync_all()
            .await
            .map_err(|e| Status::internal(format!("sync failed: {e}")))?;
        // Drop the file to close it before renaming
        drop(f);
        // Atomic replace: clients reading the workspace will only ever observe either
        // the old file or the fully uploaded file, never a partially written one.
        tokio::fs::rename(&tp, &p)
            .await
            .map_err(|e| Status::internal(format!("rename failed: {e}")))?;
    } else {
        return Err(Status::invalid_argument("no filename provided"));
    }

    let name = filename.unwrap();
    info!(
        session_id = %session_id,
        file = %name,
        size_bytes = total_size,
        "upload_file completed"
    );
    Ok(Response::new(UploadResponse {
        name,
        size_bytes: total_size,
    }))
}

pub async fn get_file(
    state: &Arc<AppState>,
    request: Request<GetFileRequest>,
) -> Result<Response<ReceiverStream<Result<FileChunk, Status>>>, Status> {
    let session_id = new_session_id();
    let _guard = state.exec_lock.read().await;

    let req = request.into_inner();
    validate_filename(&req.name)?;

    let path = state.workspace.join(&req.name);
    if !path.exists() {
        return Err(Status::not_found(format!("file not found: {}", req.name)));
    }

    let chunk_size = state.chunk_size;
    let metadata = tokio::fs::metadata(&path)
        .await
        .map_err(|e| Status::internal(format!("metadata failed: {e}")))?;
    let total = metadata.len();
    let num_chunks = total.div_ceil(chunk_size as u64);
    let name = req.name.clone();
    info!(
        session_id = %session_id,
        file = %name,
        size_bytes = total,
        chunk_size,
        chunks = num_chunks,
        path = %path.display(),
        "get_file streaming response"
    );
    let (tx, rx) = mpsc::channel(4);

    tokio::spawn(async move {
        let result = stream_file_chunks(&name, &path, chunk_size, |chunk| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(Ok(chunk)).await;
            }
        })
        .await;

        if let Err(e) = result {
            let _ = tx
                .send(Err(Status::internal(format!("read failed: {e}"))))
                .await;
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}

pub async fn delete_file(
    state: &Arc<AppState>,
    request: Request<DeleteFileRequest>,
) -> Result<Response<DeleteFileResponse>, Status> {
    let session_id = new_session_id();
    let _guard = state.exec_lock.read().await;

    let req = request.into_inner();
    validate_filename(&req.name)?;

    let path = state.workspace.join(&req.name);
    if !path.exists() {
        return Err(Status::not_found(format!("file not found: {}", req.name)));
    }

    tokio::fs::remove_file(&path)
        .await
        .map_err(|e| Status::internal(format!("delete failed: {e}")))?;

    info!(
        session_id = %session_id,
        file = %req.name,
        path = %path.display(),
        "delete_file completed"
    );
    Ok(Response::new(DeleteFileResponse {}))
}

pub async fn list_files(
    state: &Arc<AppState>,
    _request: Request<ListFilesRequest>,
) -> Result<Response<ListFilesResponse>, Status> {
    let session_id = new_session_id();
    let _guard = state.exec_lock.read().await;

    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(&state.workspace)
        .await
        .map_err(|e| Status::internal(format!("readdir failed: {e}")))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| Status::internal(format!("readdir failed: {e}")))?
    {
        let meta = entry
            .metadata()
            .await
            .map_err(|e| Status::internal(format!("metadata failed: {e}")))?;
        if meta.is_file() {
            if let Some(name) = entry.file_name().to_str() {
                files.push(FileInfo {
                    name: name.to_string(),
                    size_bytes: meta.len(),
                });
            }
        }
    }

    files.sort_by(|a, b| a.name.cmp(&b.name));
    let summary: Vec<(&str, u64)> = files
        .iter()
        .map(|f| (f.name.as_str(), f.size_bytes))
        .collect();
    info!(
        session_id = %session_id,
        file_count = files.len(),
        workspace = %state.workspace.display(),
        files = ?summary,
        "list_files completed"
    );
    Ok(Response::new(ListFilesResponse { files }))
}
