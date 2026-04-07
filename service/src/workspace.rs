use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::proto::{
    DeleteFileRequest, DeleteFileResponse, FileChunk, FileInfo, GetFileRequest, ListFilesRequest,
    ListFilesResponse, UploadResponse,
};
use crate::AppState;

fn validate_filename(name: &str) -> Result<(), Status> {
    if name.is_empty() {
        return Err(Status::invalid_argument("filename must not be empty"));
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err(Status::invalid_argument(
            "filename must not contain '/', '\\', or '..'",
        ));
    }
    Ok(())
}

pub async fn upload_file(
    state: &Arc<AppState>,
    request: Request<Streaming<FileChunk>>,
) -> Result<Response<UploadResponse>, Status> {
    let _guard = state.exec_lock.read().await;

    let mut stream = request.into_inner();
    let mut filename: Option<String> = None;
    let mut buf = Vec::new();

    while let Some(chunk) = stream.message().await? {
        if !chunk.name.is_empty() {
            if filename.is_some() {
                return Err(Status::invalid_argument(
                    "filename may only be set in the first chunk",
                ));
            }
            validate_filename(&chunk.name)?;
            filename = Some(chunk.name);
        }
        buf.extend_from_slice(&chunk.data);
    }

    let name = filename.ok_or_else(|| Status::invalid_argument("no filename provided"))?;
    let path = state.workspace.join(&name);
    tokio::fs::write(&path, &buf)
        .await
        .map_err(|e| Status::internal(format!("write failed: {e}")))?;

    info!(file = %name, size = buf.len(), "uploaded");
    Ok(Response::new(UploadResponse {
        name,
        size_bytes: buf.len() as u64,
    }))
}

pub async fn get_file(
    state: &Arc<AppState>,
    request: Request<GetFileRequest>,
) -> Result<Response<ReceiverStream<Result<FileChunk, Status>>>, Status> {
    let _guard = state.exec_lock.read().await;

    let req = request.into_inner();
    validate_filename(&req.name)?;

    let path = state.workspace.join(&req.name);
    if !path.exists() {
        return Err(Status::not_found(format!("file not found: {}", req.name)));
    }

    let data = tokio::fs::read(&path)
        .await
        .map_err(|e| Status::internal(format!("read failed: {e}")))?;

    let chunk_size = state.chunk_size;
    let name = req.name.clone();
    let (tx, rx) = mpsc::channel(4);

    tokio::spawn(async move {
        let mut offset = 0;
        let mut first = true;
        while offset < data.len() {
            let end = (offset + chunk_size).min(data.len());
            let chunk = FileChunk {
                name: if first { name.clone() } else { String::new() },
                data: data[offset..end].to_vec(),
            };
            first = false;
            if tx.send(Ok(chunk)).await.is_err() {
                break;
            }
            offset = end;
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}

pub async fn delete_file(
    state: &Arc<AppState>,
    request: Request<DeleteFileRequest>,
) -> Result<Response<DeleteFileResponse>, Status> {
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

    info!(file = %req.name, "deleted");
    Ok(Response::new(DeleteFileResponse {}))
}

pub async fn list_files(
    state: &Arc<AppState>,
    _request: Request<ListFilesRequest>,
) -> Result<Response<ListFilesResponse>, Status> {
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
    Ok(Response::new(ListFilesResponse { files }))
}
