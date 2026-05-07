use std::future::Future;
use std::io;
use std::path::Path;

use tokio::io::AsyncReadExt;

use crate::proto::FileChunk;

pub struct StreamedFile {
    pub bytes: u64,
    pub chunks: u64,
}

/// Stream one file using the service-wide chunk convention.
///
/// The first chunk carries the filename and later chunks leave `name` empty.
/// Empty files still emit a single empty chunk, so clients can distinguish an
/// empty output from a missing output.
pub async fn stream_file_chunks<F, Fut>(
    name: &str,
    path: &Path,
    chunk_size: usize,
    mut send_chunk: F,
) -> io::Result<StreamedFile>
where
    F: FnMut(FileChunk) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut file = tokio::fs::File::open(path).await?;
    let bytes = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let chunks = bytes.div_ceil(chunk_size as u64);

    let mut buf = vec![0u8; chunk_size];
    let mut first = true;

    loop {
        match file.read(&mut buf).await? {
            0 => {
                if first {
                    send_chunk(FileChunk {
                        name: name.to_string(),
                        data: Vec::new(),
                    })
                    .await;
                }
                break;
            }
            n => {
                send_chunk(FileChunk {
                    name: if first {
                        name.to_string()
                    } else {
                        String::new()
                    },
                    data: buf[..n].to_vec(),
                })
                .await;
                first = false;
            }
        }
    }

    Ok(StreamedFile { bytes, chunks })
}
