//! JS-compatible codec for libp2p request-response protocol.
//!
//! The JS node sends messages in a chunked format:
//! 1. First length-prefixed chunk: JSON-serialized header
//! 2. Subsequent length-prefixed chunks: JSON-serialized data (potentially split into 1MB chunks)
//!
//! This codec reads all chunks and reassembles them into a single message.

use std::{io, marker::PhantomData};

use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{StreamProtocol, request_response::Codec};
use serde::{Serialize, de::DeserializeOwned};
use unsigned_varint::{aio::read_usize, encode as varint_encode};

const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024; // 100 MB max total message size
const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB chunks (matching JS implementation)
const INITIAL_BUFFER_CAPACITY: usize = 64 * 1024; // 64 KB initial allocation

/// A codec that handles JS node's chunked message format.
///
/// JS sends:
/// - [varint length][header JSON]
/// - [varint length][data JSON chunk 1]
/// - [varint length][data JSON chunk 2]
/// - ...
///
/// This codec reads all chunks, concatenates the data chunks, then deserializes.
#[derive(Clone, Default)]
pub(crate) struct JsCompatCodec<Req, Resp> {
    _marker: PhantomData<(Req, Resp)>,
}

impl<Req, Resp> JsCompatCodec<Req, Resp> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Read a length-prefixed chunk from the stream.
/// Returns `Ok(None)` on clean EOF (no more chunks).
async fn read_length_prefixed<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let length = match read_usize(&mut *reader).await {
        Ok(len) => len,
        Err(e) => {
            let io_err: io::Error = e.into();
            if io_err.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(io_err);
        }
    };

    if length > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Message size {} exceeds maximum {}",
                length, MAX_MESSAGE_SIZE
            ),
        ));
    }

    let mut buffer = vec![0u8; length];
    reader.read_exact(&mut buffer).await?;

    Ok(Some(buffer))
}

/// Write a length-prefixed chunk to the stream.
async fn write_length_prefixed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> io::Result<()> {
    let mut length_buf = varint_encode::usize_buffer();
    let length_bytes = varint_encode::usize(data.len(), &mut length_buf);
    writer.write_all(length_bytes).await?;
    writer.write_all(data).await?;
    Ok(())
}

/// Read a JS-format message (header chunk + data chunks) and deserialize.
///
/// Format:
/// - First chunk: JSON header
/// - Remaining chunks: JSON data (concatenated if split into 1MB chunks)
async fn read_js_message<T, R>(io: &mut R) -> io::Result<T>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin + Send,
{
    // Read header chunk
    let header_bytes = read_length_prefixed(io)
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Expected header chunk"))?;

    // Read and concatenate all data chunks
    let mut data_bytes = Vec::with_capacity(INITIAL_BUFFER_CAPACITY);
    while let Some(chunk) = read_length_prefixed(io).await? {
        if data_bytes.len() + chunk.len() > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Total message size exceeds maximum",
            ));
        }
        data_bytes.extend_from_slice(&chunk);
    }

    // Parse header and data as JSON Values (avoids string concatenation)
    let header: serde_json::Value = serde_json::from_slice(&header_bytes).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid header JSON: {}", e),
        )
    })?;

    let data: serde_json::Value = serde_json::from_slice(&data_bytes).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid data JSON: {}", e),
        )
    })?;

    // Combine into the expected message structure
    let combined = serde_json::json!({
        "header": header,
        "data": data
    });

    tracing::trace!(
        header = %header,
        data_len = data_bytes.len(),
        "Received JS-format message"
    );

    serde_json::from_value(combined).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to deserialize message: {}", e),
        )
    })
}

/// Write a JS-format message (header chunk + 1MB data chunks).
///
/// Format:
/// - First chunk: JSON header
/// - Remaining chunks: JSON data split into 1MB chunks
async fn write_js_message<T, W>(io: &mut W, msg: T) -> io::Result<()>
where
    T: Serialize,
    W: AsyncWrite + Unpin + Send,
{
    // Serialize to JSON Value to extract header and data
    let value = serde_json::to_value(&msg).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to serialize message: {}", e),
        )
    })?;

    let header = value
        .get("header")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Message missing header"))?;
    let data = value
        .get("data")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Message missing data"))?;

    // Serialize header and data directly to bytes (avoids intermediate String)
    let header_bytes = serde_json::to_vec(header).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to serialize header: {}", e),
        )
    })?;

    let data_bytes = serde_json::to_vec(data).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to serialize data: {}", e),
        )
    })?;

    // Write header as single chunk
    write_length_prefixed(io, &header_bytes).await?;

    // Write data in 1MB chunks (matching JS implementation)
    for chunk in data_bytes.chunks(CHUNK_SIZE) {
        write_length_prefixed(io, chunk).await?;
    }

    io.close().await?;

    Ok(())
}

#[async_trait]
impl<Req, Resp> Codec for JsCompatCodec<Req, Resp>
where
    Req: Send + Serialize + DeserializeOwned,
    Resp: Send + Serialize + DeserializeOwned,
{
    type Protocol = StreamProtocol;
    type Request = Req;
    type Response = Resp;

    async fn read_request<T>(&mut self, _protocol: &Self::Protocol, io: &mut T) -> io::Result<Req>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_js_message(io).await
    }

    async fn read_response<T>(&mut self, _protocol: &Self::Protocol, io: &mut T) -> io::Result<Resp>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_js_message(io).await
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_js_message(io, req).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_js_message(io, resp).await
    }
}
