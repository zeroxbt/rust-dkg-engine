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
use unsigned_varint::aio::read_usize;
use unsigned_varint::encode as varint_encode;

const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024; // 100 MB max total message size

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
pub struct JsCompatCodec<Req, Resp> {
    _marker: PhantomData<(Req, Resp)>,
}

impl<Req, Resp> JsCompatCodec<Req, Resp> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Read a length-prefixed message chunk from the stream
async fn read_length_prefixed<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<Vec<u8>>> {
    // Read the varint length prefix
    let length = match read_usize(&mut *reader).await {
        Ok(len) => len,
        Err(e) => {
            // EOF or other error - check if it's a clean EOF
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
            format!("Message size {} exceeds maximum {}", length, MAX_MESSAGE_SIZE),
        ));
    }

    // Read the data
    let mut buffer = vec![0u8; length];
    reader.read_exact(&mut buffer).await?;

    Ok(Some(buffer))
}

/// Write a length-prefixed message chunk to the stream
async fn write_length_prefixed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> io::Result<()> {
    // Write varint length prefix
    let mut length_buf = varint_encode::usize_buffer();
    let length_bytes = varint_encode::usize(data.len(), &mut length_buf);
    writer.write_all(length_bytes).await?;

    // Write data
    writer.write_all(data).await?;

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
        // Read header chunk
        let header_bytes = read_length_prefixed(io)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Expected header chunk"))?;

        // Read all data chunks and concatenate them
        let mut data_bytes = Vec::new();
        while let Some(chunk) = read_length_prefixed(io).await? {
            if data_bytes.len() + chunk.len() > MAX_MESSAGE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Total message size exceeds maximum",
                ));
            }
            data_bytes.extend(chunk);
        }

        // Parse header and data separately, then combine
        let header_str = std::str::from_utf8(&header_bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid UTF-8 in header: {}", e),
            )
        })?;

        let data_str = std::str::from_utf8(&data_bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid UTF-8 in data: {}", e),
            )
        })?;

        // Combine into a single JSON object: {"header": ..., "data": ...}
        let combined = format!(r#"{{"header":{},"data":{}}}"#, header_str, data_str);

        tracing::trace!(
            header = %header_str,
            data_len = data_bytes.len(),
            "Received JS-format request"
        );

        serde_json::from_str(&combined).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize request: {}", e),
            )
        })
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Resp>
    where
        T: AsyncRead + Unpin + Send,
    {
        // Same format as request
        let header_bytes = read_length_prefixed(io)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Expected header chunk"))?;

        let mut data_bytes = Vec::new();
        while let Some(chunk) = read_length_prefixed(io).await? {
            if data_bytes.len() + chunk.len() > MAX_MESSAGE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Total message size exceeds maximum",
                ));
            }
            data_bytes.extend(chunk);
        }

        let header_str = std::str::from_utf8(&header_bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid UTF-8 in header: {}", e),
            )
        })?;

        let data_str = std::str::from_utf8(&data_bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid UTF-8 in data: {}", e),
            )
        })?;

        let combined = format!(r#"{{"header":{},"data":{}}}"#, header_str, data_str);

        tracing::trace!(
            header = %header_str,
            data_len = data_bytes.len(),
            "Received JS-format response"
        );

        serde_json::from_str(&combined).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize response: {}", e),
            )
        })
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
        // Serialize the full message first to extract header and data
        let value = serde_json::to_value(&req).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize request: {}", e),
            )
        })?;

        let header = value
            .get("header")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Request missing header"))?;
        let data = value
            .get("data")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Request missing data"))?;

        let header_json = serde_json::to_string(header).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize header: {}", e),
            )
        })?;

        let data_json = serde_json::to_string(data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize data: {}", e),
            )
        })?;

        // Write header chunk
        write_length_prefixed(io, header_json.as_bytes()).await?;

        // Write data in 1MB chunks (matching JS implementation)
        const CHUNK_SIZE: usize = 1024 * 1024;
        for chunk in data_json.as_bytes().chunks(CHUNK_SIZE) {
            write_length_prefixed(io, chunk).await?;
        }

        io.close().await?;

        Ok(())
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
        // Same format as request
        let value = serde_json::to_value(&resp).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize response: {}", e),
            )
        })?;

        let header = value.get("header").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Response missing header")
        })?;
        let data = value
            .get("data")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Response missing data"))?;

        let header_json = serde_json::to_string(header).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize header: {}", e),
            )
        })?;

        let data_json = serde_json::to_string(data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize data: {}", e),
            )
        })?;

        write_length_prefixed(io, header_json.as_bytes()).await?;

        const CHUNK_SIZE: usize = 1024 * 1024;
        for chunk in data_json.as_bytes().chunks(CHUNK_SIZE) {
            write_length_prefixed(io, chunk).await?;
        }

        io.close().await?;

        Ok(())
    }
}
