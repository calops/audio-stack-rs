//! Strict finite ranges with one chunk of asynchronous lookahead. Unlike the
//! ordinary transport this never downloads the rest of a track into a file.

use std::io::{self, Read, Seek, SeekFrom};

use stream_download::http::reqwest::{header, Client, StatusCode, Url};
use symphonia::core::io::MediaSource;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::queue::BoundedRange;

#[derive(Clone)]
struct Fetcher {
    client: Client,
    url: Url,
    chunk_size: u64,
    length: Option<u64>,
}

struct Chunk {
    start: u64,
    bytes: Vec<u8>,
    total: u64,
    content_type: Option<String>,
}

impl Chunk {
    fn end(&self) -> u64 {
        self.start + self.bytes.len() as u64
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn network(error: stream_download::http::reqwest::Error) -> io::Error {
    // This also removes a redirect target, not only the original signed URL.
    io::Error::other(super::reason(&error.without_url()))
}

impl Fetcher {
    async fn fetch(&self, start: u64) -> io::Result<Chunk> {
        let end = start
            .saturating_add(self.chunk_size - 1)
            .min(self.length.map_or(u64::MAX, |length| length - 1));
        let mut response = self
            .client
            .get(self.url.clone())
            .header(header::RANGE, format!("bytes={start}-{end}"))
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(network)?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(io::Error::other(format!(
                "bounded range requires 206 Partial Content; the server answered {}",
                response.status()
            )));
        }
        let (actual_start, actual_end, total) = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_content_range)
            .ok_or_else(|| invalid("invalid or missing Content-Range"))?;
        if actual_start != start
            || actual_end != end.min(total - 1)
            || self.length.is_some_and(|length| length != total)
        {
            return Err(invalid(
                "Content-Range does not match the requested file range",
            ));
        }
        if response
            .headers()
            .get(header::CONTENT_ENCODING)
            .is_some_and(|value| value != "identity")
        {
            return Err(invalid("encoded bounded range response"));
        }
        let expected = actual_end - actual_start + 1;
        if let Some(value) = response.headers().get(header::CONTENT_LENGTH) {
            let length = value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok());
            if length != Some(expected) {
                return Err(invalid("Content-Length does not match Content-Range"));
            }
        }
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(usize::try_from(expected).map_err(|_| invalid("range too large"))?)
            .map_err(|_| io::Error::other("cannot allocate bounded range buffer"))?;
        while let Some(part) = response.chunk().await.map_err(network)? {
            if part.len() as u64 > expected - bytes.len() as u64 {
                return Err(invalid("bounded range response exceeds Content-Range"));
            }
            bytes.extend_from_slice(&part);
        }
        if bytes.len() as u64 != expected {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short bounded range response",
            ));
        }
        Ok(Chunk {
            start,
            bytes,
            total,
            content_type,
        })
    }
}

/// Only the finite single-range form is accepted. No wildcard total, multipart,
/// overflowing offset, or empty resource can describe a usable audio chunk.
fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let number = |value: &str| {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        value.parse::<u64>().ok()
    };
    let (start, end, total) = (number(start)?, number(end)?, number(total)?);
    (start <= end && end < total).then_some((start, end, total))
}

pub(super) struct Source {
    fetcher: Fetcher,
    runtime: Handle,
    shown: String,
    position: u64,
    length: u64,
    chunk: Chunk,
    pending: Option<(u64, JoinHandle<io::Result<Chunk>>)>,
    failure: Option<String>,
}

impl Source {
    pub(super) async fn connect(
        client: Client,
        url: Url,
        config: &BoundedRange,
        shown: String,
    ) -> io::Result<(Self, Option<String>)> {
        if config.content_length == Some(0) {
            return Err(invalid("bounded range file length must be positive"));
        }
        let mut fetcher = Fetcher {
            client,
            url,
            chunk_size: config.chunk_size.get(),
            length: config.content_length,
        };
        let chunk = fetcher.fetch(0).await?;
        let length = chunk.total;
        let content_type = chunk.content_type.clone();
        fetcher.length = Some(length);
        let mut source = Self {
            fetcher,
            runtime: Handle::current(),
            shown,
            position: 0,
            length,
            chunk,
            pending: None,
            failure: None,
        };
        source.prefetch();
        Ok((source, content_type))
    }

    fn cancel_prefetch(&mut self) {
        if let Some((_, pending)) = self.pending.take() {
            pending.abort();
        }
    }

    fn prefetch(&mut self) {
        let start = self.chunk.end();
        if start < self.length {
            let fetcher = self.fetcher.clone();
            self.pending = Some((
                start,
                self.runtime
                    .spawn(async move { fetcher.fetch(start).await }),
            ));
        }
    }

    fn load_chunk(&mut self) -> io::Result<()> {
        let pending = match self.pending.take() {
            Some((start, pending)) if start == self.position => pending,
            other => {
                if let Some((_, pending)) = other {
                    pending.abort();
                }
                let fetcher = self.fetcher.clone();
                let start = self.position;
                self.runtime
                    .spawn(async move { fetcher.fetch(start).await })
            }
        };
        let result = self
            .runtime
            .block_on(pending)
            .map_err(|_| io::Error::other("bounded range task stopped"))
            .and_then(|result| result);
        match result {
            Ok(chunk) => {
                self.chunk = chunk;
                self.prefetch();
                Ok(())
            }
            Err(error) => {
                let message = format!("{}: {error}", self.shown);
                self.failure = Some(message.clone());
                Err(io::Error::new(error.kind(), message))
            }
        }
    }
}

impl Read for Source {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if let Some(message) = &self.failure {
            return Err(io::Error::other(message.clone()));
        }
        if self.position >= self.length {
            return Ok(0);
        }
        if self.position < self.chunk.start || self.position >= self.chunk.end() {
            self.load_chunk()?;
        }
        let offset = (self.position - self.chunk.start) as usize;
        let count = buffer.len().min(self.chunk.bytes.len() - offset);
        buffer[..count].copy_from_slice(&self.chunk.bytes[offset..offset + count]);
        self.position += count as u64;
        Ok(count)
    }
}

impl Seek for Source {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => self.length.checked_add_signed(offset),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid remote seek"))?;
        if position != self.position {
            self.cancel_prefetch();
            self.failure = None;
        }
        self.position = position;
        Ok(position)
    }
}

impl MediaSource for Source {
    fn is_seekable(&self) -> bool {
        true
    }
    fn byte_len(&self) -> Option<u64> {
        Some(self.length)
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.cancel_prefetch();
    }
}

#[cfg(test)]
mod tests;
