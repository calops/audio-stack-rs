//! Opening stable queue locations without storing resolved addresses in them.

use std::future::Future;
use std::pin::Pin;
use std::sync::{mpsc, Arc};

use tokio::runtime::Handle;

use super::decode::Decoder;
use super::queue::{Location, RemoteFile};
use super::remote;

/// Host-owned resolution of a stable source identity into a playable file.
///
/// Called for each actual open, including preloads and reopens, never for a
/// whole queue. Superseding an open drops the pending future. Errors are shown
/// to the listener unchanged: return safe text without addresses or credentials.
pub trait SourceResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<RemoteFile, String>> + Send + 'a>>;
}

/// A decoder positioned on the blocking worker, ready for the engine to adopt.
pub(crate) struct OpenedSource {
    pub decoder: Decoder,
    pub position_secs: f64,
}

/// An asynchronous open owned by the playback operation that requested it.
/// Dropping it cancels pending resolution/connection and discards stale decoders.
pub(crate) struct OpenTask {
    receiver: mpsc::Receiver<Result<OpenedSource, String>>,
    task: tokio::task::JoinHandle<()>,
}

impl OpenTask {
    pub fn try_recv(&self) -> Result<Result<OpenedSource, String>, mpsc::TryRecvError> {
        self.receiver.try_recv()
    }
}

impl Drop for OpenTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) fn open_blocking(
    location: &Location,
    resolver: Option<Arc<dyn SourceResolver>>,
    runtime: &Handle,
) -> Result<Decoder, String> {
    match location {
        Location::File(path) => Decoder::open_file(path),
        Location::Remote(file) => remote::open_blocking(file, runtime),
        Location::Deferred(source) => {
            let resolver = resolver.ok_or("no source resolver is configured")?;
            let file = runtime.block_on(resolver.resolve(&source.id))?;
            remote::open_blocking(&file, runtime)
        }
    }
}

pub(crate) fn open_in_background(
    location: Location,
    resolver: Option<Arc<dyn SourceResolver>>,
    runtime: Handle,
    position_secs: f64,
) -> OpenTask {
    let (sender, receiver) = mpsc::channel();
    let task = runtime.spawn(async move {
        let result = open(location, resolver, position_secs).await;
        let _ = sender.send(result);
    });
    OpenTask { receiver, task }
}

async fn open(
    location: Location,
    resolver: Option<Arc<dyn SourceResolver>>,
    position_secs: f64,
) -> Result<OpenedSource, String> {
    if !position_secs.is_finite() || position_secs < 0.0 {
        return Err("source position must be finite and nonnegative".to_owned());
    }
    let worker = match location {
        Location::File(path) => tokio::task::spawn_blocking(move || {
            finish_open(Decoder::open_file(&path)?, position_secs)
        }),
        location => {
            let file = match location {
                Location::Remote(file) => file,
                Location::Deferred(source) => {
                    let resolver = resolver.ok_or("no source resolver is configured")?;
                    resolver.resolve(&source.id).await?
                }
                Location::File(_) => unreachable!(),
            };
            let (source, hint) = remote::connect(&file).await?;
            tokio::task::spawn_blocking(move || {
                finish_open(Decoder::open(source, hint)?, position_secs)
            })
        }
    };
    // Blocking decoder probes cannot be interrupted safely. If this future is
    // cancelled, Tokio drops their result when they finish, including the source
    // and its download/lookahead. No stale decoder reaches the engine.
    worker
        .await
        .map_err(|error| format!("source opener worker: {error}"))?
}

fn finish_open(mut decoder: Decoder, position_secs: f64) -> Result<OpenedSource, String> {
    let position_secs = if position_secs > 0.0 {
        decoder.seek(position_secs)?
    } else {
        decoder.position_secs()
    };
    Ok(OpenedSource {
        decoder,
        position_secs,
    })
}

#[cfg(test)]
mod tests;
