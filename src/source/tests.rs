use super::*;
use crate::fixtures::{ramp, wav_bytes};
use crate::queue::DeferredSource;
use crate::remote::test_server;
use std::future::pending;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: LazyLock<tokio::runtime::Runtime> =
        LazyLock::new(|| tokio::runtime::Runtime::new().expect("test runtime"));
    &RUNTIME
}

fn deferred() -> Location {
    Location::Deferred(DeferredSource {
        id: "stable-track".to_owned(),
    })
}

fn remote(address: &str) -> RemoteFile {
    RemoteFile {
        url: format!("{address}/track.wav?token=secret"),
        extension: Some("wav".to_owned()),
        bounded_range: None,
    }
}

fn receive(task: &OpenTask) -> Result<OpenedSource, String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match task.try_recv() {
            Ok(result) => return result,
            Err(mpsc::TryRecvError::Empty) => {
                assert!(Instant::now() < deadline, "the source opener answers");
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("the source opener disconnected: {error}"),
        }
    }
}

struct SequenceResolver {
    files: Vec<RemoteFile>,
    calls: Mutex<Vec<String>>,
}

impl SourceResolver for SequenceResolver {
    fn resolve<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<RemoteFile, String>> + Send + 'a>> {
        Box::pin(async move {
            let mut calls = self.calls.lock().unwrap();
            let file = self.files.get(calls.len()).cloned();
            calls.push(id.to_owned());
            file.ok_or_else(|| "no further source was supplied".to_owned())
        })
    }
}

#[test]
fn every_actual_open_resolves_again_and_seeks_the_new_native_source() {
    let first = test_server::serve(wav_bytes(8_000, 1, &ramp(1, 32_000)));
    let negative: Vec<i16> = ramp(1, 32_000).into_iter().map(|sample| -sample).collect();
    let second = test_server::serve(wav_bytes(8_000, 1, &negative));
    let resolver = Arc::new(SequenceResolver {
        files: vec![remote(&first), remote(&second), remote(&first)],
        calls: Mutex::new(Vec::new()),
    });
    let location = deferred();
    let unchanged = location.clone();
    for expected_sign in [1.0, -1.0] {
        let task = open_in_background(
            location.clone(),
            Some(resolver.clone()),
            runtime().handle().clone(),
            3.0,
        );
        let mut opened = receive(&task).expect("the freshly resolved file opens");
        assert!((opened.position_secs - 3.0).abs() < 0.001);
        assert!((opened.decoder.position_secs() - opened.position_secs).abs() < 0.001);
        let mut audio = [0.0; 128];
        assert!(opened.decoder.read(&mut audio).unwrap() > 0);
        assert!(
            audio[0] * expected_sign > 0.6,
            "fresh source samples: {}",
            audio[0]
        );
    }
    let mut decoder = open_blocking(&location, Some(resolver.clone()), runtime().handle())
        .expect("a synchronous worker also resolves afresh");
    assert!((decoder.seek(3.0).unwrap() - 3.0).abs() < 0.001);
    let mut audio = [0.0; 128];
    assert!(decoder.read(&mut audio).unwrap() > 0);
    assert!(audio[0] > 0.6);
    assert_eq!(location, unchanged, "queue identities never become URLs");
    assert_eq!(
        resolver.calls.lock().unwrap().as_slice(),
        ["stable-track"; 3]
    );
}

struct FailedResolver;

impl SourceResolver for FailedResolver {
    fn resolve<'a>(
        &'a self,
        _id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<RemoteFile, String>> + Send + 'a>> {
        Box::pin(async { Err("host authorization is unavailable".to_owned()) })
    }
}

#[test]
fn missing_and_failed_resolution_are_explicit_open_errors() {
    let missing = open_in_background(deferred(), None, runtime().handle().clone(), 0.0);
    assert_eq!(
        receive(&missing).err().unwrap(),
        "no source resolver is configured"
    );
    assert_eq!(
        open_blocking(&deferred(), None, runtime().handle())
            .err()
            .unwrap(),
        "no source resolver is configured"
    );
    let failed = open_in_background(
        deferred(),
        Some(Arc::new(FailedResolver)),
        runtime().handle().clone(),
        0.0,
    );
    assert_eq!(
        receive(&failed).err().unwrap(),
        "host authorization is unavailable"
    );
    assert_eq!(
        open_blocking(
            &deferred(),
            Some(Arc::new(FailedResolver)),
            runtime().handle()
        )
        .err()
        .unwrap(),
        "host authorization is unavailable"
    );
}

#[test]
fn invalid_positions_fail_before_resolving_and_native_seek_errors_are_delivered() {
    let address = test_server::serve(wav_bytes(8_000, 1, &ramp(1, 8_000)));
    let resolver = Arc::new(SequenceResolver {
        files: vec![remote(&address)],
        calls: Mutex::new(Vec::new()),
    });
    for position in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
        let task = open_in_background(
            deferred(),
            Some(resolver.clone()),
            runtime().handle().clone(),
            position,
        );
        assert_eq!(
            receive(&task).err().unwrap(),
            "source position must be finite and nonnegative"
        );
    }
    assert!(resolver.calls.lock().unwrap().is_empty());
    let task = open_in_background(
        deferred(),
        Some(resolver),
        runtime().handle().clone(),
        f64::MAX,
    );
    let error = receive(&task)
        .err()
        .expect("the native seek cannot represent this timestamp");
    assert!(error.starts_with("seek target out of range:"), "{error}");
}

#[test]
fn resolved_bytes_still_have_to_pass_native_decoder_probing() {
    let address = test_server::serve(b"this is not an audio file".to_vec());
    let resolver = Arc::new(SequenceResolver {
        files: vec![remote(&address)],
        calls: Mutex::new(Vec::new()),
    });
    let task = open_in_background(deferred(), Some(resolver), runtime().handle().clone(), 0.0);
    let error = receive(&task)
        .err()
        .expect("invalid bytes do not become a decoder");
    assert!(error.starts_with("probe format:"), "{error}");
    assert!(!error.contains("secret"), "{error}");
}

struct DropNotice(mpsc::Sender<()>);

impl Drop for DropNotice {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

struct PendingResolver {
    started: mpsc::Sender<()>,
    dropped: mpsc::Sender<()>,
    calls: AtomicUsize,
}

impl SourceResolver for PendingResolver {
    fn resolve<'a>(
        &'a self,
        _id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<RemoteFile, String>> + Send + 'a>> {
        Box::pin(async move {
            let _notice = DropNotice(self.dropped.clone());
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.send(()).unwrap();
            pending().await
        })
    }
}

#[test]
fn dropping_a_superseded_open_drops_its_pending_host_future() {
    let (started, start) = mpsc::channel();
    let (dropped, drop_notice) = mpsc::channel();
    let resolver = Arc::new(PendingResolver {
        started,
        dropped,
        calls: AtomicUsize::new(0),
    });
    let task = open_in_background(
        deferred(),
        Some(resolver.clone()),
        runtime().handle().clone(),
        0.0,
    );
    start
        .recv_timeout(Duration::from_secs(5))
        .expect("resolver is being polled");
    drop(task);
    drop_notice
        .recv_timeout(Duration::from_secs(5))
        .expect("pending host future was dropped");
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_an_open_cancels_an_inflight_http_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (started, start) = mpsc::channel();
    let (closed, close_notice) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" {
                break;
            }
        }
        started.send(()).unwrap();
        let disconnected = matches!(socket.read(&mut [0]), Ok(0));
        closed.send(disconnected).unwrap();
    });
    let task = open_in_background(
        Location::Remote(remote(&address)),
        None,
        runtime().handle().clone(),
        0.0,
    );
    start
        .recv_timeout(Duration::from_secs(5))
        .expect("HTTP request is pending");
    drop(task);
    assert!(close_notice
        .recv_timeout(Duration::from_secs(5))
        .expect("HTTP peer observes cancellation"));
    server.join().unwrap();
}

#[test]
fn local_files_keep_their_native_open_and_background_seek_behavior() {
    struct LocalFile(std::path::PathBuf);
    impl Drop for LocalFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let path = std::env::temp_dir().join(format!(
        "audio-stack-source-test-{}.wav",
        std::process::id()
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("create an isolated local source");
    let cleanup = LocalFile(path.clone());
    file.write_all(&wav_bytes(8_000, 1, &ramp(1, 32_000)))
        .unwrap();
    drop(file);
    let location = Location::File(path);
    let decoder = open_blocking(&location, None, runtime().handle()).unwrap();
    assert_eq!(decoder.format().sample_rate, 8_000);
    drop(decoder);
    let task = open_in_background(location, None, runtime().handle().clone(), 3.0);
    let mut opened = receive(&task).unwrap();
    assert!((opened.position_secs - 3.0).abs() < 0.001);
    let mut audio = [0.0; 128];
    assert!(opened.decoder.read(&mut audio).unwrap() > 0);
    assert!(audio[0] > 0.6);
    drop(opened);
    drop(task);
    drop(cleanup);
}
