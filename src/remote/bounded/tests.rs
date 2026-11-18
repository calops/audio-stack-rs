use super::*;
use crate::fixtures::{ramp, wav_bytes};
use crate::queue::RemoteFile;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy)]
enum Fault {
    None,
    IgnoreRange,
    MissingRange,
    WrongStart,
    WrongEnd,
    WildcardTotal,
    ChangedTotal,
    WrongLength,
    ShortBody,
    LongBody,
    Forbidden,
    Encoded,
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<(u64, u64)>>>,
    requested: mpsc::Receiver<(u64, u64)>,
    fault_from: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn new(body: Vec<u8>, fault: Fault, fault_from: u64) -> Self {
        Self::start(body, fault, fault_from, None)
    }

    fn start(
        body: Vec<u8>,
        fault: Fault,
        fault_from: u64,
        stalled: Option<mpsc::Sender<bool>>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener.set_nonblocking(true).unwrap();
        let url = format!(
            "http://{}/track.wav?token=secret",
            listener.local_addr().unwrap()
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let (notify, requested) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let fault_from = Arc::new(AtomicU64::new(fault_from));
        let faults = fault_from.clone();
        let stopped = stop.clone();
        let body = Arc::new(body);
        let thread = thread::spawn(move || {
            let mut handlers = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let body = body.clone();
                        let seen = seen.clone();
                        let stalled = stalled.clone();
                        let notify = notify.clone();
                        let fault_from = faults.clone();
                        handlers.push(thread::spawn(move || {
                            serve(socket, &body, fault, fault_from, seen, notify, stalled);
                        }));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            }
            for handler in handlers {
                handler.join().unwrap();
            }
        });
        Self {
            url,
            requests,
            stop,
            requested,
            fault_from,
            thread: Some(thread),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}

fn serve(
    mut socket: TcpStream,
    body: &[u8],
    fault: Fault,
    fault_from: Arc<AtomicU64>,
    seen: Arc<Mutex<Vec<(u64, u64)>>>,
    notify: mpsc::Sender<(u64, u64)>,
    stalled: Option<mpsc::Sender<bool>>,
) {
    // On macOS an accepted socket inherits the listener's non-blocking flag,
    // and the first read would fail with WouldBlock before the request lands.
    socket.set_nonblocking(false).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut reader = BufReader::new(socket.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let mut range = None;
    loop {
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("range: bytes=") {
            range = value.trim().split_once('-').and_then(|(start, end)| {
                Some((start.parse::<u64>().ok()?, end.parse::<u64>().ok()?))
            });
        }
    }
    // This server deliberately rejects both ordinary and open-ended requests.
    let Some((start, requested_end)) = range else {
        let _ = socket
            .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return;
    };
    seen.lock().unwrap().push((start, requested_end));
    let _ = notify.send((start, requested_end));
    let fault = if start >= fault_from.load(Ordering::Relaxed) {
        fault
    } else {
        Fault::None
    };
    if start >= body.len() as u64 || requested_end < start {
        let _ = socket.write_all(
            b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        return;
    }
    let end = requested_end.min(body.len() as u64 - 1);
    let slice = &body[start as usize..=end as usize];
    let total = body.len() as u64;
    let content_range = match fault {
        Fault::MissingRange => String::new(),
        Fault::WrongStart => format!("Content-Range: bytes {}-{end}/{total}\r\n", start + 1),
        Fault::WrongEnd => format!("Content-Range: bytes {start}-{}/{total}\r\n", end - 1),
        Fault::WildcardTotal => format!("Content-Range: bytes {start}-{end}/*\r\n"),
        Fault::ChangedTotal => format!("Content-Range: bytes {start}-{end}/{}\r\n", total + 1),
        _ => format!("Content-Range: bytes {start}-{end}/{total}\r\n"),
    };
    let status = match fault {
        Fault::IgnoreRange => "200 OK",
        Fault::Forbidden => "403 Forbidden",
        _ => "206 Partial Content",
    };
    let length = match fault {
        Fault::WrongLength => format!("Content-Length: {}\r\n", slice.len() + 1),
        Fault::ShortBody | Fault::LongBody => String::new(),
        _ => format!("Content-Length: {}\r\n", slice.len()),
    };
    let encoding = if matches!(fault, Fault::Encoded) {
        "Content-Encoding: gzip\r\n"
    } else {
        ""
    };
    let header = format!("HTTP/1.1 {status}\r\nContent-Type: audio/wav\r\n{content_range}{length}{encoding}Connection: close\r\n\r\n");
    if socket.write_all(header.as_bytes()).is_err() {
        return;
    }
    if let Some(stalled) = stalled.filter(|_| start > 0) {
        let _ = stalled.send(false); // A lookahead request has reached the body.
        let mut byte = [0];
        // A client that drops a response mid-body may close with a reset
        // rather than a FIN; both mean the request was abandoned.
        let closed = match socket.read(&mut byte) {
            Ok(0) => true,
            Err(error) => error.kind() == io::ErrorKind::ConnectionReset,
            Ok(_) => false,
        };
        let _ = stalled.send(closed);
        return;
    }
    let slice = if matches!(fault, Fault::ShortBody) {
        &slice[..slice.len() - 1]
    } else {
        slice
    };
    let _ = socket.write_all(slice);
    if matches!(fault, Fault::LongBody) {
        let _ = socket.write_all(&[255]);
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> =
        std::sync::LazyLock::new(|| tokio::runtime::Runtime::new().unwrap());
    &RUNTIME
}

fn config(size: u64, length: Option<u64>) -> BoundedRange {
    BoundedRange {
        chunk_size: NonZeroU64::new(size).unwrap(),
        content_length: length,
    }
}

fn source(server: &Server, config: BoundedRange) -> io::Result<Source> {
    runtime()
        .block_on(Source::connect(
            Client::new(),
            server.url.parse().unwrap(),
            &config,
            server.url.split('?').next().unwrap().to_owned(),
        ))
        .map(|(source, _)| source)
}

#[test]
fn stitches_chunks_and_seeks_with_the_whole_file_length() {
    let bytes: Vec<u8> = (0..53).collect();
    let server = Server::new(bytes.clone(), Fault::None, 0);
    for length in [None, Some(bytes.len() as u64)] {
        let mut source = source(&server, config(7, length)).unwrap();
        assert_eq!(source.byte_len(), Some(53));
        let mut output = Vec::new();
        source.read_to_end(&mut output).unwrap();
        assert_eq!(output, bytes);
        for offset in [3, 24, 52, 0, 7, 53, 80] {
            assert_eq!(source.seek(SeekFrom::Start(offset)).unwrap(), offset);
            let mut output = [255; 6];
            let count = source.read(&mut output).unwrap();
            let start = (offset as usize).min(bytes.len());
            assert_eq!(&output[..count], &bytes[start..start + count]);
            assert_eq!(count == 0, offset >= 53);
        }
        source.seek(SeekFrom::End(-2)).unwrap();
        let mut output = Vec::new();
        source.read_to_end(&mut output).unwrap();
        assert_eq!(output, [51, 52]);
        assert!(source.seek(SeekFrom::Current(-54)).is_err());
        source.seek(SeekFrom::Start(u64::MAX)).unwrap();
        assert!(source.seek(SeekFrom::Current(1)).is_err());
        assert_eq!(source.read(&mut [0]).unwrap(), 0);
    }
    let requests = server.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|&(start, end)| start == 49 && end == 52));
    assert!(requests
        .iter()
        .all(|&(start, end)| start <= end && end - start < 7));
}

#[test]
fn rejects_unusable_range_responses_without_exposing_credentials() {
    for fault in [
        Fault::IgnoreRange,
        Fault::MissingRange,
        Fault::WrongStart,
        Fault::WrongEnd,
        Fault::WildcardTotal,
        Fault::ChangedTotal,
        Fault::WrongLength,
        Fault::ShortBody,
        Fault::LongBody,
        Fault::Forbidden,
        Fault::Encoded,
    ] {
        let server = Server::new(vec![42; 31], fault, 0);
        let error = source(&server, config(7, Some(31)))
            .err()
            .expect("invalid range fails");
        assert!(!error.to_string().contains("secret"));
    }
    let server = Server::new(vec![42; 31], Fault::None, 0);
    assert!(source(&server, config(7, Some(30))).is_err());
    assert!(source(&server, config(7, Some(0))).is_err());
}

#[test]
fn later_failures_reach_the_reader_instead_of_becoming_eof_or_retrying() {
    for fault in [Fault::Forbidden, Fault::ChangedTotal, Fault::ShortBody] {
        let server = Server::new(vec![42; 31], fault, 7);
        let mut source = source(&server, config(7, None)).unwrap();
        let mut first = [0; 7];
        source.read_exact(&mut first).unwrap();
        assert_eq!(first, [42; 7]);
        let error = source.read(&mut first).unwrap_err().to_string();
        assert!(!error.contains("secret"));
        assert!(error.contains("/track.wav:"));
        assert!(source.read(&mut first).is_err());
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}

#[test]
fn a_late_short_mp3_range_is_a_decoder_error_not_track_completion() {
    // Silent MPEG-1 Layer III frames, 128 kbps at 44.1 kHz.
    let mut frame = vec![0; 417];
    frame[..4].copy_from_slice(&[0xff, 0xfb, 0x90, 0x64]);
    let server = Server::new(frame.repeat(1000), Fault::ShortBody, u64::MAX);
    let source = source(&server, config(4096, None)).unwrap();
    let mut hint = symphonia::core::formats::probe::Hint::new();
    hint.with_extension("mp3");
    let mut decoder = crate::decode::Decoder::open(Box::new(source), hint).unwrap();
    server.fault_from.store(65_536, Ordering::Relaxed);
    let mut samples = [0.0; 4096];
    let mut decoded = 0;
    let error = loop {
        match decoder.read(&mut samples) {
            Ok(0) => panic!("short HTTP response silently completed the track"),
            Ok(count) => decoded += count,
            Err(error) => break error,
        }
    };
    assert!(decoded > 0, "the failure occurs after playback starts");
    assert!(error.contains("short bounded range response"), "{error}");
    assert!(!error.contains("secret"));
    assert!(!decoder.is_exhausted());
}

#[test]
fn cached_and_boundary_seeks_reuse_the_downloaded_lookahead() {
    let server = Server::new((0..31).collect(), Fault::None, 0);
    let mut source = source(&server, config(7, None)).unwrap();
    assert_eq!(
        server
            .requested
            .recv_timeout(Duration::from_secs(3))
            .unwrap(),
        (0, 6)
    );
    assert_eq!(
        server
            .requested
            .recv_timeout(Duration::from_secs(3))
            .unwrap(),
        (7, 13)
    );
    source.seek(SeekFrom::Start(3)).unwrap();
    let mut cached = [0; 4];
    source.read_exact(&mut cached).unwrap();
    assert_eq!(cached, [3, 4, 5, 6]);
    source.seek(SeekFrom::Start(0)).unwrap();
    source.seek(SeekFrom::Start(7)).unwrap();
    let mut next = [0; 7];
    source.read_exact(&mut next).unwrap();
    assert_eq!(next, [7, 8, 9, 10, 11, 12, 13]);
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|&&(start, _)| start == 7)
            .count(),
        1
    );
}

#[test]
fn returning_from_an_eof_probe_restarts_lookahead_before_cached_reads() {
    let server = Server::new((0..31).collect(), Fault::None, 0);
    let mut source = source(&server, config(7, None)).unwrap();
    assert_eq!(
        server
            .requested
            .recv_timeout(Duration::from_secs(3))
            .unwrap(),
        (0, 6)
    );
    assert_eq!(
        server
            .requested
            .recv_timeout(Duration::from_secs(3))
            .unwrap(),
        (7, 13)
    );
    source.seek(SeekFrom::End(0)).unwrap();
    source.seek(SeekFrom::Start(0)).unwrap();
    assert_eq!(
        server
            .requested
            .recv_timeout(Duration::from_secs(3))
            .unwrap(),
        (7, 13)
    );
    let mut bytes = [0; 14];
    source.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|&&(start, _)| start == 7)
            .count(),
        2
    );
}

#[test]
fn dropping_or_seeking_cancels_inflight_lookahead() {
    for seek in [false, true] {
        let (sender, receiver) = mpsc::channel();
        let server = Server::start(vec![42; 31], Fault::None, 0, Some(sender));
        let mut source = source(&server, config(7, None)).unwrap();
        assert!(!receiver.recv_timeout(Duration::from_secs(3)).unwrap());
        if seek {
            source.seek(SeekFrom::End(0)).unwrap();
            assert_eq!(source.read(&mut [0]).unwrap(), 0);
        } else {
            drop(source);
        }
        assert!(
            receiver.recv_timeout(Duration::from_secs(3)).unwrap(),
            "request body was closed"
        );
    }
}

#[test]
fn native_decoder_and_background_preload_use_bounded_requests() {
    let sample_rate = 8_000;
    let bytes = wav_bytes(sample_rate, 1, &ramp(1, sample_rate as usize * 4));
    let server = Server::new(bytes.clone(), Fault::None, 0);
    let remote = RemoteFile {
        url: server.url.clone(),
        extension: Some("wav".into()),
        bounded_range: Some(config(4096, Some(bytes.len() as u64))),
    };
    let task = crate::source::open_in_background(
        crate::queue::Location::Remote(remote),
        None,
        runtime().handle().clone(),
        3.0,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut opened = loop {
        match task.try_recv() {
            Ok(result) => break result.unwrap(),
            Err(mpsc::TryRecvError::Empty) => {
                assert!(std::time::Instant::now() < deadline, "the opener answers");
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("the opener disconnected: {error}"),
        }
    };
    assert!((opened.position_seconds - 3.0).abs() < 0.001);
    let decoder = &mut opened.decoder;
    let duration = decoder.format().duration_secs.unwrap();
    assert!((duration - 4.0).abs() < 0.01);
    let mut audio = vec![0.0; 256];
    assert!(decoder.read(&mut audio).unwrap() > 0);
    assert!(audio[0] > 0.6);
    decoder.seek(0.5).unwrap();
    assert!(decoder.read(&mut audio).unwrap() > 0);
    assert!(audio[0] < 0.3);
    drop(opened);
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|&(start, end)| end - start < 4096));
}

#[test]
fn finite_content_range_parser_rejects_invalid_boundaries() {
    for value in [
        "bytes 0-0/0",
        "bytes 9-8/10",
        "bytes 0-10/10",
        "bytes 0-1/*",
        "items 0-1/2",
        "bytes +0-1/2",
        "bytes 0-1/18446744073709551616",
        "bytes 0-1/2,3-4/5",
    ] {
        assert_eq!(parse_content_range(value), None, "{value}");
    }
    assert_eq!(parse_content_range("bytes 0-0/1"), Some((0, 0, 1)));
}
