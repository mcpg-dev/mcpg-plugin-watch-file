//! `file_tail`: each line appended to the files a set of globs matches is
//! one event.
//!
//! A file is tracked by its identity (device and inode, or its creation
//! time where there are none) and a fingerprint of its first bytes, never by
//! its name: a rename keeps its position, a rotated file is read to its end
//! for `rotate_wait_ms`, and a truncated or rewritten file is read again
//! from the start after a `file.truncated` event.
//!
//! Lines are emitted at most `max_in_flight` ahead of the host's
//! acknowledgements. When the host passes over a line without
//! acknowledging it (it dropped the event and may want it again), or an
//! acknowledgement is overdue, the files are read again from the oldest
//! line in flight; a repeated line keeps its event id and is taken once.
//! Each event's cursor holds, for every file, the offset before its oldest
//! line in flight, so a restart from any stored cursor misses nothing.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{File, Metadata};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use notify::{RecommendedWatcher, RecursiveMode, Watcher, recommended_watcher};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The kind this strategy registers under.
pub const TAIL_KIND: &str = "file_tail";
/// The event type of one line.
pub const LINE_EVENT_TYPE: &str = "file.line";
/// The event type of a file read again from its start.
pub const TRUNCATED_EVENT_TYPE: &str = "file.truncated";

const CURSOR_VERSION: u64 = 1;
const MAX_LINE_BYTES_CAP: usize = 128 * 1024;
const MIN_POLL_INTERVAL_MS: u64 = 100;
/// Bytes read from one file in one pass.
const READ_CHUNK: usize = 1024 * 1024;

/// How a line becomes event data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineFormat {
    /// Its JSON when the line is JSON, else its text (`lineEncoding` says
    /// which; a line that is not UTF-8 is base64).
    #[default]
    Auto,
    /// Always its text.
    Text,
}

/// Where a file present at a first start, with no stored position, is read
/// from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartAt {
    /// Its end: only lines written from now on.
    #[default]
    End,
    /// Its start: every line it holds.
    Beginning,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TailSpec {
    /// Files to tail: paths or glob patterns (`/var/log/app/*.jsonl`).
    paths: Vec<String>,
    #[serde(default)]
    format: LineFormat,
    #[serde(default)]
    start_at: StartAt,
    /// Longest line kept whole; a longer one is cut there and marked
    /// `truncated`. Default 65 536, at most 131 072.
    #[serde(default = "default_max_line_bytes")]
    max_line_bytes: usize,
    /// How often the files are checked when nothing wakes the tail.
    /// Default 1000 ms, at least 100.
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    /// Most files tailed at once; the most recently modified win.
    /// Default 64, at most 256.
    #[serde(default = "default_max_files")]
    max_files: usize,
    /// Most lines emitted ahead of the host's acknowledgements. Default
    /// 256, at most 1000.
    #[serde(default = "default_max_in_flight")]
    max_in_flight: usize,
    /// Bytes at the start of a file that identify it with its device and
    /// inode. Default 1024, 16 to 65 536.
    #[serde(default = "default_fingerprint_bytes")]
    fingerprint_bytes: usize,
    /// How long a renamed or removed file is still read to its end.
    /// Default 5000 ms.
    #[serde(default = "default_rotate_wait_ms")]
    rotate_wait_ms: u64,
    /// How long a line waits for its acknowledgement before it is read
    /// again. Default 30 000 ms.
    #[serde(default = "default_ack_timeout_ms")]
    ack_timeout_ms: u64,
}

fn default_max_line_bytes() -> usize {
    64 * 1024
}
fn default_poll_interval_ms() -> u64 {
    1_000
}
fn default_max_files() -> usize {
    64
}
fn default_max_in_flight() -> usize {
    256
}
fn default_fingerprint_bytes() -> usize {
    1024
}
fn default_rotate_wait_ms() -> u64 {
    5_000
}
fn default_ack_timeout_ms() -> u64 {
    30_000
}

impl TailSpec {
    fn validate(&self) -> Result<(), String> {
        if self.paths.is_empty() || self.paths.iter().any(|p| p.trim().is_empty()) {
            return Err("paths must list at least one path or glob, none empty".into());
        }
        for pattern in &self.paths {
            glob::Pattern::new(pattern).map_err(|e| format!("paths: `{pattern}`: {e}"))?;
        }
        let ranges: [(&str, usize, usize, usize); 4] = [
            ("max_line_bytes", self.max_line_bytes, 1, MAX_LINE_BYTES_CAP),
            ("max_files", self.max_files, 1, 256),
            ("max_in_flight", self.max_in_flight, 1, 1000),
            ("fingerprint_bytes", self.fingerprint_bytes, 16, 65_536),
        ];
        for (field, value, low, high) in ranges {
            if !(low..=high).contains(&value) {
                return Err(format!("{field} must be {low} to {high}, not {value}"));
            }
        }
        if self.poll_interval_ms < MIN_POLL_INTERVAL_MS {
            return Err(format!(
                "poll_interval_ms must be at least {MIN_POLL_INTERVAL_MS}"
            ));
        }
        if self.ack_timeout_ms == 0 {
            return Err("ack_timeout_ms must be greater than 0".into());
        }
        Ok(())
    }
}

/// Where the tail resumes: per file, the offset before its oldest line not
/// yet stored.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Cursor {
    v: u64,
    /// The line this event is, so an acknowledgement names it.
    seq: u64,
    files: Vec<FilePosition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct FilePosition {
    /// The path it had.
    path: String,
    /// Its identity.
    id: String,
    /// Hex hash of its first `fl` bytes.
    fp: String,
    fl: u64,
    /// The generation of its content, part of every line's event id.
    #[serde(rename = "gen")]
    generation: String,
    /// The offset to read from.
    off: u64,
    /// The bytes at `off` are the rest of a line already cut.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    skip: bool,
}

/// The identity of a file: device and inode where the platform has them.
#[cfg(unix)]
pub(crate) fn identity(meta: &Metadata) -> String {
    use std::os::unix::fs::MetadataExt as _;
    format!("{:x}:{:x}", meta.dev(), meta.ino())
}

/// The identity of a file: its creation time, where there is no inode.
#[cfg(not(unix))]
pub(crate) fn identity(meta: &Metadata) -> String {
    let created = meta
        .created()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_nanos());
    format!("c:{created:x}")
}

/// A token no other content of the file `id` had: a line's event id holds
/// it, so a file that reuses an inode, or is rewritten, repeats no id.
fn new_generation(id: &str) -> String {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut hasher = blake3::Hasher::new();
    hasher.update(id.as_bytes());
    hasher.update(&now.to_be_bytes());
    hasher.update(&std::process::id().to_be_bytes());
    hasher.update(&COUNTER.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    hasher.finalize().to_hex()[..16].to_owned()
}

/// The hash of the first `len` bytes of `file`; `None` when it is shorter or
/// unreadable.
fn fingerprint(file: &mut File, len: u64) -> Option<String> {
    let mut buf = vec![0u8; usize::try_from(len).ok()?];
    file.seek(SeekFrom::Start(0)).ok()?;
    file.read_exact(&mut buf).ok()?;
    Some(blake3::hash(&buf).to_hex()[..32].to_owned())
}

/// One file the tail reads.
struct Tracked {
    path: PathBuf,
    id: String,
    file: File,
    /// Fingerprint and the bytes it covers.
    fp: String,
    fl: u64,
    /// The generation of its content.
    generation: String,
    /// The next byte to read.
    read: u64,
    /// The bytes at `read` are the rest of a line already cut.
    skip: bool,
    /// The path no longer leads here: read on until this moment.
    retiring_until: Option<Instant>,
}

impl Tracked {
    fn open(path: &Path, fingerprint_bytes: usize) -> Option<Self> {
        let mut file = File::open(path).ok()?;
        let meta = file.metadata().ok()?;
        if !meta.is_file() {
            return None;
        }
        let fl = meta.len().min(fingerprint_bytes as u64);
        let fp = fingerprint(&mut file, fl)?;
        let id = identity(&meta);
        Some(Self {
            path: path.to_owned(),
            generation: new_generation(&id),
            id,
            file,
            fp,
            fl,
            read: 0,
            skip: false,
            retiring_until: None,
        })
    }

    /// Whether the file holds fewer bytes than were read, or other bytes
    /// where its fingerprint was: truncated or rewritten in place.
    fn rewritten(&mut self, len: u64) -> bool {
        len < self.read
            || len < self.fl
            || fingerprint(&mut self.file, self.fl).as_deref() != Some(self.fp.as_str())
    }
}

/// A line emitted and not yet acknowledged.
#[derive(Debug, Clone)]
struct InFlight {
    seq: u64,
    id: String,
    start: u64,
    at: Instant,
}

/// One line read from a file.
struct Line {
    start: u64,
    end: u64,
    bytes: Vec<u8>,
    cut: bool,
}

/// What wakes the tail thread.
enum Wake {
    /// The host is done with line `seq`.
    Ack(u64),
    /// Something under a watched directory changed.
    Changed,
    Stop,
}

/// Boxed behind the opaque [`WatchHandleBox`].
struct TailState {
    wake: Sender<Wake>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    _watcher: Option<RecommendedWatcher>,
}

/// The tail loop of one watch.
struct Tail {
    spec: TailSpec,
    uri: String,
    emit: Box<dyn Fn(&str) + Send + Sync + 'static>,
    files: BTreeMap<String, Tracked>,
    in_flight: VecDeque<InFlight>,
    next_seq: u64,
    /// Positions from a stored cursor, by identity, not yet matched.
    resume: BTreeMap<String, FilePosition>,
    /// The first discovery of a start with no stored position.
    fresh: bool,
}

impl Tail {
    /// Find the files, when `discover`, then read new lines up to the
    /// window and emit them.
    fn pass(&mut self, discover: bool) {
        if discover {
            self.discover();
        }
        let ids: Vec<String> = self.files.keys().cloned().collect();
        for id in ids {
            if self.in_flight.len() >= self.spec.max_in_flight {
                break;
            }
            self.read_file(&id);
        }
        let now = Instant::now();
        let retired: Vec<String> = self
            .files
            .iter()
            .filter(|(_, f)| f.retiring_until.is_some_and(|until| until <= now))
            .map(|(id, _)| id.clone())
            .collect();
        for id in retired {
            self.files.remove(&id);
            self.in_flight.retain(|line| line.id != id);
        }
    }

    /// Track every file the globs match, by identity.
    fn discover(&mut self) {
        let mut found: Vec<(PathBuf, SystemTime)> = Vec::new();
        for pattern in &self.spec.paths {
            let Ok(paths) = glob::glob(pattern) else {
                continue;
            };
            for path in paths.flatten() {
                if let Ok(meta) = std::fs::metadata(&path)
                    && meta.is_file()
                {
                    found.push((path, meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)));
                }
            }
        }
        found.sort_by(|a, b| a.0.cmp(&b.0));
        found.dedup_by(|a, b| a.0 == b.0);
        found.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        if found.len() > self.spec.max_files {
            tracing::warn!(
                uri = %self.uri,
                matched = found.len(),
                max_files = self.spec.max_files,
                "file_tail: more files match than max_files; the most recently modified are tailed"
            );
            found.truncate(self.spec.max_files);
        }
        let mut seen = BTreeSet::new();
        for (path, _) in found {
            let Some(mut opened) = Tracked::open(&path, self.spec.fingerprint_bytes) else {
                continue;
            };
            seen.insert(opened.id.clone());
            if let Some(tracked) = self.files.get_mut(&opened.id) {
                tracked.path = path;
                tracked.retiring_until = None;
                continue;
            }
            let len = opened.file.metadata().map_or(0, |m| m.len());
            match self.resume.remove(&opened.id) {
                Some(position)
                    if position.fl <= len
                        && position.off <= len
                        && fingerprint(&mut opened.file, position.fl).as_deref()
                            == Some(position.fp.as_str()) =>
                {
                    opened.read = position.off;
                    opened.skip = position.skip;
                    opened.generation = position.generation;
                }
                // Present at a first start: where the spec says.
                _ if self.fresh => {
                    opened.read = match self.spec.start_at {
                        StartAt::End => len,
                        StartAt::Beginning => 0,
                    };
                }
                // Appeared since, or no longer the file the cursor knew:
                // every line is new.
                _ => opened.read = 0,
            }
            if opened.read > 0 && opened.fl < self.spec.fingerprint_bytes as u64 {
                // Fingerprint all that is there, so a later rewrite of
                // these bytes shows.
                let fl = len.min(self.spec.fingerprint_bytes as u64);
                if let Some(fp) = fingerprint(&mut opened.file, fl) {
                    opened.fp = fp;
                    opened.fl = fl;
                }
            }
            self.files.insert(opened.id.clone(), opened);
        }
        self.fresh = false;
        let rotate_wait = Duration::from_millis(self.spec.rotate_wait_ms);
        for (id, tracked) in &mut self.files {
            if !seen.contains(id) && tracked.retiring_until.is_none() {
                tracked.retiring_until = Some(Instant::now() + rotate_wait);
            }
        }
    }

    /// Read the lines of file `id` after its read offset, as many as the
    /// window allows, holding back a partial last line, and emit them.
    fn read_file(&mut self, id: &str) {
        let max_line = self.spec.max_line_bytes;
        let fingerprint_bytes = self.spec.fingerprint_bytes as u64;
        let room = self.spec.max_in_flight.saturating_sub(self.in_flight.len());
        let Some(tracked) = self.files.get_mut(id) else {
            return;
        };
        let Ok(len) = tracked.file.metadata().map(|m| m.len()) else {
            return;
        };
        if tracked.rewritten(len) {
            let fl = len.min(fingerprint_bytes);
            let Some(fp) = fingerprint(&mut tracked.file, fl) else {
                return;
            };
            tracked.read = 0;
            tracked.skip = false;
            tracked.fp = fp;
            tracked.fl = fl;
            tracked.generation = new_generation(&tracked.id);
            let path = tracked.path.display().to_string();
            self.in_flight.retain(|line| line.id != id);
            self.send(WatchEvent {
                event_type: Some(TRUNCATED_EVENT_TYPE.to_owned()),
                data: Some(serde_json::json!({ "path": path })),
                ..WatchEvent::default()
            });
            return self.read_file(id);
        }
        let want = len.min(fingerprint_bytes);
        if want > tracked.fl
            && let Some(fp) = fingerprint(&mut tracked.file, want)
        {
            tracked.fp = fp;
            tracked.fl = want;
        }
        if tracked.read >= len || room == 0 {
            return;
        }
        let size = usize::try_from(len - tracked.read)
            .unwrap_or(READ_CHUNK)
            .min(READ_CHUNK);
        let mut chunk = vec![0u8; size];
        if tracked.file.seek(SeekFrom::Start(tracked.read)).is_err() {
            return;
        }
        let Ok(got) = tracked.file.read(&mut chunk) else {
            return;
        };
        chunk.truncate(got);
        let base = tracked.read;
        let mut at = 0usize;
        let mut lines: Vec<Line> = Vec::new();
        let mut skip = tracked.skip;
        while lines.len() < room && at < chunk.len() {
            let rest = &chunk[at..];
            let newline = rest.iter().position(|&b| b == b'\n');
            if skip {
                // The rest of a line already cut: drop it to its end.
                match newline {
                    Some(n) => {
                        at += n + 1;
                        skip = false;
                        continue;
                    }
                    None => {
                        at = chunk.len();
                        break;
                    }
                }
            }
            match newline {
                Some(n) => {
                    let mut line = &rest[..n];
                    if line.last() == Some(&b'\r') {
                        line = &line[..line.len() - 1];
                    }
                    let cut = line.len() > max_line;
                    lines.push(Line {
                        start: base + at as u64,
                        end: base + (at + n + 1) as u64,
                        bytes: line[..line.len().min(max_line)].to_vec(),
                        cut,
                    });
                    at += n + 1;
                }
                // A partial last line waits for its newline, unless it is
                // already too long to keep whole.
                None if rest.len() > max_line => {
                    lines.push(Line {
                        start: base + at as u64,
                        end: base + chunk.len() as u64,
                        bytes: rest[..max_line].to_vec(),
                        cut: true,
                    });
                    at = chunk.len();
                    skip = true;
                }
                None => break,
            }
        }
        let path = tracked.path.display().to_string();
        let last = lines.len();
        for (index, line) in lines.into_iter().enumerate() {
            if let Some(tracked) = self.files.get_mut(id) {
                tracked.read = line.end;
                tracked.skip = skip && index + 1 == last;
            }
            self.emit_line(id, &path, &line);
        }
        if let Some(tracked) = self.files.get_mut(id) {
            tracked.read = base + at as u64;
            tracked.skip = skip;
        }
    }

    fn emit_line(&mut self, id: &str, path: &str, line: &Line) {
        let (value, encoding) = match self.spec.format {
            LineFormat::Auto => mcpg_plugin_protocol::event_body(&line.bytes),
            LineFormat::Text => (
                Value::String(String::from_utf8_lossy(&line.bytes).into_owned()),
                "text",
            ),
        };
        let Some(tracked) = self.files.get(id) else {
            return;
        };
        let seq = self.next_seq;
        self.next_seq += 1;
        let mut hasher = blake3::Hasher::new();
        for part in [
            id.as_bytes(),
            tracked.generation.as_bytes(),
            &line.start.to_be_bytes(),
        ] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        let mut data = serde_json::json!({
            "path": path,
            "offset": line.start,
            "line": value,
            "lineEncoding": encoding,
        });
        if line.cut {
            data["truncated"] = Value::Bool(true);
        }
        let cursor = self.cursor(seq);
        self.in_flight.push_back(InFlight {
            seq,
            id: id.to_owned(),
            start: line.start,
            at: Instant::now(),
        });
        self.send(WatchEvent {
            event_id: Some(format!("line-{}", &hasher.finalize().to_hex()[..32])),
            event_type: Some(LINE_EVENT_TYPE.to_owned()),
            data: Some(data),
            upstream_cursor: serde_json::to_value(cursor).ok(),
            ..WatchEvent::default()
        });
    }

    /// The cursor of line `seq`, read up to its end: every file at the
    /// offset before its oldest line still in flight, else at its read
    /// offset.
    fn cursor(&self, seq: u64) -> Cursor {
        let files = self
            .files
            .values()
            .filter(|f| f.retiring_until.is_none())
            .map(|f| {
                let oldest = self
                    .in_flight
                    .iter()
                    .filter(|line| line.id == f.id)
                    .map(|line| line.start)
                    .min();
                FilePosition {
                    path: f.path.display().to_string(),
                    id: f.id.clone(),
                    fp: f.fp.clone(),
                    fl: f.fl,
                    generation: f.generation.clone(),
                    off: oldest.unwrap_or(f.read),
                    skip: oldest.is_none() && f.skip,
                }
            })
            .collect();
        Cursor {
            v: CURSOR_VERSION,
            seq,
            files,
        }
    }

    fn send(&self, event: WatchEvent) {
        if let Ok(json) = serde_json::to_string(&event) {
            (self.emit)(&json);
        }
    }

    /// The host is done with line `seq`. Lines before it that it never
    /// acknowledged were dropped: read again from them.
    fn acknowledged(&mut self, seq: u64) {
        let Some(front) = self.in_flight.front() else {
            return;
        };
        if seq < front.seq {
            return;
        }
        if seq > front.seq {
            self.rewind();
            return;
        }
        self.in_flight.pop_front();
    }

    /// Read every file again from its oldest line in flight.
    fn rewind(&mut self) {
        for line in std::mem::take(&mut self.in_flight) {
            if let Some(tracked) = self.files.get_mut(&line.id)
                && line.start < tracked.read
            {
                tracked.read = line.start;
                tracked.skip = false;
            }
        }
    }

    /// Whether the oldest line in flight waited too long.
    fn overdue(&self) -> bool {
        self.in_flight.front().is_some_and(|line| {
            line.at.elapsed() >= Duration::from_millis(self.spec.ack_timeout_ms)
        })
    }
}

fn run(mut tail: Tail, wake: Receiver<Wake>, stop: Arc<AtomicBool>) {
    let poll = Duration::from_millis(tail.spec.poll_interval_ms);
    let mut discover = true;
    loop {
        if stop.load(Ordering::Acquire) {
            return;
        }
        tail.pass(discover);
        let deadline = Instant::now() + poll;
        discover = loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let first = match wake.recv_timeout(left) {
                Ok(first) => first,
                Err(RecvTimeoutError::Timeout) => break true,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            // Take everything waiting, so a burst of acknowledgements is
            // one pass.
            let mut changed = false;
            let mut acked = false;
            let mut next = Some(first);
            while let Some(wake_up) = next {
                match wake_up {
                    Wake::Ack(seq) => {
                        tail.acknowledged(seq);
                        acked = true;
                    }
                    Wake::Changed => changed = true,
                    Wake::Stop => return,
                }
                next = match wake.try_recv() {
                    Ok(more) => Some(more),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => return,
                };
            }
            if changed {
                break true;
            }
            if acked && tail.in_flight.len() < tail.spec.max_in_flight {
                break false;
            }
        };
        if tail.overdue() {
            tracing::warn!(uri = %tail.uri, "file_tail: a line was not acknowledged in time; reading again from it");
            tail.rewind();
        }
    }
}

/// The directories to watch for a wake-up: each pattern's longest fixed
/// prefix, recursively when a directory part has a wildcard.
pub(crate) fn wake_dirs(patterns: &[String]) -> Vec<(PathBuf, RecursiveMode)> {
    patterns
        .iter()
        .map(|pattern| {
            let parts: Vec<_> = Path::new(pattern).components().collect();
            let mut fixed = PathBuf::new();
            let mut wild_dir = false;
            for (index, part) in parts.iter().enumerate() {
                let last = index + 1 == parts.len();
                if part.as_os_str().to_string_lossy().contains(['*', '?', '[']) {
                    wild_dir = !last;
                    break;
                }
                if last {
                    break;
                }
                fixed.push(part);
            }
            if fixed.as_os_str().is_empty() {
                fixed.push(".");
            }
            let mode = if wild_dir {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            (fixed, mode)
        })
        .collect()
}

/// `WatchStrategyPlugin` for `kind: "file_tail"`.
pub struct FileTailPlugin {
    manifest: PluginManifest,
}

impl FileTailPlugin {
    /// SDK factory; the files arrive per watch, so the config is ignored.
    pub fn from_config_json(_config_json: &str) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.watch.file_tail",
                name: "File Tail Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for FileTailPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        TAIL_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, resume) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: TailSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("file_tail watch spec: {e}"),
            })?;
        parsed
            .validate()
            .map_err(|message| WatchError::InvalidSpec {
                message: format!("file_tail: {message}"),
            })?;
        let resume: Option<Cursor> =
            resume
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| WatchError::InvalidSpec {
                    message: format!(
                        "file_tail: the stored position is not one this plugin wrote: {e}"
                    ),
                })?;
        let fresh = resume.is_none();
        let resume = resume
            .map(|cursor| {
                cursor
                    .files
                    .into_iter()
                    .map(|position| (position.id.clone(), position))
                    .collect()
            })
            .unwrap_or_default();
        let (wake, wakes) = channel();
        let watcher = {
            let wake = wake.clone();
            recommended_watcher(move |res: notify::Result<notify::Event>| {
                // Opening a file to read it is an access; it must not wake
                // the tail that opened it.
                if let Ok(event) = res
                    && !matches!(event.kind, notify::EventKind::Access(_))
                {
                    let _ = wake.send(Wake::Changed);
                }
            })
            .ok()
            .map(|mut watcher| {
                for (dir, mode) in wake_dirs(&parsed.paths) {
                    // A directory missing now is found by polling.
                    let _ = watcher.watch(&dir, mode);
                }
                watcher
            })
        };
        let tail = Tail {
            spec: parsed,
            uri: resource_uri.to_owned(),
            emit: emit_event,
            files: BTreeMap::new(),
            in_flight: VecDeque::new(),
            next_seq: 0,
            resume,
            fresh,
        };
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("mcpg-file-tail".into())
                .spawn(move || run(tail, wakes, stop))
                .map_err(|e| WatchError::Subscribe {
                    message: format!("file_tail: cannot start the tail thread: {e}"),
                })?
        };
        let state = Box::new(TailState {
            wake,
            stop,
            thread: Some(thread),
            _watcher: watcher,
        });
        Ok(WatchHandleBox(Box::into_raw(state) as *mut ()))
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        if watch_handle.0.is_null() {
            return;
        }
        let Some(seq) = position.get("seq").and_then(Value::as_u64) else {
            return;
        };
        // SAFETY: the pointer `watch` returned, still owned by the host,
        // which never acknowledges after `cancel`.
        let state = unsafe { &*(watch_handle.0 as *const TailState) };
        let _ = state.wake.send(Wake::Ack(seq));
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: pointer produced by `Box::into_raw` in `watch`, round-tripped
        // by the host exactly once.
        let mut state = unsafe { Box::from_raw(watch_handle.0 as *mut TailState) };
        state.stop.store(true, Ordering::Release);
        let _ = state.wake.send(Wake::Stop);
        if let Some(thread) = state.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
#[path = "tail_tests.rs"]
mod tests;
