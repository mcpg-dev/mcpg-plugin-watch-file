use super::*;
use std::io::Write as _;
use std::sync::Mutex;

/// Every event a tail emitted, decoded.
type Emitted = Arc<Mutex<Vec<WatchEvent>>>;

fn spec(paths: &[&Path], extra: Value) -> TailSpec {
    let mut value = serde_json::json!({
        "paths": paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
        "start_at": "beginning",
    });
    for (k, v) in extra.as_object().unwrap() {
        value[k] = v.clone();
    }
    let spec: TailSpec = serde_json::from_value(value).unwrap();
    spec.validate().unwrap();
    spec
}

fn tail(spec: TailSpec, resume: Option<Cursor>) -> (Tail, Emitted) {
    let emitted: Emitted = Arc::default();
    let sink = Arc::clone(&emitted);
    let fresh = resume.is_none();
    let tail = Tail {
        spec,
        uri: "mcpg-event://logs".into(),
        emit: Box::new(move |json| {
            sink.lock()
                .unwrap()
                .push(serde_json::from_str(json).unwrap());
        }),
        files: BTreeMap::new(),
        in_flight: VecDeque::new(),
        next_seq: 0,
        resume: resume
            .map(|c| c.files.into_iter().map(|p| (p.id.clone(), p)).collect())
            .unwrap_or_default(),
        fresh,
    };
    (tail, emitted)
}

fn append(path: &Path, text: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(text.as_bytes()).unwrap();
}

fn lines(emitted: &Emitted) -> Vec<Value> {
    emitted
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.event_type.as_deref() == Some(LINE_EVENT_TYPE))
        .map(|e| e.data.as_ref().unwrap()["line"].clone())
        .collect()
}

fn last_cursor(emitted: &Emitted) -> Cursor {
    let events = emitted.lock().unwrap();
    let event = events
        .iter()
        .rev()
        .find(|e| e.upstream_cursor.is_some())
        .unwrap();
    serde_json::from_value(event.upstream_cursor.clone().unwrap()).unwrap()
}

/// Acknowledge every line in flight, as a host that stored them would.
fn ack_all(tail: &mut Tail) {
    while let Some(front) = tail.in_flight.front() {
        let seq = front.seq;
        tail.acknowledged(seq);
    }
}

#[test]
fn the_spec_is_checked() {
    let dir = tempfile::tempdir().unwrap();
    let base = serde_json::json!({ "paths": [dir.path().join("a.log").display().to_string()] });
    let cases = [
        (serde_json::json!({ "paths": [] }), "at least one path"),
        (serde_json::json!({ "paths": [""] }), "none empty"),
        (serde_json::json!({ "paths": ["/x/[a"] }), "paths: `/x/[a`"),
        (
            serde_json::json!({ "max_line_bytes": 0 }),
            "max_line_bytes must be 1 to 131072",
        ),
        (
            serde_json::json!({ "max_line_bytes": 200000 }),
            "max_line_bytes",
        ),
        (serde_json::json!({ "max_files": 0 }), "max_files"),
        (
            serde_json::json!({ "max_in_flight": 1001 }),
            "max_in_flight",
        ),
        (
            serde_json::json!({ "fingerprint_bytes": 8 }),
            "fingerprint_bytes",
        ),
        (
            serde_json::json!({ "poll_interval_ms": 10 }),
            "poll_interval_ms",
        ),
        (serde_json::json!({ "ack_timeout_ms": 0 }), "ack_timeout_ms"),
    ];
    for (extra, needle) in cases {
        let mut value = base.clone();
        for (k, v) in extra.as_object().unwrap() {
            value[k] = v.clone();
        }
        let spec: TailSpec = serde_json::from_value(value).unwrap();
        let error = spec.validate().unwrap_err();
        assert!(error.contains(needle), "{extra}: {error}");
    }
    let unknown = serde_json::json!({ "paths": ["/x"], "typo": 1 });
    assert!(serde_json::from_value::<TailSpec>(unknown).is_err());
}

#[test]
fn each_complete_line_is_one_event_and_a_partial_one_waits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.jsonl");
    append(&path, "{\"n\":1}\nplain text\r\n{\"n\":");
    let (mut t, emitted) = tail(spec(&[&path], serde_json::json!({})), None);
    t.pass(true);
    assert_eq!(
        lines(&emitted),
        [
            serde_json::json!({ "n": 1 }),
            serde_json::json!("plain text")
        ]
    );
    let events = emitted.lock().unwrap().clone();
    assert_eq!(events[0].data.as_ref().unwrap()["lineEncoding"], "json");
    assert_eq!(events[1].data.as_ref().unwrap()["lineEncoding"], "text");
    assert_eq!(events[1].data.as_ref().unwrap()["offset"], 8);
    assert!(events[0].event_id.as_ref().unwrap().starts_with("line-"));

    append(&path, "2}\n");
    ack_all(&mut t);
    t.pass(false);
    assert_eq!(lines(&emitted).last(), Some(&serde_json::json!({ "n": 2 })));
}

#[test]
fn text_format_never_parses_and_a_long_line_is_cut_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    let long = "x".repeat(40);
    append(&path, &format!("200\n{long}\nnext\n"));
    let (mut t, emitted) = tail(
        spec(
            &[&path],
            serde_json::json!({ "format": "text", "max_line_bytes": 16 }),
        ),
        None,
    );
    t.pass(true);
    let events = emitted.lock().unwrap().clone();
    let data: Vec<&Value> = events.iter().map(|e| e.data.as_ref().unwrap()).collect();
    assert_eq!(
        data[0]["line"], "200",
        "text keeps a number-looking line as text"
    );
    assert_eq!(data[1]["line"], "x".repeat(16));
    assert_eq!(data[1]["truncated"], true);
    assert_eq!(data[2]["line"], "next");
    assert!(data[2].get("truncated").is_none());
}

#[test]
fn a_partial_line_past_the_limit_is_cut_and_its_rest_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &"y".repeat(30));
    let (mut t, emitted) = tail(
        spec(
            &[&path],
            serde_json::json!({ "format": "text", "max_line_bytes": 10 }),
        ),
        None,
    );
    t.pass(true);
    assert_eq!(lines(&emitted), [serde_json::json!("y".repeat(10))]);
    // The writer finishes the long line, then writes another.
    append(&path, &format!("{}\nafter\n", "y".repeat(5)));
    ack_all(&mut t);
    t.pass(false);
    assert_eq!(
        lines(&emitted),
        [
            serde_json::json!("y".repeat(10)),
            serde_json::json!("after")
        ],
        "the rest of a cut line is no line of its own"
    );
}

#[test]
fn the_window_holds_lines_until_the_host_acknowledges() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, "a\nb\nc\nd\ne\n");
    let (mut t, emitted) = tail(
        spec(&[&path], serde_json::json!({ "max_in_flight": 2 })),
        None,
    );
    t.pass(true);
    assert_eq!(lines(&emitted), ["a", "b"]);
    t.pass(false);
    assert_eq!(
        lines(&emitted).len(),
        2,
        "nothing more while the window is full"
    );
    t.acknowledged(0);
    t.pass(false);
    assert_eq!(lines(&emitted), ["a", "b", "c"]);
}

#[test]
fn a_line_the_host_passed_over_is_read_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, "a\nb\nc\n");
    let (mut t, emitted) = tail(spec(&[&path], serde_json::json!({})), None);
    t.pass(true);
    let first: Vec<WatchEvent> = emitted.lock().unwrap().clone();
    // The host stored `a` and `c`; `b` it dropped and never acknowledged.
    t.acknowledged(0);
    t.acknowledged(2);
    t.pass(false);
    let all = emitted.lock().unwrap().clone();
    assert_eq!(lines(&emitted), ["a", "b", "c", "b", "c"]);
    assert_eq!(
        all[3].event_id, first[1].event_id,
        "the line read again keeps its id, so the host takes it once"
    );
    assert_eq!(all[4].event_id, first[2].event_id);
}

#[test]
fn an_overdue_acknowledgement_reads_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, "a\n");
    let (mut t, emitted) = tail(
        spec(&[&path], serde_json::json!({ "ack_timeout_ms": 1 })),
        None,
    );
    t.pass(true);
    std::thread::sleep(Duration::from_millis(5));
    assert!(t.overdue());
    t.rewind();
    t.pass(false);
    assert_eq!(lines(&emitted), ["a", "a"]);
}

#[test]
fn a_cursor_never_passes_a_line_still_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, "a\nbb\nccc\n");
    let (mut t, emitted) = tail(spec(&[&path], serde_json::json!({})), None);
    t.pass(true);
    let cursors: Vec<Cursor> = emitted
        .lock()
        .unwrap()
        .iter()
        .map(|e| serde_json::from_value(e.upstream_cursor.clone().unwrap()).unwrap())
        .collect();
    // Line 0 resumes after itself; lines 1 and 2 resume at line 0, still in
    // flight when they were emitted.
    assert_eq!(cursors[0].files[0].off, 2);
    assert_eq!(cursors[1].files[0].off, 0);
    assert_eq!(cursors[2].files[0].off, 0);
    ack_all(&mut t);
    append(&path, "dddd\n");
    t.pass(false);
    assert_eq!(last_cursor(&emitted).files[0].off, 14);
}

#[test]
fn a_restart_resumes_each_file_where_it_was_stored() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.log");
    let b = dir.path().join("b.log");
    append(&a, "a1\na2\n");
    append(&b, "b1\n");
    let pattern = dir.path().join("*.log");
    let (mut t, emitted) = tail(spec(&[&pattern], serde_json::json!({})), None);
    t.pass(true);
    ack_all(&mut t);
    let stored = last_cursor(&emitted);
    assert_eq!(stored.files.len(), 2);
    let before: BTreeMap<String, Value> = emitted
        .lock()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e.event_id.clone().unwrap(),
                e.data.as_ref().unwrap()["line"].clone(),
            )
        })
        .collect();

    // Down: both grow, and a third file appears.
    append(&a, "a3\n");
    append(&b, "b2\n");
    append(&dir.path().join("c.log"), "c1\n");
    let (mut again, emitted) = tail(spec(&[&pattern], serde_json::json!({})), Some(stored));
    again.pass(true);
    let mut new = Vec::new();
    for event in emitted.lock().unwrap().iter() {
        let id = event.event_id.clone().unwrap();
        let line = event.data.as_ref().unwrap()["line"].clone();
        match before.get(&id) {
            // A line that was still in flight when the cursor was taken is
            // read again with its id, which the host takes once.
            Some(seen) => assert_eq!(seen, &line),
            None => new.push(line.as_str().unwrap().to_owned()),
        }
    }
    new.sort();
    assert_eq!(new, ["a3", "b2", "c1"], "every new line, none lost");
}

#[test]
fn a_first_start_at_the_end_reads_only_new_lines_of_every_file() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.log");
    let b = dir.path().join("b.log");
    append(&a, "old\n");
    append(&b, "old\n");
    let pattern = dir.path().join("*.log");
    let (mut t, emitted) = tail(
        spec(&[&pattern], serde_json::json!({ "start_at": "end" })),
        None,
    );
    t.pass(true);
    assert!(lines(&emitted).is_empty());
    append(&a, "new a\n");
    append(&b, "new b\n");
    t.pass(true);
    let mut got: Vec<Value> = lines(&emitted);
    got.sort_by_key(|v| v.to_string());
    assert_eq!(got, ["new a", "new b"]);
}

#[cfg(unix)]
#[test]
fn a_rename_keeps_the_position_and_the_new_file_is_read_from_its_start() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("app.log");
    append(&live, "l1\n");
    let (mut t, emitted) = tail(
        spec(&[&dir.path().join("app.log*")], serde_json::json!({})),
        None,
    );
    t.pass(true);
    ack_all(&mut t);
    // Rotation: the writer's last line lands in the renamed file, then a
    // new file starts.
    append(&live, "l2\n");
    std::fs::rename(&live, dir.path().join("app.log.1")).unwrap();
    append(&live, "l3\n");
    t.pass(true);
    ack_all(&mut t);
    t.pass(true);
    let mut got: Vec<Value> = lines(&emitted);
    got.sort_by_key(|v| v.to_string());
    assert_eq!(got, ["l1", "l2", "l3"], "no line twice, none lost");
}

#[test]
fn a_truncated_file_is_read_again_from_its_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, "one\ntwo\n");
    let (mut t, emitted) = tail(spec(&[&path], serde_json::json!({})), None);
    t.pass(true);
    ack_all(&mut t);
    // copytruncate: the same file, emptied, then written again.
    std::fs::write(&path, "three\n").unwrap();
    t.pass(true);
    let events = emitted.lock().unwrap().clone();
    let types: Vec<&str> = events
        .iter()
        .filter_map(|e| e.event_type.as_deref())
        .collect();
    assert_eq!(
        types,
        ["file.line", "file.line", "file.truncated", "file.line"]
    );
    assert_eq!(events[3].data.as_ref().unwrap()["line"], "three");
    assert_eq!(events[3].data.as_ref().unwrap()["offset"], 0);
    assert_ne!(
        events[3].event_id, events[0].event_id,
        "new content at an old offset is a new event, never a repeat"
    );
}

#[test]
fn a_new_file_matching_the_glob_is_read_from_its_start() {
    let dir = tempfile::tempdir().unwrap();
    let pattern = dir.path().join("*.jsonl");
    let (mut t, emitted) = tail(
        spec(&[&pattern], serde_json::json!({ "start_at": "end" })),
        None,
    );
    t.pass(true);
    append(&dir.path().join("new.jsonl"), "{\"a\":1}\n");
    t.pass(true);
    assert_eq!(lines(&emitted), [serde_json::json!({ "a": 1 })]);
}

#[test]
fn the_tail_runs_in_its_own_thread_until_cancelled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, "ready\n");
    let plugin = FileTailPlugin::from_config_json("{}");
    let emitted: Emitted = Arc::default();
    let sink = Arc::clone(&emitted);
    let handle = plugin
        .watch(
            "mcpg-event://logs",
            &serde_json::json!({
                "paths": [path.display().to_string()],
                "start_at": "beginning",
                "poll_interval_ms": 100,
                "max_in_flight": 1,
            }),
            Box::new(move |json| {
                sink.lock()
                    .unwrap()
                    .push(serde_json::from_str(json).unwrap());
            }),
        )
        .unwrap();
    let wait = |n: usize| {
        for _ in 0..100 {
            if emitted.lock().unwrap().len() >= n {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    };
    assert!(wait(1));
    append(&path, "second\n");
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(emitted.lock().unwrap().len(), 1, "held until acknowledged");
    let first = emitted.lock().unwrap()[0].upstream_cursor.clone().unwrap();
    plugin.acknowledge(&handle, &first);
    assert!(wait(2));
    assert_eq!(lines(&emitted), ["ready", "second"]);
    plugin.cancel(handle);
}

#[test]
fn a_stored_position_from_another_plugin_is_refused() {
    let plugin = FileTailPlugin::from_config_json("{}");
    let error = plugin
        .watch(
            "mcpg-event://logs",
            &serde_json::json!({
                "paths": ["/tmp/x.log"],
                mcpg_plugin_protocol::WATCH_RESUME_SPEC_KEY: { "v": 1, "k": [1] },
            }),
            Box::new(|_| {}),
        )
        .err()
        .unwrap();
    assert!(
        matches!(error, WatchError::InvalidSpec { message } if message.contains("stored position"))
    );
}

#[test]
fn wake_dirs_are_each_patterns_fixed_prefix() {
    let dirs = wake_dirs(&[
        "/var/log/app.log".to_owned(),
        "/var/log/*/x.log".to_owned(),
        "/srv/*.jsonl".to_owned(),
        "relative.log".to_owned(),
    ]);
    assert_eq!(
        dirs[0],
        (PathBuf::from("/var/log"), RecursiveMode::NonRecursive)
    );
    assert_eq!(
        dirs[1],
        (PathBuf::from("/var/log"), RecursiveMode::Recursive)
    );
    assert_eq!(
        dirs[2],
        (PathBuf::from("/srv"), RecursiveMode::NonRecursive)
    );
    assert_eq!(dirs[3], (PathBuf::from("."), RecursiveMode::NonRecursive));
}
