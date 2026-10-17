use super::*;
use mcpg_plugin_protocol::backend::WatchEvent;
use std::sync::{Arc, Mutex};

type Emitted = Arc<Mutex<Vec<WatchEvent>>>;

fn start(spec: Value) -> (FileObjectsPlugin, WatchHandleBox, Emitted) {
    let plugin = FileObjectsPlugin::from_config_json("{}");
    let emitted: Emitted = Arc::default();
    let sink = Arc::clone(&emitted);
    let handle = plugin
        .watch(
            "mcpg-event://inbox",
            &spec,
            Box::new(move |json| {
                sink.lock()
                    .unwrap()
                    .push(serde_json::from_str(json).unwrap());
            }),
        )
        .unwrap();
    (plugin, handle, emitted)
}

fn wait(emitted: &Emitted, n: usize) -> bool {
    for _ in 0..150 {
        if emitted.lock().unwrap().len() >= n {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

fn changes(emitted: &Emitted) -> Vec<(String, String)> {
    emitted
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.event_type.is_some())
        .map(|e| {
            let data = e.data.as_ref().unwrap();
            (
                data["op"].as_str().unwrap().to_owned(),
                data["key"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn ack_all(plugin: &FileObjectsPlugin, handle: &WatchHandleBox, emitted: &Emitted) {
    for event in emitted.lock().unwrap().iter() {
        if let Some(position) = &event.upstream_cursor {
            plugin.acknowledge(handle, position);
        }
    }
}

fn op(op: &str, key: &str) -> (String, String) {
    (op.to_owned(), key.to_owned())
}

#[test]
fn files_created_modified_and_deleted_in_a_folder_are_object_changes() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("old.csv"), "x").unwrap();
    let (plugin, handle, emitted) = start(serde_json::json!({
        "paths": [dir.path().join("*.csv").display().to_string()],
        "interval_ms": 250,
    }));
    std::thread::sleep(Duration::from_millis(150));
    std::fs::write(dir.path().join("new.csv"), "abc").unwrap();
    std::fs::write(dir.path().join("skip.txt"), "no").unwrap();
    assert!(wait(&emitted, 1));
    assert_eq!(changes(&emitted), [op("created", "new.csv")]);
    let event = emitted.lock().unwrap()[0].clone();
    let data = event.data.unwrap();
    assert_eq!(data["store"], "file");
    assert_eq!(data["location"], dir.path().display().to_string());
    assert_eq!(data["size"], 3);
    assert!(data["modifiedAt"].is_string());
    assert!(data["version"].is_string());
    ack_all(&plugin, &handle, &emitted);

    std::fs::write(dir.path().join("new.csv"), "abcdef").unwrap();
    assert!(wait(&emitted, 2));
    ack_all(&plugin, &handle, &emitted);
    std::fs::remove_file(dir.path().join("old.csv")).unwrap();
    assert!(wait(&emitted, 3));
    plugin.cancel(handle);
    assert_eq!(
        changes(&emitted),
        [
            op("created", "new.csv"),
            op("modified", "new.csv"),
            op("deleted", "old.csv")
        ]
    );
}

#[test]
fn a_restart_reports_what_changed_in_the_folder_while_stopped() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.json"), "1").unwrap();
    let spec = serde_json::json!({
        "paths": [dir.path().join("**/*.json").display().to_string()],
        "interval_ms": 250,
    });
    let (plugin, handle, emitted) = start(spec.clone());
    std::thread::sleep(Duration::from_millis(150));
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    std::fs::write(dir.path().join("sub/b.json"), "2").unwrap();
    assert!(wait(&emitted, 1));
    ack_all(&plugin, &handle, &emitted);
    std::thread::sleep(Duration::from_millis(100));
    plugin.cancel(handle);
    let stored = emitted
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .upstream_cursor
        .clone()
        .unwrap();
    assert_eq!(changes(&emitted), [op("created", "sub/b.json")]);

    // Stopped: a deleted, c created.
    std::fs::remove_file(dir.path().join("a.json")).unwrap();
    std::fs::write(dir.path().join("sub/c.json"), "3").unwrap();
    let mut resumed = spec;
    resumed[mcpg_plugin_protocol::WATCH_RESUME_SPEC_KEY] = stored;
    let (plugin, handle, emitted) = start(resumed);
    assert!(wait(&emitted, 2));
    plugin.cancel(handle);
    let mut got = changes(&emitted);
    got.sort();
    assert_eq!(got, [op("created", "sub/c.json"), op("deleted", "a.json")]);
}

#[test]
fn a_file_replaced_under_its_name_is_a_modification() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.bin");
    std::fs::write(&path, "same").unwrap();
    let (plugin, handle, emitted) = start(serde_json::json!({
        "paths": [path.display().to_string()],
        "interval_ms": 250,
    }));
    std::thread::sleep(Duration::from_millis(150));
    // An atomic replace: a new inode under the same name, same size.
    let staged = dir.path().join("data.bin.tmp");
    std::fs::write(&staged, "same").unwrap();
    std::fs::rename(&staged, &path).unwrap();
    assert!(wait(&emitted, 1));
    plugin.cancel(handle);
    assert_eq!(changes(&emitted), [op("modified", "data.bin")]);
}

#[test]
fn the_spec_is_checked() {
    let plugin = FileObjectsPlugin::from_config_json("{}");
    for (spec, needle) in [
        (serde_json::json!({ "paths": [] }), "at least one path"),
        (serde_json::json!({ "paths": ["/x/[a"] }), "paths: `/x/[a`"),
        (
            serde_json::json!({ "paths": ["/x"], "interval_ms": 10 }),
            "interval_ms",
        ),
        (
            serde_json::json!({ "paths": ["/x"], "max_files": 0 }),
            "max_files",
        ),
        (
            serde_json::json!({ "paths": ["/x"], "snapshot_bytes": 999999 }),
            "snapshot_bytes",
        ),
        (
            serde_json::json!({ "paths": ["/x"], "typo": 1 }),
            "unknown field",
        ),
    ] {
        let error = plugin
            .watch("mcpg-event://x", &spec, Box::new(|_| {}))
            .err()
            .unwrap();
        let WatchError::InvalidSpec { message } = error else {
            panic!("an invalid spec");
        };
        assert!(message.contains(needle), "{spec}: {message}");
    }
}

#[test]
fn the_location_is_the_directory_every_pattern_starts_with() {
    assert_eq!(
        common_root(&["/data/in/*.csv".into(), "/data/in/sub/**/*.csv".into()]),
        PathBuf::from("/data/in")
    );
    assert_eq!(
        common_root(&["/data/a/*.csv".into(), "/data/b/*.csv".into()]),
        PathBuf::from("/data")
    );
    assert_eq!(common_root(&["/x.log".into()]), PathBuf::from("/"));
}
