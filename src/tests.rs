use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

use notify::EventKind;
use notify::event::{AccessKind, CreateKind, ModifyKind, RemoveKind};

use mcpg_plugin_protocol::backend::WatchError;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use serde_json::{Value, json};

use super::{
    FileEventKind, FileWatchPlugin, PLUGIN_ID, WATCH_KIND, default_event_kinds, event_is_relevant,
};

type Sink = Arc<Mutex<Vec<String>>>;

fn plugin() -> FileWatchPlugin {
    FileWatchPlugin::from_config_json("{}")
}
fn sink() -> Sink {
    Arc::new(Mutex::new(Vec::new()))
}
fn emit_for(s: &Sink) -> Box<dyn Fn(&str) + Send + Sync + 'static> {
    let s = Arc::clone(s);
    Box::new(move |ev: &str| s.lock().unwrap().push(ev.to_owned()))
}
fn count(s: &Sink) -> usize {
    s.lock().unwrap().len()
}
fn watch(p: &FileWatchPlugin, spec: Value, s: &Sink) -> Result<WatchHandleBox, WatchError> {
    p.watch("res://x", &spec, emit_for(s))
}

fn unique_dir(tag: &str) -> PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("mcpg-watchfile-{}-{n}-{tag}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn wait_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        sleep(Duration::from_millis(50));
    }
    cond()
}

// --- pure event-filter logic (deterministic, no filesystem) -----------------

#[test]
fn relevant_matches_default_kinds() {
    let d = default_event_kinds();
    assert!(event_is_relevant(&EventKind::Create(CreateKind::Any), &d));
    assert!(event_is_relevant(&EventKind::Modify(ModifyKind::Any), &d));
    assert!(event_is_relevant(&EventKind::Remove(RemoveKind::Any), &d));
    // Access is NOT in the default set.
    assert!(!event_is_relevant(&EventKind::Access(AccessKind::Any), &d));
    // `Other` never ticks; `Any` ticks when watching anything.
    assert!(!event_is_relevant(&EventKind::Other, &d));
    assert!(event_is_relevant(&EventKind::Any, &d));
}

#[test]
fn relevant_honours_custom_kinds() {
    let only_access = vec![FileEventKind::Access];
    assert!(event_is_relevant(
        &EventKind::Access(AccessKind::Any),
        &only_access
    ));
    assert!(!event_is_relevant(
        &EventKind::Create(CreateKind::Any),
        &only_access
    ));
    // Empty allow-list → nothing (incl. imprecise `Any`).
    assert!(!event_is_relevant(&EventKind::Any, &[]));
}

// --- config / spec validation ----------------------------------------------

#[test]
fn manifest_and_kind_are_correct() {
    use mcpg_plugin_protocol::PluginClass;
    let p = plugin();
    let m = SyncWatchStrategyPlugin::manifest(&p);
    assert_eq!(m.id, PLUGIN_ID);
    assert_eq!(m.plugin_class, PluginClass::WatchStrategy);
    assert_eq!(p.kind(), WATCH_KIND);
    assert!(m.required_capabilities.is_empty());
}

#[test]
fn missing_path_is_invalid_spec() {
    let p = plugin();
    let s = sink();
    assert!(matches!(
        watch(&p, json!({}), &s),
        Err(WatchError::InvalidSpec { .. })
    ));
}

#[test]
fn empty_path_is_invalid_spec() {
    let p = plugin();
    let s = sink();
    assert!(matches!(
        watch(&p, json!({ "path": "  " }), &s),
        Err(WatchError::InvalidSpec { .. })
    ));
}

#[test]
fn unknown_spec_field_is_invalid_spec() {
    let p = plugin();
    let s = sink();
    assert!(matches!(
        watch(&p, json!({ "path": "/tmp", "bogus": 1 }), &s),
        Err(WatchError::InvalidSpec { .. })
    ));
}

#[test]
fn nonexistent_path_is_subscribe_error() {
    let p = plugin();
    let s = sink();
    let missing = format!("/nonexistent/mcpg-watchfile-{}", std::process::id());
    assert!(matches!(
        watch(&p, json!({ "path": missing }), &s),
        Err(WatchError::Subscribe { .. })
    ));
}

#[test]
fn cancel_null_handle_is_safe() {
    plugin().cancel(WatchHandleBox(std::ptr::null_mut()));
}

// --- functional: real inotify round-trip ------------------------------------

#[test]
fn file_change_emits_tick_and_cancel_stops() {
    let dir = unique_dir("basic");
    let p = plugin();
    let s = sink();
    let handle = watch(&p, json!({ "path": dir.to_str().unwrap() }), &s).unwrap();

    // Give the native watcher a moment to arm before mutating.
    sleep(Duration::from_millis(200));
    std::fs::write(dir.join("a.txt"), b"hello").unwrap();

    assert!(
        wait_until(|| count(&s) >= 1, Duration::from_secs(3)),
        "a file write should produce at least one tick"
    );

    // Cancel drops the native watcher (synchronous stop).
    p.cancel(handle);
    sleep(Duration::from_millis(300));
    let after_cancel = count(&s);

    // Writes after cancel must not produce further ticks.
    std::fs::write(dir.join("b.txt"), b"more").unwrap();
    std::fs::write(dir.join("c.txt"), b"more2").unwrap();
    sleep(Duration::from_millis(700));
    assert_eq!(
        count(&s),
        after_cancel,
        "no ticks should arrive after cancel"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
