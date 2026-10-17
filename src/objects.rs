//! `file_objects`: each file the globs match that was created, modified or
//! deleted is one `object.changed` event, the shape every inventory
//! strategy shares.
//!
//! The listing is polled every `interval_ms`; a change under a watched
//! directory lists at once. A file's key is its path relative to the
//! patterns' common directory, and its version is its identity (device and
//! inode), so a file replaced under the same name is a modification even
//! when its size and time match.

use std::path::{Path, PathBuf};
use std::time::Duration;

use mcpg_plugin_protocol::backend::WatchError;
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::inventory::{
    InventoryOptions, InventoryStart, ObjectEntry, acknowledge_inventory_watch,
    cancel_inventory_watch, spawn_inventory_watch_with_waker,
};
use notify::{RecommendedWatcher, Watcher, recommended_watcher};
use serde::Deserialize;
use serde_json::Value;

/// The kind this strategy registers under.
pub const OBJECTS_KIND: &str = "file_objects";
/// Most files listed; past it the listing is cut, with a warning.
const MAX_FILES_CAP: usize = 100_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectsSpec {
    /// Files to watch: paths or glob patterns (`/data/inbox/**/*.csv`).
    paths: Vec<String>,
    /// Milliseconds between listings. Default 5000, at least 250.
    #[serde(default = "default_interval_ms")]
    interval_ms: u64,
    /// `now` (default): the files there at a first start are the baseline;
    /// `existing`: each is reported as created.
    #[serde(default)]
    start_at: InventoryStart,
    /// Largest snapshot of the listing an event carries, so a restart is
    /// exact. Default 131 072, at most 196 608.
    #[serde(default = "default_snapshot_bytes")]
    snapshot_bytes: usize,
    /// Most changes emitted ahead of the gateway's acknowledgements.
    /// Default 256.
    #[serde(default = "default_max_in_flight")]
    max_in_flight: usize,
    /// How long a change waits for its acknowledgement. Default 60 000.
    #[serde(default = "default_ack_timeout_ms")]
    ack_timeout_ms: u64,
    /// Most files listed. Default 10 000, at most 100 000.
    #[serde(default = "default_max_files")]
    max_files: usize,
}

fn default_interval_ms() -> u64 {
    5_000
}
fn default_snapshot_bytes() -> usize {
    128 * 1024
}
fn default_max_in_flight() -> usize {
    256
}
fn default_ack_timeout_ms() -> u64 {
    60_000
}
fn default_max_files() -> usize {
    10_000
}

/// The longest directory every pattern starts with.
fn common_root(patterns: &[String]) -> PathBuf {
    let fixed: Vec<PathBuf> = super::tail::wake_dirs(patterns)
        .into_iter()
        .map(|(dir, _)| dir)
        .collect();
    let mut root = fixed.first().cloned().unwrap_or_default();
    for dir in &fixed[1..] {
        while !dir.starts_with(&root) {
            if !root.pop() {
                return PathBuf::new();
            }
        }
    }
    root
}

/// Every file the patterns match, keyed relative to `root`.
fn list(patterns: &[String], root: &Path, max_files: usize) -> Result<Vec<ObjectEntry>, String> {
    let mut entries = Vec::new();
    for pattern in patterns {
        let paths = glob::glob(pattern).map_err(|e| format!("`{pattern}`: {e}"))?;
        for path in paths.flatten() {
            let Ok(meta) = std::fs::metadata(&path) else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            let key = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string();
            entries.push(ObjectEntry {
                key,
                size: Some(meta.len()),
                modified_at_ms: meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .and_then(|d| i64::try_from(d.as_millis()).ok()),
                etag: None,
                version: Some(super::tail::identity(&meta)),
            });
        }
    }
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    entries.dedup_by(|a, b| a.key == b.key);
    if entries.len() > max_files {
        tracing::warn!(
            matched = entries.len(),
            max_files,
            "file_objects: more files match than max_files; the listing is cut"
        );
        entries.truncate(max_files);
    }
    Ok(entries)
}

/// Boxed behind the opaque [`WatchHandleBox`]: the inventory watch and the
/// notifier that wakes it.
struct ObjectsState {
    inner: WatchHandleBox,
    _watcher: Option<RecommendedWatcher>,
}

/// `WatchStrategyPlugin` for `kind: "file_objects"`.
pub struct FileObjectsPlugin {
    manifest: PluginManifest,
}

impl FileObjectsPlugin {
    /// SDK factory; the files arrive per watch, so the config is ignored.
    pub fn from_config_json(_config_json: &str) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.watch.file_objects",
                name: "File Objects Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for FileObjectsPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        OBJECTS_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, resume) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: ObjectsSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("file_objects watch spec: {e}"),
            })?;
        let invalid = |message: String| WatchError::InvalidSpec {
            message: format!("file_objects: {message}"),
        };
        if parsed.paths.is_empty() || parsed.paths.iter().any(|p| p.trim().is_empty()) {
            return Err(invalid(
                "paths must list at least one path or glob, none empty".into(),
            ));
        }
        for pattern in &parsed.paths {
            glob::Pattern::new(pattern).map_err(|e| invalid(format!("paths: `{pattern}`: {e}")))?;
        }
        if !(1..=MAX_FILES_CAP).contains(&parsed.max_files) {
            return Err(invalid(format!("max_files must be 1 to {MAX_FILES_CAP}")));
        }
        let root = common_root(&parsed.paths);
        let options = InventoryOptions {
            store: "file".into(),
            location: root.display().to_string(),
            interval: Duration::from_millis(parsed.interval_ms),
            start: parsed.start_at,
            snapshot_bytes: parsed.snapshot_bytes,
            max_in_flight: parsed.max_in_flight,
            ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
        };
        let patterns = parsed.paths.clone();
        let max_files = parsed.max_files;
        let list_root = root.clone();
        let (inner, waker) = spawn_inventory_watch_with_waker(
            resource_uri,
            options,
            resume,
            emit_event,
            move || list(&patterns, &list_root, max_files),
        )?;
        let watcher = recommended_watcher(move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res
                && !matches!(event.kind, notify::EventKind::Access(_))
            {
                waker.wake();
            }
        })
        .ok()
        .map(|mut watcher| {
            for (dir, mode) in super::tail::wake_dirs(&parsed.paths) {
                // A directory missing now is found by the next listing.
                let _ = watcher.watch(&dir, mode);
            }
            watcher
        });
        let state = Box::new(ObjectsState {
            inner,
            _watcher: watcher,
        });
        Ok(WatchHandleBox(Box::into_raw(state) as *mut ()))
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: the pointer `watch` returned, still owned by the host,
        // which never acknowledges after `cancel`.
        let state = unsafe { &*(watch_handle.0 as *const ObjectsState) };
        acknowledge_inventory_watch(&state.inner, position);
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: pointer produced by `Box::into_raw` in `watch`, round-tripped
        // by the host exactly once. The notifier stops before the watch.
        let state = unsafe { Box::from_raw(watch_handle.0 as *mut ObjectsState) };
        let ObjectsState {
            inner,
            _watcher: watcher,
        } = *state;
        drop(watcher);
        cancel_inventory_watch(inner);
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
