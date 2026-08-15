//! Filesystem `watch_strategy` plugin (`dev.mcpg.watch.file`).
//!
//! Emits a resource-change tick when a watched path (file or directory)
//! changes — create / modify / remove by default. Backed by `notify`
//! (inotify / kqueue / ReadDirectoryChanges). Each watcher owns a native
//! watcher whose background thread invokes the host's `emit_event`; the host
//! cancels by dropping the handle, which stops the native watcher
//! synchronously. No network.
//!
//! The per-watch `spec` is `{ "path": "/data/config.json", "recursive": false,
//! "event_kinds": ["create","modify","remove"] }`.

use std::path::Path;
use std::sync::Arc;

use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher, recommended_watcher};
use serde::Deserialize;
use serde_json::Value;

const PLUGIN_ID: &str = "dev.mcpg.watch.file";
const WATCH_KIND: &str = "file";

/// Which categories of filesystem event emit a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileEventKind {
    Create,
    Modify,
    Remove,
    Access,
}

fn default_event_kinds() -> Vec<FileEventKind> {
    vec![
        FileEventKind::Create,
        FileEventKind::Modify,
        FileEventKind::Remove,
    ]
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// Path to watch (file or directory).
    path: String,
    /// Recurse into subdirectories (directory paths only).
    #[serde(default)]
    recursive: bool,
    /// Event categories that emit a tick. Default: create/modify/remove.
    #[serde(default = "default_event_kinds")]
    event_kinds: Vec<FileEventKind>,
}

/// Does this notify event match the operator's selected categories?
fn event_is_relevant(kind: &EventKind, allowed: &[FileEventKind]) -> bool {
    match kind {
        EventKind::Create(_) => allowed.contains(&FileEventKind::Create),
        EventKind::Modify(_) => allowed.contains(&FileEventKind::Modify),
        EventKind::Remove(_) => allowed.contains(&FileEventKind::Remove),
        EventKind::Access(_) => allowed.contains(&FileEventKind::Access),
        // Imprecise backends collapse everything to `Any`; emit if the operator
        // is watching for anything at all.
        EventKind::Any => !allowed.is_empty(),
        EventKind::Other => false,
    }
}

/// Boxed behind the opaque [`WatchHandleBox`]; dropping it stops the native
/// watcher (and its background thread) synchronously.
struct FileWatchState {
    _watcher: RecommendedWatcher,
}

pub struct FileWatchPlugin {
    manifest: PluginManifest,
}

impl FileWatchPlugin {
    /// SDK factory. No plugin-level config (the path arrives per-watch), so the
    /// config JSON is ignored.
    pub fn from_config_json(_config_json: &str) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: PLUGIN_ID,
                name: "Filesystem Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for FileWatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        _resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let parsed: WatchSpec =
            serde_json::from_value(spec.clone()).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid watch spec: {e}"),
            })?;
        if parsed.path.trim().is_empty() {
            return Err(WatchError::InvalidSpec {
                message: "path must not be empty".to_owned(),
            });
        }

        let allowed = parsed.event_kinds.clone();
        let tick =
            serde_json::to_string(&WatchEvent::default()).unwrap_or_else(|_| "{}".to_owned());
        // notify's handler is FnMut; share the host's `Fn` callback behind an Arc.
        let emit = Arc::new(emit_event);

        let mut watcher = recommended_watcher(move |res: notify::Result<Event>| {
            if let Ok(event) = res
                && event_is_relevant(&event.kind, &allowed)
            {
                emit(&tick);
            }
        })
        .map_err(|e| WatchError::Subscribe {
            message: format!("failed to create filesystem watcher: {e}"),
        })?;

        let mode = if parsed.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        watcher
            .watch(Path::new(&parsed.path), mode)
            .map_err(|e| WatchError::Subscribe {
                message: format!("failed to watch path `{}`: {e}", parsed.path),
            })?;

        let state = Box::new(FileWatchState { _watcher: watcher });
        Ok(WatchHandleBox(Box::into_raw(state) as *mut ()))
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: pointer produced by `Box::into_raw` in `watch`, round-tripped
        // by the host exactly once. Dropping the state drops the native watcher,
        // which stops its background thread.
        let _state = unsafe { Box::from_raw(watch_handle.0 as *mut FileWatchState) };
    }
}

mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.watch.file",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[],
    entities: [
        watch_strategy as watch {
            inner_name: "",
            plugin_type: FileWatchPlugin,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| FileWatchPlugin::from_config_json(cfg),
        },
    ],
}

#[cfg(test)]
mod tests;
