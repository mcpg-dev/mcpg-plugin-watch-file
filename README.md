# Filesystem Watch Strategy — `dev.mcpg.watch.file`

> class `watch_strategy` · `native` · package `mcpg-plugin-watch-file` · artifact `libmcpg_plugin_watch_file.so` · Apache-2.0

Two watch strategies over local files.

- **`file`** emits a resource-change tick when a watched path changes on disk,
  so an MCP gateway re-notifies subscribers of a file-backed resource the
  moment the file is created, modified, or removed. It is backed by the
  operating system's native notifier — inotify on Linux, FSEvents on macOS,
  ReadDirectoryChangesW on Windows — so there is no polling interval to tune.
  Reach for it when a resource is a file or a directory the gateway can see
  locally: a mounted config document, a generated artifact, a drop directory
  an upstream job writes into.
- **`file_tail`** emits each line appended to the files that one or more
  globs match, as `file.line` MCP Events: JSON Lines, application logs,
  audit trails. It follows renames and rotation, reads a truncated file again
  from its start, and resumes each file at its offset after a restart. See
  [Tailing files](#tailing-files-file_tail).
- **`file_objects`** reports each file that globs match that was created,
  modified or deleted, as the `object.changed` event every inventory
  strategy shares, and resumes exactly after a restart. See
  [Watching a folder](#watching-a-folder-file_objects).

## What it does
- Watches a single file or a directory, optionally recursing into
  subdirectories, and ticks when it changes.
- Filters events by category, ticking on creates, modifications, and removals
  by default, with file accesses available but off.
- Runs one native watcher per watched resource; its background thread invokes
  the gateway's emit callback directly, with no intervening poll loop.
- Stops the native watcher synchronously on cancellation, so no tick can arrive
  after cancel returns.
- Fails the watch up front rather than starting a dead watcher: an empty path or
  an unknown spec field is a spec error, and a path that cannot be watched
  because it does not exist or is not readable is a subscribe error.
- Declares no `required_capabilities` and opens no network connections.
  `file` registers a change notification on the path and never reads its
  contents; `file_tail` reads the files its globs match.

## Configuration
Loaded from the flat top-level `plugins:` list. The plugin itself takes no
instance config; the path is chosen per watched resource.

```yaml
plugins:
  - id: dev.mcpg.watch.file
    class: watch_strategy
    source:
      oci: ghcr.io/mcpg-dev/plugins/watch-file
```

Each resource that should tick on disk changes selects it under
`mcp.capabilities.resources[].watch.strategy` with the generic `type: plugin`
form, where `kind` names the plugin's watch kind — `file` — and the remaining
fields flatten into the spec passed to the plugin.

```yaml
mcp:
  capabilities:
    resources:
      - name: app-settings
        description: Application settings, re-notified whenever the file changes.
        uri: "config://app/settings"
        mime_type: application/json
        backend:
          kind: command
          command: /bin/cat
          args: ["/etc/myapp/settings.json"]
        watch:
          strategy:
            type: plugin
            kind: file
            path: /etc/myapp/settings.json
            event_kinds: [create, modify, remove]
```

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | string | *(required)* | File or directory to watch. Must be non-empty and watchable at subscribe time. |
| `recursive` | bool | `false` | Recurse into subdirectories. Applies to directory paths. |
| `event_kinds` | array of `create` \| `modify` \| `remove` \| `access` | `[create, modify, remove]` | Event categories that produce a tick. |

Unknown fields are rejected.

Watching a directory ticks on changes to its entries; watch a single file to
tick only on that file. Editors and deployment tooling that replace a file
atomically produce a remove followed by a create rather than a modification, so
keep `create` and `remove` in `event_kinds` for paths written that way. Some
platforms report changes without a precise category; those tick whenever
`event_kinds` is non-empty.

## Tailing files (`file_tail`)
An MCP Events type takes each new line with `event_type: file.line`:

```yaml
mcp:
  capabilities:
    events:
      - name: app.log
        description: Each line the app writes to its JSON log.
        source:
          watch:
            strategy:
              type: plugin
              kind: file_tail
              paths: ["/var/log/app/*.jsonl", "/var/log/app/*.jsonl.1"]
              start_at: end
            event_type: file.line
```

| Field | Type | Default | Description |
|---|---|---|---|
| `paths` | array of path or glob | *(required)* | Files to tail. A file that appears later and matches is read from its start. |
| `format` | `auto` \| `text` | `auto` | `auto` gives a JSON line as its JSON and any other line as its text (base64 when it is not UTF-8); `text` never parses. |
| `start_at` | `end` \| `beginning` | `end` | Where a file present at a first start, with no stored position, is read from. |
| `max_line_bytes` | integer | 65 536 (at most 131 072) | A longer line is cut there, marked `truncated: true`, and the rest of it is skipped. |
| `poll_interval_ms` | integer | 1000 (at least 100) | How often the files are checked; a change under a watched directory wakes the tail sooner. |
| `max_files` | integer | 64 (at most 256) | Most files tailed at once; the most recently modified win. |
| `max_in_flight` | integer | 256 (at most 1000) | Most lines emitted ahead of the gateway's acknowledgements. |
| `fingerprint_bytes` | integer | 1024 | Bytes at the start of a file that, with its device and inode, identify it. |
| `rotate_wait_ms` | integer | 5000 | How long a renamed or removed file is still read to its end. |
| `ack_timeout_ms` | integer | 30 000 | How long a line waits for its acknowledgement before the file is read again from it. |

Each event's `data` is `{path, offset, line, lineEncoding, truncated?}`. Its
event id comes from the file's identity, a generation token, and the line's
offset, so a line read twice is taken once. A trailing line without a newline
waits until the writer ends it.

- **Rotation.** A file is tracked by its device and inode (its creation time
  on Windows) and a fingerprint of its first bytes, not by its name. A
  renamed file keeps its position and is read to its end for
  `rotate_wait_ms`; include the rotated names in `paths` to keep reading
  them across a restart. A truncated or rewritten file (copytruncate) emits
  `file.truncated` `{path}` and is read again from its start; lines written
  between the copy and the truncate are lost to every tailer.
- **No line lost.** The tail stays at most `max_in_flight` lines ahead of
  the gateway. When the gateway passes over a line without acknowledging it
  (it dropped the event and wants it again), or does not acknowledge it in
  `ack_timeout_ms`, the tail reads again from that line. Each event's cursor
  holds, per file, the offset before its oldest unacknowledged line, so a
  restart repeats at most a window of lines, which keep their ids.
- **Placement.** The source runs on the events owner, so it tails the files
  of that node: run one replica, or put the files on a volume every replica
  sees.

## Watching a folder (`file_objects`)
An MCP Events type takes each change with `event_type: object.changed`:

```yaml
mcp:
  capabilities:
    events:
      - name: inbox.files
        description: A CSV landed in, changed in, or left the inbox.
        source:
          watch:
            strategy:
              type: plugin
              kind: file_objects
              paths: ["/data/inbox/**/*.csv"]
            event_type: object.changed
```

| Field | Type | Default | Description |
|---|---|---|---|
| `paths` | array of path or glob | *(required)* | Files to watch. |
| `interval_ms` | integer | 5000 (at least 250) | Milliseconds between listings; a change under a watched directory lists at once. |
| `start_at` | `now` \| `existing` | `now` | `now` takes the files there at a first start as the baseline; `existing` reports each as created. |
| `max_files` | integer | 10 000 (at most 100 000) | Most files listed; past it the listing is cut, with a warning. |
| `max_in_flight` | integer | 256 (at most 1000) | Most changes offered ahead of the gateway's acknowledgements. |
| `ack_timeout_ms` | integer | 60 000 | How long a change waits for its acknowledgement before it is offered again. |
| `snapshot_bytes` | integer | 131 072 (at most 196 608) | Largest snapshot of the listing an event carries, so a restart is exact. |

Each event's `data` is `{op, store: "file", location, key, size, modifiedAt,
version}`: `op` is `created`, `modified` or `deleted`, `location` is the
directory every pattern starts with, `key` the file's path relative to it,
and `version` its device and inode, so a file atomically replaced under its
name is a modification even when its size and time match. A change keeps its
event id when it is offered again, so the gateway takes it once.

A restart reports exactly what changed while the source was stopped,
deletions included, while the snapshot of the listing fits `snapshot_bytes`
(about 2 000 files at the default). Past that, the source warns once, and a
restart records an upstream gap and takes the files it finds as its new
baseline.

## Change-watching
The gateway starts one watcher per resource URI when a session first calls
`resources/subscribe` on it, shares that watcher across every later subscriber,
and cancels it when the last subscriber goes away — an unsubscribed resource
holds no filesystem watch.

A tick carries no principal and no session: it says the resource changed, not
who changed it. The gateway turns each tick into
`notifications/resources/updated` for that URI's subscribers. Because the tick
carries no identity, a `notification_filter` scoped to `subject_id` or
`session_id` has nothing to narrow on and falls back to fanning out to every
subscriber; an `expression` filter still evaluates per subscriber against
`subscriber.*` and `event.uri`.

Only one plugin may register a given watch kind, so `kind: file` resolves to
this plugin. A resource that names a kind no loaded plugin serves gets a
watcher that idles until cancelled rather than an error at boot.

## Security
- The watched path comes from the gateway's own configuration, never from a
  request. A caller cannot steer the watcher at another path by manipulating
  tool arguments or a resource URI.
- `file` observes change notifications on the path; it never opens or reads
  the file, and a tick carries no file content, no path, and no identity — only
  the fact that the bound resource should be re-read.
- `file_tail` reads the content of the files its configured globs match and
  puts each line in an event, so anyone who may read the event type reads
  those lines. Govern the type as you would the files.
- `file_objects` reads only names and metadata, never content; its events
  still disclose which files exist.
- A path that cannot be watched fails the subscription rather than degrading to
  a silent watcher that never fires.

## Build
Default feature set is OFF (avoids `mcpg_plugin_register` linker
collisions in the workspace build); opt in to the cdylib:

```bash
cargo build -p mcpg-plugin-watch-file --features cdylib-export --release   # → target/release/libmcpg_plugin_watch_file.so
```

## Testing
The suite includes a real notifier round-trip that creates a temporary
directory, writes into it, and asserts a tick arrives, so it needs a working
native filesystem notifier and a writable temp directory:

```bash
cargo test -p mcpg-plugin-watch-file
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Plugin classes and the ABI: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- Resource bindings and their watch block: <https://mcpg.dev/docs/reference/configuration>
- Sibling watch strategy: `libs/plugins/watch/cron` (ticks on a schedule)
