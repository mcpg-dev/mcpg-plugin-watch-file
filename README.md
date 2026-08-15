# Filesystem Watch Strategy — `dev.mcpg.watch.file`

> class `watch_strategy` · `native` · package `mcpg-plugin-watch-file` · artifact `libmcpg_plugin_watch_file.so` · Apache-2.0

Emits a resource-change tick when a watched path changes on disk, so an MCP
gateway re-notifies subscribers of a file-backed resource the moment the file is
created, modified, or removed. It is backed by the operating system's native
notifier — inotify on Linux, kqueue on the BSDs and macOS, ReadDirectoryChanges
on Windows — so there is no polling interval to tune and no window in which a
change goes unnoticed. Reach for it when a resource is a file or a directory the
gateway can see locally: a mounted config document, a generated artifact, a drop
directory an upstream job writes into.

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
- Declares no `required_capabilities` — the plugin registers a change
  notification on the path and never reads its contents, and it opens no
  network connections.

## Configuration
Loaded from the flat top-level `plugins:` list. The plugin itself takes no
instance config; the path is chosen per watched resource.

```yaml
plugins:
  - id: dev.mcpg.watch.file
    class: watch_strategy
    source:
      oci: ghcr.io/mcpg-dev/source-code/plugins/watch-file:protocol-1
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
- The plugin observes change notifications on the path; it never opens or reads
  the file, and a tick carries no file content, no path, and no identity — only
  the fact that the bound resource should be re-read.
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
