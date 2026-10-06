# telemcp

Self-sufficient local Telegram archive: a resident live-sync daemon
(`watch`), one-shot imports (`import`), archive maintenance (`prune`), and a
read-only [MCP](https://modelcontextprotocol.io) server exposing the archive
to AI clients — chats, messages, forum topics, and full-text search. One
binary, one repo, no cloud middleman.

The live-sync stack is a port from
[openclaw/telecrawl](https://github.com/openclaw/telecrawl) (branch `watch`);
the SQLite format is shared, so an existing telecrawl archive keeps working
as-is.

```
Telegram ◄──MTProto── telemcp watch                telemcp            MCP client
                     (resident daemon) ──► ~/.telemcp/telemcp.db ──► Claude …
                                          (SQLite, WAL)   read-only stdio
```

## Requirements

- [Telegram Desktop](https://telegram.org) installed and logged in at least
  once — the sync authorizes through its local session (`tdata`), or through
  a dedicated `telemcp login` session (see below)
- Go 1.27+ to build from source

## Install

Homebrew (macOS):

```sh
brew install mukhanov/telemcp/telemcp
```

or from source (any OS with Telegram Desktop):

```sh
go install github.com/mukhanov/telemcp/cmd/telemcp@latest
```

or clone and build:

```sh
git clone https://github.com/mukhanov/telemcp && cd telemcp
go build -o bin/telemcp ./cmd/telemcp
```

The binary is a single executable with no runtime dependencies; the SQLite
driver is pure Go, so it cross-compiles cleanly.

The archive lives at `~/.telemcp/telemcp.db` by default; every
subcommand takes `--db` to point elsewhere, and the MCP server resolves the
path from its first CLI argument, the `TELEMCP_DB` environment variable, or
the same default.

## A dedicated session (recommended)

By default the sync authorizes through Telegram Desktop's local session
(tdata): telemcp and Desktop then share one authorization, and Telegram
meters rate limits and the update stream per authorization — on busy
archives Desktop can feel sluggish while `telemcp watch` runs. Give telemcp
its own login:

```sh
bin/telemcp login
```

Enter your phone number, the confirmation code and — if enabled — your
two-factor password; the session is stored at `~/.telemcp/session.json`
(`--session` places it elsewhere). Telegram then treats the daemon as its
own device: independent rate limits, independent update stream, visible and
revocable in Desktop under Settings → Devices. `watch`, `import` and media
downloads prefer this session automatically and fall back to tdata when the
file is absent — remove or rename the file to switch back.

The daemon presents itself with Telegram Desktop's public app credentials by
default. Registering your own pair at [my.telegram.org](https://my.telegram.org)
and exporting `TELEMCP_API_ID` / `TELEMCP_API_HASH` (both, or neither) is the
gentlest option for the account.

## Keeping the archive fresh

- **`telemcp watch`** (recommended): a resident daemon holding one
  authorized MTProto connection — new messages land in the archive
  within seconds, and periodic full reconcile passes on the same connection
  cover what the live path defers (edits, counters, media, chat metadata).

  ```sh
  bin/telemcp watch --messages-limit 100 --reconcile-every 30m
  ```

  Run it under launchd — template in `contrib/`:

  ```sh
  sed -e "s|__TELEMCP_BIN__|$PWD/bin/telemcp|" \
      -e "s|__LOG__|$HOME/.telemcp/watch.log|g" \
      -e "s|com\.example\.telemcp-watch|com.$USER.telemcp-watch|" \
      contrib/com.example.telemcp-watch.plist > ~/Library/LaunchAgents/com.$USER.telemcp-watch.plist
  launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.$USER.telemcp-watch.plist
  ```

  Only one daemon may hold the archive's `watch.lock` at a time — stop any
  other watcher for the same database first. A manual `telemcp import`
  refuses to run while the daemon holds the lock.

- **`telemcp import`** on a schedule (cron/launchd): repeated imports merge
  idempotently (no duplicates); `--messages-limit 100` keeps periodic syncs
  fast.

  ```sh
  bin/telemcp import                 # first run: full history import
  bin/telemcp import --messages-limit 100   # later: fast top-up
  ```

Useful flags (both subcommands): `--path` to point at a non-default tdata
directory, `--session` for a non-default session file (a dedicated
`telemcp login` session is preferred automatically when it exists),
`--dialogs-limit`/`--messages-limit` to bound the initial fetch,
`--fetch-media` with `--fetch-media-max-age`/`--fetch-media-max-mb` to
archive photos, videos and documents alongside the messages. `--json`
switches the output to machine-readable stats.

Reading the database while a sync runs is safe (WAL).

Excluded chats (see below) can linger between syncs; a small launchd job
runs `telemcp prune` to delete them from the archive — template in
`contrib/`.

## Configure MCP access

Claude Code:

```sh
claude mcp add --scope user telegram -- /path/to/telemcp/bin/telemcp
```

Any other MCP client (stdio):

```json
{
  "mcpServers": {
    "telegram": {
      "command": "/path/to/telemcp/bin/telemcp"
    }
  }
}
```

## Tools

| Tool | What it does |
|---|---|
| `get_status` | Archive freshness: counts, newest message, last import time |
| `list_chats` | Chats, most recently active first; filter by folder, unread, or chat kind |
| `get_messages` | Messages with filters: chat, sender, topic, time range, direction, chat kind |
| `search_messages` | Full-text search (FTS5) with highlighted snippets; narrow by chat kind |
| `list_topics` | Forum topics of a chat, pinned first |
| `get_sync_config` | Show chats excluded from synchronization |
| `exclude_chat` | Exclude a chat: removed from the archive immediately and hidden from all tools |
| `include_chat` | Stop excluding a chat; history returns on the next sync |
| `download_media` | Download a message's media — or a chat's recent media — into a local folder |

Kind filters: `list_chats`, `get_messages` and `search_messages` take
`kinds` (include list) or `exclude_kinds` (omit list) over the chat kinds
`user` (direct messages), `bot`, `group`, `channel`. Examples: only direct
messages — `kinds: ["user"]`; everything but bots and channels —
`exclude_kinds: ["bot", "channel"]`. Supergroups count as groups, bots are a
kind of their own.

Search notes: plain words match by prefix, so Russian inflections work —
`договор` finds *договорились*, *договорённости*. FTS5 syntax also works:
`"quoted phrase"`, `prefix*`, `sender:NAME`, `chat:NAME`, `OR`.

Media downloads: `download_media` copies files out of the archive without
ever writing to it. `chat` is required; pass `message` to fetch one message's
media, or omit it to grab the chat's most recent media messages (`limit`,
default 20, max 100; optional `types` filter such as `photo` or `document`).
Files land in `dest` (default `~/Downloads/telemcp/<chat>`) named
`<message-id>_<filename>`. Source order: media already archived by
`--fetch-media` is copied straight from disk; the rest is downloaded through
the running `telemcp watch` daemon's connection (local control socket
`<archive dir>/watch.sock`); with no daemon the server opens its own
short-lived tdata session under the same connection lock the sync commands
use (`TELEMCP_SOURCE` overrides the tdata path, mirroring `--path`). Each
file reports a status: `archived`, `downloaded`, `no_media`, `not_found`,
`too_large`, `timeout` or `error`. `max_mb` caps per-file Telegram downloads.

Chat and sender arguments accept either the id (from `list_chats`) or the
exact display name. Time arguments accept RFC3339 or `YYYY-MM-DD`. Every tool
is read-only toward the archive; `download_media` writes only to the
destination folder you name.

## Sync exclusions

Some chats are not worth archiving — dead groups, or conversations you would
rather not keep on disk. Exclude them from any MCP client:

> Exclude the chat *Old Project Team* from sync, reason: archived.

`exclude_chat` records the chat in the telemcp config (managed entirely via
MCP tools, stored under the OS config dir — `~/Library/Application
Support/telemcp/config.json` on macOS, `~/.config/telemcp/config.json` on
Linux; override with `TELEMCP_CONFIG`), immediately deletes its messages, topics and
archived media from the database, and hides it from every telemcp tool. The
prune job removes anything the sync re-fetches later; `include_chat` reverses
the exclusion and the chat's history reappears after the next sync.

## Project layout

```
cmd/telemcp/          entry point: MCP server (default) + watch/import/prune
internal/archive/     read-only archive access for MCP: queries, FTS search, prune
internal/config/      exclusion config (managed via MCP tools)
internal/server/      MCP tool wiring
internal/store/       archive writer: schema, idempotent merges, tombstones
internal/telegram/    sessions (tdata + dedicated login), live updates,
                      media downloads, import (telecrawl watch port)
internal/cli/         watch/import subcommands, media staging
internal/localfile/   path-containment guards for archived media
contrib/              launchd templates (watch, prune)
```

## Development

```sh
go test ./...
go build -o bin/telemcp ./cmd/telemcp
```

## License

[MIT](LICENSE)
