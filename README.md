# telemcp

Read-only [MCP](https://modelcontextprotocol.io) server that exposes a local
[telecrawl](https://github.com/openclaw/telecrawl) Telegram archive to AI
clients: chats, messages, forum topics, and full-text search.

Keep your Telegram history in a local SQLite archive with `telecrawl`, then
let any MCP client (Claude Code, Claude Desktop, …) read it — build digests,
summarize work agreements, search decisions, without a cloud middleman.

```
telecrawl watch (resident daemon)          telemcp                    MCP client
──────────────────────────────►  ~/.telecrawl/telecrawl.db ──────────► Claude …
                                  (SQLite, WAL)      read-only stdio
```

## Requirements

- [telecrawl](https://github.com/openclaw/telecrawl) installed, and at least
  one successful import so the archive exists at
  `~/.telecrawl/telecrawl.db`
- Go 1.24+ to build from source

## Install

```sh
git clone <repo> && cd telemcp
go build -o bin/telemcp ./cmd/telemcp
```

The database path is resolved from the first CLI argument, the `TELEMCP_DB`
environment variable, or the telecrawl default `~/.telecrawl/telecrawl.db`.

## Configure

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
| `list_chats` | Chats, most recently active first; filter by folder or unread |
| `get_messages` | Messages with filters: chat, sender, topic, time range, direction |
| `search_messages` | Full-text search (FTS5) with highlighted snippets |
| `list_topics` | Forum topics of a chat, pinned first |
| `get_sync_config` | Show chats excluded from synchronization |
| `exclude_chat` | Exclude a chat: removed from the archive immediately and hidden from all tools |
| `include_chat` | Stop excluding a chat; history returns on the next sync |

Search notes: plain words match by prefix, so Russian inflections work —
`договор` finds *договорились*, *договорённости*. FTS5 syntax also works:
`"quoted phrase"`, `prefix*`, `sender:NAME`, `chat:NAME`, `OR`.

Chat and sender arguments accept either the id (from `list_chats`) or the
exact display name. Time arguments accept RFC3339 or `YYYY-MM-DD`. All tools
are strictly read-only; telemcp never writes to the archive.

## Keeping the archive fresh

telemcp reads whatever the last sync left behind. Two ways to keep it fresh:

- **`telecrawl watch`** (recommended): a resident daemon holding one
  tdata-authorized MTProto connection — new messages land in the archive
  within seconds, with periodic full reconcile passes on the same
  connection. Needs a telecrawl build that includes `watch`; until it ships
  upstream, use the branch from the telecrawl repository.
- **`telecrawl import` on a schedule** (cron/launchd): repeated imports
  merge idempotently (no duplicates); `--messages-limit 100` keeps periodic
  syncs fast.

Reading the database while a sync runs is safe (WAL).

Excluded chats (see below) can linger between syncs; a small launchd job
runs `telemcp prune` to delete them from the archive — template in
`contrib/`:

```sh
sed -e "s|__TELEMCP_BIN__|$PWD/bin/telemcp|" \
    -e "s|__LOG__|$HOME/.telecrawl/prune.log|g" \
    -e "s|com\.example\.telemcp-prune|com.$USER.telemcp-prune|" \
    contrib/com.example.telemcp-prune.plist > ~/Library/LaunchAgents/com.$USER.telemcp-prune.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.$USER.telemcp-prune.plist
```

## Sync exclusions

Some chats are not worth archiving — dead groups, or conversations you would
rather not keep on disk. Exclude them from any MCP client:

> Exclude the chat *Old Project Team* from sync, reason: archived.

`exclude_chat` records the chat in the telemcp config (managed entirely via
MCP tools, stored under `~/Library/Application Support/telemcp/config.json`,
override with `TELEMCP_CONFIG`), immediately deletes its messages, topics and
archived media from the database, and hides it from every telemcp tool. The
prune job removes anything the sync re-fetches later; `include_chat` reverses
the exclusion and the chat's history reappears after the next sync.

## Project layout

```
cmd/telemcp/       entry point: MCP stdio server + the prune subcommand
internal/archive/  read-only archive access: queries, FTS search, prune
internal/config/   exclusion config (managed via MCP tools)
internal/server/   MCP tool wiring
contrib/           launchd templates
```

## Development

```sh
go test ./...
go build -o bin/telemcp ./cmd/telemcp
```

## License

[MIT](LICENSE)
