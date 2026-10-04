# telemcp

Read-only [MCP](https://modelcontextprotocol.io) server that exposes a local
[telecrawl](https://github.com/openclaw/telecrawl) Telegram archive to AI
clients: chats, messages, forum topics, and full-text search.

Keep your Telegram history in a local SQLite archive with `telecrawl`, then
let any MCP client (Claude Code, Claude Desktop, …) read it — build digests,
summarize work agreements, search decisions, without a cloud middleman.

```
telecrawl import (cron / launchd)          telemcp                    MCP client
──────────────────────────────►  ~/.telecrawl/telecrawl.db ──────────► Claude …
                                  (SQLite, WAL)      read-only stdio
```

## Requirements

- [telecrawl](https://github.com/openclaw/telecrawl) installed, and at least
  one successful `telecrawl import` (the archive lives at
  `~/.telecrawl/telecrawl.db`)
- Go 1.24+ to build from source

## Install

```sh
git clone <repo> && cd telemcp
go build -o bin/telemcp .
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
| `include_chat` | Stop excluding a chat; history returns on the next import |

Search notes: plain words match by prefix, so Russian inflections work —
`договор` finds *договорились*, *договорённости*. FTS5 syntax also works:
`"quoted phrase"`, `prefix*`, `sender:NAME`, `chat:NAME`, `OR`.

Chat and sender arguments accept either the id (from `list_chats`) or the
exact display name. Time arguments accept RFC3339 or `YYYY-MM-DD`. All tools
are strictly read-only; telemcp never writes to the archive.

## Keeping the archive fresh

telemcp reads whatever the last import left behind; run `telecrawl import`
on a schedule. Repeated imports merge idempotently (no duplicates). A
launchd template for macOS lives in `contrib/` (imports every 10 minutes,
then prunes excluded chats):

```sh
sed -e "s|__TELECRAWL_BIN__|$HOME/bin/telecrawl|" \
    -e "s|__TELEMCP_BIN__|$PWD/bin/telemcp|" \
    -e "s|__LOG__|$HOME/.telecrawl/sync.log|g" \
    -e "s|com\.example\.telemcp-sync|com.$USER.telemcp-sync|" \
    contrib/com.example.telemcp-sync.plist > ~/Library/LaunchAgents/com.$USER.telemcp-sync.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.$USER.telemcp-sync.plist
```

`telecrawl import --messages-limit 100` keeps periodic syncs fast; drop the
flag for a full-depth import. Reading the database while an import runs is
safe (WAL).

## Sync exclusions

Some chats are not worth archiving — dead groups, or conversations you would
rather not keep on disk. Exclude them from any MCP client:

> Exclude the chat *Old Project Team* from sync, reason: archived.

`exclude_chat` records the chat in the telemcp config (managed entirely via
MCP tools, stored under `~/Library/Application Support/telemcp/config.json`,
override with `TELEMCP_CONFIG`), immediately deletes its messages, topics and
archived media from the database, and hides it from every telemcp tool. The
sync agent runs `telemcp prune` after each import so excluded chats never
linger in the archive. `include_chat` reverses the exclusion; the chat's
history reappears after the next import.

## Development

```sh
go test ./...
go build -o bin/telemcp .
```

## License

[MIT](LICENSE)
