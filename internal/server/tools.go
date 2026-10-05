// Package server wires the archive tools onto an MCP server.
package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/mukhanov/telemcp/internal/archive"
	"github.com/mukhanov/telemcp/internal/config"
)

// configMu serializes read-modify-write cycles on the config file: MCP tool
// calls may run concurrently.
var configMu sync.Mutex

// Register wires the archive tools onto the MCP server. Query tools hide
// chats excluded in the config at configPath; exclude_chat prunes them.
func Register(server *mcp.Server, db *archive.DB, configPath string) {
	exclusions := func() []string {
		cfg, err := config.Load(configPath)
		if err != nil {
			return nil
		}
		return cfg.ExcludedIDs()
	}

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_status",
		Description: "Report archive freshness: database path, last import time, counts of chats/messages/topics, newest message time. Call this first to learn how fresh the Telegram data is.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, *archive.Status, error) {
		st, err := db.Status(ctx)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, st, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_chats",
		Description: "List Telegram chats in the local archive, most recently active first. Use the returned chat id in get_messages, search_messages and list_topics. Filter by kind (user=direct messages, bot, group, channel) with kinds/exclude_kinds. Chats excluded from sync (see get_sync_config) are not listed.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args listChatsArgs) (*mcp.CallToolResult, *ChatsResult, error) {
		kinds, excludeKinds, err := resolveKinds(args.Kinds, args.ExcludeKinds)
		if err != nil {
			return nil, nil, err
		}
		chats, err := db.Chats(ctx, archive.ChatFilter{
			Limit:        args.Limit,
			Folder:       args.Folder,
			UnreadOnly:   args.UnreadOnly,
			Kinds:        kinds,
			ExcludeKinds: excludeKinds,
		}, exclusions()...)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, &ChatsResult{Chats: chats}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_messages",
		Description: "Read messages from the local archive with filters: chat, sender, forum topic, time range, direction, chat kind (user=direct messages, bot, group, channel). Newest first unless asc=true. Combine after/before with a chat id for a timeline.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args getMessagesArgs) (*mcp.CallToolResult, *MessagesResult, error) {
		kinds, excludeKinds, err := resolveKinds(args.Kinds, args.ExcludeKinds)
		if err != nil {
			return nil, nil, err
		}
		messages, err := db.Messages(ctx, archive.MessageFilter{
			Chat:         args.Chat,
			Sender:       args.Sender,
			Topic:        args.Topic,
			After:        args.After,
			Before:       args.Before,
			FromMe:       args.FromMe,
			Kinds:        kinds,
			ExcludeKinds: excludeKinds,
			Limit:        args.Limit,
			Asc:          args.Asc,
		}, exclusions()...)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, &MessagesResult{Messages: messages}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "search_messages",
		Description: "Full-text search over archived Telegram messages (FTS5). Plain words match by prefix (договор matches договорённости). Also supports \"quoted phrases\", prefix*, sender:NAME, chat:NAME, OR. Returns highlighted snippets, newest first. Narrow by chat kind with kinds/exclude_kinds (user=direct messages, bot, group, channel).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args searchMessagesArgs) (*mcp.CallToolResult, *SearchResult, error) {
		kinds, excludeKinds, err := resolveKinds(args.Kinds, args.ExcludeKinds)
		if err != nil {
			return nil, nil, err
		}
		hits, err := db.Search(ctx, archive.SearchFilter{
			Query:        args.Query,
			Chat:         args.Chat,
			Kinds:        kinds,
			ExcludeKinds: excludeKinds,
			Limit:        args.Limit,
		}, exclusions()...)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, &SearchResult{Hits: hits}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_topics",
		Description: "List forum topics of a Telegram chat from the local archive, pinned first. Use topic ids in get_messages topic filter.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args listTopicsArgs) (*mcp.CallToolResult, *TopicsResult, error) {
		topics, err := db.Topics(ctx, args.Chat, args.Limit, exclusions()...)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, &TopicsResult{Topics: topics}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_sync_config",
		Description: "Show sync configuration: chats excluded from synchronization. Excluded chats are hidden from every telemcp tool and removed from the archive. Manage with exclude_chat and include_chat.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, *SyncConfig, error) {
		cfg, err := config.Load(configPath)
		if err != nil {
			return nil, nil, err
		}
		if cfg.ExcludeChats == nil {
			cfg.ExcludeChats = []config.ChatExclusion{}
		}
		return &mcp.CallToolResult{}, &SyncConfig{ExcludeChats: cfg.ExcludeChats}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "exclude_chat",
		Description: "Exclude a chat from synchronization: it is immediately removed from the archive (messages, topics, media) and hidden from all telemcp tools. Use for archived chats or contacts you do not want tracked. Reversible with include_chat; data returns after the next sync.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args excludeChatArgs) (*mcp.CallToolResult, *ExcludeResult, error) {
		configMu.Lock()
		defer configMu.Unlock()
		cfg, err := config.Load(configPath)
		if err != nil {
			return nil, nil, err
		}
		id, name, err := db.ChatRef(ctx, args.Chat)
		if err != nil {
			return nil, nil, err
		}
		entry, err := cfg.Exclude(id, name, args.Reason)
		if err != nil {
			return nil, nil, err
		}
		if err := config.Save(configPath, cfg); err != nil {
			return nil, nil, err
		}
		pruned, err := archive.Prune(ctx, db.Path(), []string{id})
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, &ExcludeResult{Excluded: entry, Pruned: pruned}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "include_chat",
		Description: "Stop excluding a chat from synchronization (accepts the chat id or its name). Its history reappears after the next sync.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args includeChatArgs) (*mcp.CallToolResult, *IncludeResult, error) {
		configMu.Lock()
		defer configMu.Unlock()
		cfg, err := config.Load(configPath)
		if err != nil {
			return nil, nil, err
		}
		entry, err := cfg.Include(args.Chat)
		if err != nil {
			return nil, nil, err
		}
		if err := config.Save(configPath, cfg); err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, &IncludeResult{
			Restored: entry,
			Note:     "chat will reappear in tools after the next sync",
		}, nil
	})

	registerDownloadMedia(server, db, exclusions)
}

// Result wrappers below keep every tool's structured output an object with a
// "type": "object" outputSchema, as the MCP spec requires (Claude Code
// tolerates top-level arrays; stricter clients like pi/omp reject them).

// ChatsResult wraps list_chats output.
type ChatsResult struct {
	Chats []archive.Chat `json:"chats"`
}

// MessagesResult wraps get_messages output.
type MessagesResult struct {
	Messages []archive.Message `json:"messages"`
}

// SearchResult wraps search_messages output.
type SearchResult struct {
	Hits []archive.SearchHit `json:"hits"`
}

// TopicsResult wraps list_topics output.
type TopicsResult struct {
	Topics []archive.Topic `json:"topics"`
}

// SyncConfig is the sync configuration exposed to clients.
type SyncConfig struct {
	ExcludeChats []config.ChatExclusion `json:"exclude_chats"`
}

// ExcludeResult reports a chat exclusion and what was removed from disk.
type ExcludeResult struct {
	Excluded config.ChatExclusion `json:"excluded"`
	Pruned   *archive.PruneResult `json:"pruned"`
}

// IncludeResult reports a chat restored to synchronization.
type IncludeResult struct {
	Restored config.ChatExclusion `json:"restored"`
	Note     string               `json:"note,omitempty"`
}

type listChatsArgs struct {
	Limit        int      `json:"limit,omitempty" jsonschema:"max chats to return; default 50, max 500"`
	Folder       string   `json:"folder,omitempty" jsonschema:"filter by folder id or title"`
	UnreadOnly   bool     `json:"unread_only,omitempty" jsonschema:"only chats with unread messages"`
	Kinds        []string `json:"kinds,omitempty" jsonschema:"only chats of these kinds: user (direct messages), bot, group, channel"`
	ExcludeKinds []string `json:"exclude_kinds,omitempty" jsonschema:"omit chats of these kinds: user (direct messages), bot, group, channel"`
}

type getMessagesArgs struct {
	Chat         string   `json:"chat,omitempty" jsonschema:"chat id (from list_chats) or exact chat name"`
	Sender       string   `json:"sender,omitempty" jsonschema:"sender id or exact sender name"`
	Topic        string   `json:"topic,omitempty" jsonschema:"forum topic id (from list_topics)"`
	After        string   `json:"after,omitempty" jsonschema:"only messages at or after this time; RFC3339 or YYYY-MM-DD"`
	Before       string   `json:"before,omitempty" jsonschema:"only messages at or before this time; RFC3339 or YYYY-MM-DD"`
	FromMe       *bool    `json:"from_me,omitempty" jsonschema:"only messages sent by me (true) or by others (false)"`
	Kinds        []string `json:"kinds,omitempty" jsonschema:"only messages from chats of these kinds: user (direct messages), bot, group, channel"`
	ExcludeKinds []string `json:"exclude_kinds,omitempty" jsonschema:"omit messages from chats of these kinds: user (direct messages), bot, group, channel"`
	Limit        int      `json:"limit,omitempty" jsonschema:"max messages to return; default 50, max 500"`
	Asc          bool     `json:"asc,omitempty" jsonschema:"oldest first instead of newest first"`
}

type searchMessagesArgs struct {
	Query        string   `json:"query" jsonschema:"FTS5 query: plain words match by prefix; supports \"quoted phrases\", prefix*, sender:NAME, chat:NAME, OR"`
	Chat         string   `json:"chat,omitempty" jsonschema:"restrict search to this chat id or exact chat name"`
	Kinds        []string `json:"kinds,omitempty" jsonschema:"only hits from chats of these kinds: user (direct messages), bot, group, channel"`
	ExcludeKinds []string `json:"exclude_kinds,omitempty" jsonschema:"omit hits from chats of these kinds: user (direct messages), bot, group, channel"`
	Limit        int      `json:"limit,omitempty" jsonschema:"max hits to return; default 20, max 100"`
}

type listTopicsArgs struct {
	Chat  string `json:"chat" jsonschema:"forum chat id (from list_chats) or exact chat name"`
	Limit int    `json:"limit,omitempty" jsonschema:"max topics to return; default 100"`
}

type excludeChatArgs struct {
	Chat   string `json:"chat" jsonschema:"chat id (from list_chats) or exact chat name to exclude from sync"`
	Reason string `json:"reason,omitempty" jsonschema:"why this chat is excluded (e.g. archived, personal)"`
}

type includeChatArgs struct {
	Chat string `json:"chat" jsonschema:"excluded chat id or its name, as shown by get_sync_config"`
}

// chatKinds is the closed set of chat kinds stored by the sync.
var chatKinds = map[string]bool{
	"user": true, "bot": true, "group": true, "channel": true, "unknown": true,
}

// resolveKinds normalizes and validates a kinds/exclude_kinds pair. At most
// one of the two may be set; kinds are lowercased on the way through.
func resolveKinds(kinds, excludeKinds []string) (normalized, exclude []string, err error) {
	if len(kinds) > 0 && len(excludeKinds) > 0 {
		return nil, nil, errors.New("pass either kinds or exclude_kinds, not both")
	}
	normalize := func(vals []string, what string) ([]string, error) {
		out := make([]string, 0, len(vals))
		for _, v := range vals {
			v = strings.ToLower(strings.TrimSpace(v))
			if !chatKinds[v] {
				return nil, fmt.Errorf("unknown chat kind %q in %s; valid kinds: user, bot, group, channel, unknown", v, what)
			}
			out = append(out, v)
		}
		return out, nil
	}
	if normalized, err = normalize(kinds, "kinds"); err != nil {
		return nil, nil, err
	}
	if exclude, err = normalize(excludeKinds, "exclude_kinds"); err != nil {
		return nil, nil, err
	}
	return normalized, exclude, nil
}
