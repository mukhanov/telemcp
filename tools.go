package main

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// registerTools wires the read-only archive tools onto the MCP server.
func registerTools(server *mcp.Server, db *DB) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_status",
		Description: "Report telecrawl archive freshness: database path, last import time, counts of chats/messages/topics, newest message time. Call this first to learn how fresh the Telegram data is.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, *Status, error) {
		st, err := db.Status(ctx)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, st, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_chats",
		Description: "List Telegram chats in the local telecrawl archive, most recently active first. Use the returned chat id in get_messages, search_messages and list_topics.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args listChatsArgs) (*mcp.CallToolResult, []Chat, error) {
		chats, err := db.Chats(ctx, ChatFilter{
			Limit:      args.Limit,
			Folder:     args.Folder,
			UnreadOnly: args.UnreadOnly,
		})
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, chats, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_messages",
		Description: "Read messages from the local telecrawl archive with filters: chat, sender, forum topic, time range, direction. Newest first unless asc=true. Combine after/before with a chat id for a timeline.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args getMessagesArgs) (*mcp.CallToolResult, []Message, error) {
		messages, err := db.Messages(ctx, MessageFilter{
			Chat:   args.Chat,
			Sender: args.Sender,
			Topic:  args.Topic,
			After:  args.After,
			Before: args.Before,
			FromMe: args.FromMe,
			Limit:  args.Limit,
			Asc:    args.Asc,
		})
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, messages, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "search_messages",
		Description: "Full-text search over archived Telegram messages (FTS5). Plain words match by prefix (договор matches договорённости). Also supports \"quoted phrases\", prefix*, sender:NAME, chat:NAME, OR. Returns highlighted snippets, newest first.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args searchMessagesArgs) (*mcp.CallToolResult, []SearchHit, error) {
		hits, err := db.Search(ctx, args.Query, args.Chat, args.Limit)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, hits, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_topics",
		Description: "List forum topics of a Telegram chat from the local telecrawl archive, pinned first. Use topic ids in get_messages topic filter.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args listTopicsArgs) (*mcp.CallToolResult, []Topic, error) {
		topics, err := db.Topics(ctx, args.Chat, args.Limit)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, topics, nil
	})
}

type listChatsArgs struct {
	Limit      int    `json:"limit,omitempty" jsonschema:"max chats to return; default 50, max 500"`
	Folder     string `json:"folder,omitempty" jsonschema:"filter by folder id or title (see 'telecrawl folders')"`
	UnreadOnly bool   `json:"unread_only,omitempty" jsonschema:"only chats with unread messages"`
}

type getMessagesArgs struct {
	Chat   string `json:"chat,omitempty" jsonschema:"chat id (from list_chats) or exact chat name"`
	Sender string `json:"sender,omitempty" jsonschema:"sender id or exact sender name"`
	Topic  string `json:"topic,omitempty" jsonschema:"forum topic id (from list_topics)"`
	After  string `json:"after,omitempty" jsonschema:"only messages at or after this time; RFC3339 or YYYY-MM-DD"`
	Before string `json:"before,omitempty" jsonschema:"only messages at or before this time; RFC3339 or YYYY-MM-DD"`
	FromMe *bool  `json:"from_me,omitempty" jsonschema:"only messages sent by me (true) or by others (false)"`
	Limit  int    `json:"limit,omitempty" jsonschema:"max messages to return; default 50, max 500"`
	Asc    bool   `json:"asc,omitempty" jsonschema:"oldest first instead of newest first"`
}

type searchMessagesArgs struct {
	Query string `json:"query" jsonschema:"FTS5 query: plain words match by prefix; supports \"quoted phrases\", prefix*, sender:NAME, chat:NAME, OR"`
	Chat  string `json:"chat,omitempty" jsonschema:"restrict search to this chat id or exact chat name"`
	Limit int    `json:"limit,omitempty" jsonschema:"max hits to return; default 20, max 100"`
}

type listTopicsArgs struct {
	Chat  string `json:"chat" jsonschema:"forum chat id (from list_chats) or exact chat name"`
	Limit int    `json:"limit,omitempty" jsonschema:"max topics to return; default 100"`
}
