// Command telemcp exposes a local telecrawl archive (SQLite) to MCP clients
// as read-only tools: chats, messages, forum topics, and full-text search.
//
// The database path comes from the first CLI argument, the TELEMCP_DB
// environment variable, or the telecrawl default ~/.telecrawl/telecrawl.db.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const version = "0.1.0"

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr) // stdout carries the MCP stdio transport

	dbPath := os.Getenv("TELEMCP_DB")
	if dbPath == "" && len(os.Args) > 1 {
		dbPath = os.Args[1]
	}
	db, err := Open(dbPath)
	if err != nil {
		log.Fatalf("telemcp: %v", err)
	}
	defer db.Close()

	server := mcp.NewServer(&mcp.Implementation{Name: "telemcp", Version: version}, nil)
	registerTools(server, db)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		log.Fatalf("telemcp: %v", err)
	}
}
