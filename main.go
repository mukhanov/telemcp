// Command telemcp exposes a local telecrawl archive (SQLite) to MCP clients
// as read-only tools: chats, messages, forum topics, and full-text search.
//
// The database path comes from the first CLI argument, the TELEMCP_DB
// environment variable, or the telecrawl default ~/.telecrawl/telecrawl.db.
//
// The prune subcommand removes chats excluded in the telemcp config from the
// archive; run it after each telecrawl import (see contrib/ for a launchd
// example).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const version = "0.2.1"

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr) // stdout carries the MCP stdio transport

	if len(os.Args) > 1 && os.Args[1] == "prune" {
		if err := runPrune(context.Background()); err != nil {
			log.Fatalf("telemcp prune: %v", err)
		}
		return
	}

	dbPath := os.Getenv("TELEMCP_DB")
	if dbPath == "" && len(os.Args) > 1 {
		dbPath = os.Args[1]
	}
	db, err := Open(dbPath)
	if err != nil {
		log.Fatalf("telemcp: %v", err)
	}
	defer db.Close()

	configPath, err := defaultConfigPath()
	if err != nil {
		log.Fatalf("telemcp: %v", err)
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "telemcp", Version: version}, nil)
	registerTools(server, db, configPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		log.Fatalf("telemcp: %v", err)
	}
}

// runPrune removes all chats excluded in the config from the archive. It is
// a no-op when nothing is excluded.
func runPrune(ctx context.Context) error {
	dbPath := os.Getenv("TELEMCP_DB")
	if dbPath == "" {
		var err error
		if dbPath, err = defaultDBPath(); err != nil {
			return err
		}
	}
	configPath, err := defaultConfigPath()
	if err != nil {
		return err
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	if len(cfg.ExcludeChats) == 0 {
		fmt.Println("telemcp prune: no chats excluded, nothing to do")
		return nil
	}
	res, err := Prune(ctx, dbPath, cfg.excludedIDs())
	if err != nil {
		return err
	}
	fmt.Printf("telemcp prune: %d chats, %d messages, %d topics, %d media files removed\n",
		res.Chats, res.Messages, res.Topics, res.MediaFiles)
	return nil
}
