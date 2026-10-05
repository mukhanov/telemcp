// Command telemcp is the full local Telegram archive stack: a live-sync
// daemon (watch), one-shot imports (import), archive maintenance (prune), and
// a read-only MCP server (default mode) exposing the archive to AI clients.
//
// The database path for the MCP server comes from the first CLI argument, the
// TELEMCP_DB environment variable, or the default ~/.telecrawl/telecrawl.db.
// The sync subcommands accept global flags (--db, --json, --source) before
// the subcommand name.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"telemcp/internal/archive"
	"telemcp/internal/cli"
	"telemcp/internal/config"
	"telemcp/internal/server"
)

const version = "0.2.1"

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr) // stdout carries the MCP stdio transport

	if subcommand := detectSubcommand(os.Args[1:]); subcommand != "" {
		switch subcommand {
		case "prune":
			if err := runPrune(context.Background()); err != nil {
				log.Fatalf("telemcp prune: %v", err)
			}
			return
		case "watch", "import", "sync":
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			err := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
			stop()
			if err != nil {
				fmt.Fprintln(os.Stderr, "telemcp:", err)
				os.Exit(cli.ExitCode(err))
			}
			return
		}
	}

	dbPath := os.Getenv("TELEMCP_DB")
	if dbPath == "" && len(os.Args) > 1 {
		dbPath = os.Args[1]
	}
	db, err := archive.Open(dbPath)
	if err != nil {
		log.Fatalf("telemcp: %v", err)
	}
	defer db.Close()

	configPath, err := config.DefaultPath()
	if err != nil {
		log.Fatalf("telemcp: %v", err)
	}

	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "telemcp", Version: version}, nil)
	server.Register(mcpServer, db, configPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := mcpServer.Run(ctx, &mcp.StdioTransport{}); err != nil {
		log.Fatalf("telemcp: %v", err)
	}
}

// detectSubcommand finds the sync subcommand in args, tolerating global
// flags before it (telemcp --db X import); it returns "" for MCP mode.
func detectSubcommand(args []string) string {
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "prune", "watch", "import", "sync":
			return args[i]
		case "--db", "--source": // value-carrying globals: skip their values
			i++
		}
	}
	return ""
}

// runPrune removes all chats excluded in the config from the archive. It is
// a no-op when nothing is excluded.
func runPrune(ctx context.Context) error {
	dbPath := os.Getenv("TELEMCP_DB")
	if dbPath == "" {
		var err error
		if dbPath, err = archive.DefaultPath(); err != nil {
			return err
		}
	}
	configPath, err := config.DefaultPath()
	if err != nil {
		return err
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if len(cfg.ExcludeChats) == 0 {
		fmt.Println("telemcp prune: no chats excluded, nothing to do")
		return nil
	}
	res, err := archive.Prune(ctx, dbPath, cfg.ExcludedIDs())
	if err != nil {
		return err
	}
	fmt.Printf("telemcp prune: %d chats, %d messages, %d topics, %d media files removed\n",
		res.Chats, res.Messages, res.Topics, res.MediaFiles)
	return nil
}
