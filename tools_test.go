package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fixtureBase is a fixed mid-day UTC instant so date-only filters in any
// timezone cover all fixture rows.
var fixtureBase = time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "telecrawl.db")
	sqlDB, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	for _, stmt := range []string{
		`CREATE TABLE chats (id TEXT PRIMARY KEY, kind TEXT, name TEXT, username TEXT, last_message_at INTEGER, unread_count INTEGER DEFAULT 0, message_count INTEGER DEFAULT 0, folder_id TEXT, forum INTEGER DEFAULT 0, deleted_at INTEGER, deletion_source TEXT, deletion_reason TEXT)`,
		`CREATE TABLE folders (id TEXT PRIMARY KEY, title TEXT, emoticon TEXT, color TEXT, flags_json TEXT, deleted_at INTEGER, deletion_source TEXT, deletion_reason TEXT)`,
		`CREATE TABLE folder_chats (folder_id TEXT, chat_jid TEXT, position INTEGER, deleted_at INTEGER, deletion_source TEXT, deletion_reason TEXT, PRIMARY KEY (folder_id, chat_jid))`,
		`CREATE TABLE topics (chat_jid TEXT, topic_id TEXT, title TEXT, top_message_id TEXT, icon_color INTEGER, icon_emoji_id TEXT, unread_count INTEGER DEFAULT 0, unread_mentions_count INTEGER DEFAULT 0, unread_reactions_count INTEGER DEFAULT 0, pinned INTEGER DEFAULT 0, closed INTEGER DEFAULT 0, hidden INTEGER DEFAULT 0, last_message_at INTEGER, deleted_at INTEGER, deletion_source TEXT, deletion_reason TEXT, PRIMARY KEY (chat_jid, topic_id))`,
		`CREATE TABLE messages (rowid INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, source_pk INTEGER, chat_jid TEXT, chat_name TEXT, msg_id TEXT, sender_jid TEXT, sender_name TEXT, ts INTEGER NOT NULL, from_me INTEGER, text TEXT, raw_type INTEGER, message_type TEXT, media_type TEXT, media_title TEXT, media_path TEXT, media_url TEXT, media_size INTEGER, starred INTEGER, topic_id TEXT, deleted_at INTEGER, deletion_source TEXT, deletion_reason TEXT)`,
		`CREATE VIRTUAL TABLE messages_fts USING fts5(text, chat, sender, media)`,
		`CREATE TABLE sync_state (key TEXT PRIMARY KEY, value TEXT, updated_at INTEGER)`,
	} {
		if _, err := sqlDB.Exec(stmt); err != nil {
			t.Fatalf("fixture schema: %v", err)
		}
	}
	base := fixtureBase.Unix()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := sqlDB.Exec(q, args...); err != nil {
			t.Fatalf("fixture data (%s): %v", q, err)
		}
	}
	exec(`INSERT INTO chats (id, kind, name, username, last_message_at, unread_count, message_count, forum) VALUES (?,?,?,?,?,?,?,?)`,
		"-100123456", "group", "Work Chat", "workchat", base+300, 3, 6, 1)
	exec(`INSERT INTO chats (id, kind, name, username, last_message_at, unread_count, message_count, forum) VALUES (?,?,?,?,?,?,?,?)`,
		"777000111", "user", "Иван Петров", "ivanp", base+100, 0, 1, 0)
	exec(`INSERT INTO chats (id, kind, name, last_message_at, deleted_at) VALUES (?,?,?,?,?)`,
		"-100999", "group", "Deleted Chat", base+50, base)
	exec(`INSERT INTO folders (id, title) VALUES ('folder-1', 'Work')`)
	exec(`INSERT INTO folder_chats (folder_id, chat_jid, position) VALUES ('folder-1', '-100123456', 0)`)
	exec(`INSERT INTO topics (chat_jid, topic_id, title, unread_count, pinned, last_message_at) VALUES (?,?,?,?,?,?)`,
		"-100123456", "2", "Бюджет", 1, 0, base+200)
	exec(`INSERT INTO topics (chat_jid, topic_id, title, unread_count, pinned, last_message_at) VALUES (?,?,?,?,?,?)`,
		"-100123456", "1", "Релиз", 0, 1, base+150)

	msgs := []struct {
		rowid      int
		chat, name string
		msgID      string
		sender     string
		ts         int64
		fromMe     int
		text       string
		topic      string
		deletedAt  any
	}{
		{1, "-100123456", "Work Chat", "101", "Анна", base + 100, 0, "Договорённости по релизу: дедлайн в пятницу", "1", nil},
		{2, "-100123456", "Work Chat", "102", "Анна", base + 200, 0, "Бюджет утверждён", "2", nil},
		{3, "-100123456", "Work Chat", "103", "Николай", base + 300, 1, "Ок, фиксирую договорённости", "1", nil},
		{4, "-100123456", "Work Chat", "104", "Анна", base + 400, 0, "секретноепредложение", "", base + 500},
		{5, "777000111", "Иван Петров", "201", "Иван Петров", base + 150, 0, "Привет, как саммари?", "", nil},
	}
	for _, m := range msgs {
		exec(`INSERT INTO messages (rowid, event_id, chat_jid, chat_name, msg_id, sender_name, ts, from_me, text, topic_id, deleted_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			m.rowid, "evt-"+m.msgID, m.chat, m.name, m.msgID, m.sender, m.ts, m.fromMe, m.text, m.topic, m.deletedAt)
		exec(`INSERT INTO messages_fts (rowid, text, chat, sender) VALUES (?,?,?,?)`,
			m.rowid, m.text, m.name, m.sender)
	}
	exec(`INSERT INTO sync_state (key, value, updated_at) VALUES ('last_import_at', ?, ?)`,
		fixtureBase.Add(1000*time.Second).Format(time.RFC3339Nano), base+1000)
	return &DB{sql: sqlDB, path: path}
}

func TestOpenMissingDB(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "absent.db"))
	if err == nil || !strings.Contains(err.Error(), "telecrawl import") {
		t.Fatalf("error = %v, want hint to run telecrawl import", err)
	}
}

func TestOpenNotArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "other.db")
	sqlDB, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE x (v)"); err != nil {
		t.Fatal(err)
	}
	sqlDB.Close()
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "not a telecrawl archive") {
		t.Fatalf("error = %v, want not-a-telecrawl-archive error", err)
	}
}

func TestChats(t *testing.T) {
	db := newTestDB(t)
	chats, err := db.Chats(context.Background(), ChatFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(chats) != 2 { // deleted chat excluded
		t.Fatalf("chats = %d, want 2: %+v", len(chats), chats)
	}
	if chats[0].Name != "Work Chat" || !chats[0].Forum {
		t.Fatalf("first chat = %+v, want Work Chat forum", chats[0])
	}
	unread, _ := db.Chats(context.Background(), ChatFilter{UnreadOnly: true})
	if len(unread) != 1 || unread[0].ID != "-100123456" {
		t.Fatalf("unread chats = %+v", unread)
	}
	folder, err := db.Chats(context.Background(), ChatFilter{Folder: "Work"}) // by title
	if err != nil {
		t.Fatal(err)
	}
	if len(folder) != 1 || folder[0].ID != "-100123456" {
		t.Fatalf("folder chats = %+v", folder)
	}
	if _, err := db.Chats(context.Background(), ChatFilter{Folder: "Nope"}); err == nil {
		t.Fatal("unknown folder must error")
	}
}

func TestMessagesFilters(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	base := fixtureBase

	byID, err := db.Messages(ctx, MessageFilter{Chat: "-100123456"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byID) != 3 { // 101,102,103 live; 104 tombstoned
		t.Fatalf("messages by id = %d, want 3", len(byID))
	}
	byName, _ := db.Messages(ctx, MessageFilter{Chat: "Work Chat"})
	if len(byName) != 3 {
		t.Fatalf("messages by name = %d, want 3", len(byName))
	}
	if byID[0].MessageID != "103" { // newest first
		t.Fatalf("first = %+v, want newest (103)", byID[0])
	}

	after, err := db.Messages(ctx, MessageFilter{After: base.Add(150 * time.Second).Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 3 { // 201, 102, 103 (inclusive lower bound)
		t.Fatalf("after filter = %d, want 3", len(after))
	}
	before, _ := db.Messages(ctx, MessageFilter{Before: base.Add(200 * time.Second).Format(time.RFC3339)})
	if len(before) != 3 { // 101, 201, 102 (inclusive upper bound)
		t.Fatalf("before filter = %d, want 3", len(before))
	}
	range1, err := db.Messages(ctx, MessageFilter{
		After:  base.Add(150 * time.Second).Format(time.RFC3339),
		Before: base.Add(200 * time.Second).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(range1) != 2 { // 201, 102
		t.Fatalf("range filter = %d, want 2", len(range1))
	}
	dateOnly, err := db.Messages(ctx, MessageFilter{Chat: "-100123456", After: "2026-01-02"})
	if err != nil {
		t.Fatal(err)
	}
	if len(dateOnly) != 3 {
		t.Fatalf("date-only after = %d, want 3", len(dateOnly))
	}
	if _, err := db.Messages(ctx, MessageFilter{After: "tomorrow"}); err == nil {
		t.Fatal("invalid date must error")
	}

	fromMe := true
	mine, _ := db.Messages(ctx, MessageFilter{Chat: "-100123456", FromMe: &fromMe})
	if len(mine) != 1 || !mine[0].FromMe {
		t.Fatalf("from_me = %+v", mine)
	}

	topic, _ := db.Messages(ctx, MessageFilter{Chat: "-100123456", Topic: "1"})
	if len(topic) != 2 { // 101, 103
		t.Fatalf("topic filter = %d, want 2", len(topic))
	}

	asc, _ := db.Messages(ctx, MessageFilter{Chat: "-100123456", Asc: true})
	if asc[0].MessageID != "101" {
		t.Fatalf("asc first = %+v, want 101", asc[0])
	}

	limited, _ := db.Messages(ctx, MessageFilter{Limit: 1})
	if len(limited) != 1 {
		t.Fatalf("limit = %d, want 1", len(limited))
	}
}

func TestSearch(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	// Plain Russian word must match its inflected form via the prefix rewrite.
	hits, err := db.Search(ctx, "договор", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 { // договорённости (101) and фиксирую договорённости (103)
		t.Fatalf("prefix search hits = %d, want 2: %+v", len(hits), hits)
	}
	if !strings.Contains(hits[0].Snippet, "⟦") {
		t.Fatalf("snippet without highlight: %q", hits[0].Snippet)
	}

	phrase, err := db.Search(ctx, `"бюджет утверждён"`, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(phrase) != 1 || phrase[0].ChatName != "Work Chat" {
		t.Fatalf("phrase hits = %+v", phrase)
	}

	inChat, _ := db.Search(ctx, "релиз", "Work Chat", 0)
	if len(inChat) != 1 {
		t.Fatalf("chat-scoped hits = %+v", inChat)
	}

	deleted, _ := db.Search(ctx, "секретноепредложение", "", 0)
	if len(deleted) != 0 {
		t.Fatalf("tombstoned message leaked into search: %+v", deleted)
	}

	if _, err := db.Search(ctx, `("unclosed`, "", 0); err == nil || !strings.Contains(err.Error(), "FTS") {
		t.Fatalf("syntax error = %v, want FTS guidance", err)
	}
	if _, err := db.Search(ctx, "   ", "", 0); err == nil {
		t.Fatal("empty query must error")
	}
}

func TestTopics(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	topics, err := db.Topics(ctx, "Work Chat", 0) // resolve by name
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 2 {
		t.Fatalf("topics = %+v", topics)
	}
	if topics[0].TopicID != "1" || !topics[0].Pinned { // pinned first
		t.Fatalf("pinned ordering broken: %+v", topics)
	}
	if _, err := db.Topics(ctx, "No Such Chat", 0); err == nil {
		t.Fatal("unknown chat must error")
	}
	if _, err := db.Topics(ctx, "", 0); err == nil {
		t.Fatal("empty chat must error")
	}
}

func TestStatus(t *testing.T) {
	db := newTestDB(t)
	st, err := db.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Chats != 2 || st.Messages != 4 || st.Topics != 2 {
		t.Fatalf("counts = %+v", st)
	}
	if st.LastImportAt == "" || st.NewestMessage == "" {
		t.Fatalf("freshness fields empty: %+v", st)
	}
	if want := fixtureBase.Add(300 * time.Second).UTC().Format(time.RFC3339); st.NewestMessage != want {
		t.Fatalf("newest = %s, want %s", st.NewestMessage, want)
	}
}

func TestFTSQueryRewrite(t *testing.T) {
	cases := []struct{ in, want string }{
		{"договор", "договор*"},
		{"договор сроки", "договор* сроки*"},
		{`"бюджет утверждён"`, `"бюджет утверждён"`},
		{"prefix*", "prefix*"},
		{"sender:Анна", "sender:Анна"},
		{"кот OR пёс", "кот OR пёс"},
		{"(a AND b)", "(a AND b)"},
	}
	for _, c := range cases {
		if got := ftsQuery(c.in); got != c.want {
			t.Errorf("ftsQuery(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestConfigRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.ExcludeChats) != 0 {
		t.Fatalf("missing config not empty: %+v", cfg)
	}
	if _, err := cfg.exclude("-100123456", "Work Chat", "archived"); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.exclude("-100123456", "Work Chat", "dup"); err == nil {
		t.Fatal("duplicate exclusion must error")
	}
	if err := saveConfig(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.ExcludeChats) != 1 || loaded.ExcludeChats[0].ID != "-100123456" || loaded.ExcludeChats[0].ExcludedAt == "" {
		t.Fatalf("loaded = %+v", loaded)
	}
	entry, err := loaded.include("Work Chat") // by name
	if err != nil {
		t.Fatal(err)
	}
	if entry.ID != "-100123456" {
		t.Fatalf("entry = %+v", entry)
	}
	if _, err := loaded.include("Work Chat"); err == nil {
		t.Fatal("including a non-excluded chat must error")
	}
}

func TestQueryExclusions(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	excl := []string{"-100123456"}

	chats, err := db.Chats(ctx, ChatFilter{}, excl...)
	if err != nil {
		t.Fatal(err)
	}
	if len(chats) != 1 || chats[0].ID != "777000111" {
		t.Fatalf("chats with exclusion = %+v", chats)
	}
	messages, err := db.Messages(ctx, MessageFilter{}, excl...)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Chat != "777000111" {
		t.Fatalf("messages with exclusion = %+v", messages)
	}
	hits, err := db.Search(ctx, "договор", "", 0, excl...)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("search leaked excluded chat: %+v", hits)
	}
	if _, err := db.Topics(ctx, "Work Chat", 0, excl...); err == nil || !strings.Contains(err.Error(), "excluded") {
		t.Fatalf("topics on excluded chat: %v", err)
	}
	if _, err := db.Messages(ctx, MessageFilter{Chat: "Work Chat"}, excl...); err == nil || !strings.Contains(err.Error(), "excluded") {
		t.Fatalf("messages for excluded chat: %v", err)
	}
	// Unknown chat still returns empty results rather than an error.
	unknown, err := db.Messages(ctx, MessageFilter{Chat: "Ghost"}, excl...)
	if err != nil || len(unknown) != 0 {
		t.Fatalf("unknown chat: %v %+v", err, unknown)
	}
}

func TestPrune(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	// Stage one media file inside the archive media dir and one outside;
	// prune must remove only the archived one.
	mediaDir := filepath.Join(filepath.Dir(db.path), "media", "ab")
	if err := os.MkdirAll(mediaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(mediaDir, "deadbeef")
	outside := filepath.Join(t.TempDir(), "keepme")
	for _, p := range []string{inside, outside} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.sql.Exec(`UPDATE messages SET media_path = ? WHERE rowid = 1`, inside); err != nil {
		t.Fatal(err)
	}
	if _, err := db.sql.Exec(`UPDATE messages SET media_path = ? WHERE rowid = 2`, outside); err != nil {
		t.Fatal(err)
	}

	res, err := Prune(ctx, db.path, []string{"-100123456"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Chats != 1 || res.Messages != 4 || res.Topics != 2 || res.MediaFiles != 1 {
		t.Fatalf("prune result = %+v", res)
	}
	if _, err := os.Stat(inside); !os.IsNotExist(err) {
		t.Fatalf("archived media still on disk: %v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("file outside media dir was removed: %v", err)
	}
	var n int
	if err := db.sql.QueryRow("SELECT count(*) FROM messages WHERE chat_jid = '-100123456'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("messages after prune = %d (%v)", n, err)
	}
	if err := db.sql.QueryRow("SELECT count(*) FROM messages_fts").Scan(&n); err != nil || n != 1 { // only Иван's row
		t.Fatalf("fts after prune = %d (%v)", n, err)
	}
	if err := db.sql.QueryRow("SELECT count(*) FROM folder_chats").Scan(&n); err != nil || n != 0 {
		t.Fatalf("folder_chats after prune = %d (%v)", n, err)
	}

	again, err := Prune(ctx, db.path, []string{"-100123456"})
	if err != nil {
		t.Fatal(err)
	}
	if again.Chats != 0 || again.Messages != 0 {
		t.Fatalf("prune not idempotent: %+v", again)
	}
	noop, err := Prune(ctx, db.path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if noop.Chats != 0 {
		t.Fatalf("empty prune = %+v", noop)
	}
}

func TestPathWithin(t *testing.T) {
	dir := t.TempDir()
	if !pathWithin(dir, filepath.Join(dir, "a", "b")) {
		t.Fatal("nested path should be within")
	}
	if pathWithin(dir, filepath.Join(dir, "..", "escape")) {
		t.Fatal("escape should not be within")
	}
	if pathWithin(dir, dir) {
		t.Fatal("dir itself should not be within")
	}
}

func TestParseTimeArg(t *testing.T) {
	if _, err := parseTimeArg("2026-01-02T12:00:00Z"); err != nil {
		t.Errorf("RFC3339: %v", err)
	}
	if _, err := parseTimeArg("2026-01-02"); err != nil {
		t.Errorf("date-only: %v", err)
	}
	if _, err := parseTimeArg("2026-01-02 15:04"); err != nil {
		t.Errorf("date-time: %v", err)
	}
	if v, _ := parseTimeArg(""); v != 0 {
		t.Errorf("empty = %d, want 0", v)
	}
	if _, err := parseTimeArg("not a date"); err == nil {
		t.Error("invalid date must error")
	}
}
