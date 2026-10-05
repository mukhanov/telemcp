package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// DB is a read-only handle to a telemcp SQLite archive (format shared
// with telecrawl).
type DB struct {
	sql  *sql.DB
	path string
}

const (
	primaryLimit = 500 // hard cap for any list query
	searchLimit  = 100
	maxTextRunes = 2000
)

// DefaultPath returns the default archive database location.
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".telemcp", "telemcp.db"), nil
}

// Open opens the telemcp archive at path (empty means the default
// ~/.telemcp/telemcp.db). The connection is read-only when possible;
// telemcp only ever issues SELECTs.
func Open(path string) (*DB, error) {
	if path == "" {
		var err error
		if path, err = DefaultPath(); err != nil {
			return nil, err
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("telemcp database not found at %s; run 'telemcp import' first", path)
		}
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory, not a database", path)
	}

	uri := (&url.URL{Scheme: "file", Path: path}).String()
	sqlDB, err := openSQLite(uri + "?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		// A read-only connection may fail to join a WAL database when the
		// shared-memory files are absent; fall back to a read-write DSN
		// (still only used for SELECTs).
		sqlDB, err = openSQLite(uri + "?_pragma=busy_timeout(5000)")
		if err != nil {
			return nil, err
		}
	}
	db := &DB{sql: sqlDB, path: path}
	if err := db.checkSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func openSQLite(dsn string) (*sql.DB, error) {
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := sqlDB.Ping(); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	return sqlDB, nil
}

func (d *DB) checkSchema() error {
	var n int
	err := d.sql.QueryRow("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('messages', 'chats')").Scan(&n)
	if err != nil || n < 2 {
		return fmt.Errorf("%s is not a telemcp archive (messages/chats tables missing); run 'telemcp import' first", d.path)
	}
	return nil
}

func (d *DB) Close() error { return d.sql.Close() }

// Chat describes a Telegram chat in the archive.
type Chat struct {
	ID            string `json:"id"`
	Kind          string `json:"kind,omitempty"`
	Name          string `json:"name,omitempty"`
	Username      string `json:"username,omitempty"`
	LastMessageAt string `json:"last_message_at,omitempty"`
	UnreadCount   int    `json:"unread_count,omitempty"`
	MessageCount  int    `json:"message_count,omitempty"`
	Forum         bool   `json:"forum,omitempty"`
}

// ChatFilter narrows Chats results.
type ChatFilter struct {
	Limit        int
	Folder       string   // folder id or title
	UnreadOnly   bool     // only chats with unread messages
	Kinds        []string // only chats of these kinds (user, bot, group, channel)
	ExcludeKinds []string // omit chats of these kinds
}

// Chats lists archived chats, most recently active first. Excluded chats
// (see Config) are omitted.
func (d *DB) Chats(ctx context.Context, f ChatFilter, excluded ...string) ([]Chat, error) {
	limit := clampLimit(f.Limit, 50, primaryLimit)
	var (
		args  []any
		join  string
		where = []string{"c.deleted_at IS NULL"}
	)
	if clause := notIn("c.id", excluded, &args); clause != "" {
		where = append(where, clause)
	}
	if clause := inListClause("c.kind", f.Kinds, f.ExcludeKinds, &args); clause != "" {
		where = append(where, clause)
	}
	if f.UnreadOnly {
		where = append(where, "c.unread_count > 0")
	}
	if f.Folder != "" {
		folderID, err := d.resolveFolder(ctx, f.Folder)
		if err != nil {
			return nil, err
		}
		join = "JOIN folder_chats fc ON fc.chat_jid = c.id AND fc.deleted_at IS NULL"
		where = append(where, "fc.folder_id = ?")
		args = append(args, folderID)
	}
	args = append(args, limit)
	rows, err := d.sql.QueryContext(ctx, `
		SELECT c.id, c.kind, c.name, c.username, c.last_message_at, c.unread_count, c.message_count, c.forum
		FROM chats c `+join+`
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY c.last_message_at DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Chat{}
	for rows.Next() {
		var (
			c      Chat
			last   sql.NullInt64
			unread sql.NullInt64
			count  sql.NullInt64
			forumN sql.NullInt64
		)
		if err := rows.Scan(&c.ID, &c.Kind, &c.Name, &c.Username, &last, &unread, &count, &forumN); err != nil {
			return nil, err
		}
		c.LastMessageAt = unixToISO(last)
		c.UnreadCount = int(unread.Int64)
		c.MessageCount = int(count.Int64)
		c.Forum = forumN.Int64 != 0
		out = append(out, c)
	}
	return out, rows.Err()
}

func (d *DB) resolveFolder(ctx context.Context, folder string) (string, error) {
	var id string
	err := d.sql.QueryRowContext(ctx,
		"SELECT id FROM folders WHERE deleted_at IS NULL AND (id = ? OR title = ?) LIMIT 1",
		folder, folder).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("folder %q not found; use the folder id or title shown in chat listings", folder)
	}
	return id, err
}

func (d *DB) resolveChat(ctx context.Context, chat string) (string, error) {
	id, _, err := d.ChatRef(ctx, chat)
	return id, err
}

// ChatRef resolves a chat id or exact display name to its archive id and name.
func (d *DB) ChatRef(ctx context.Context, chat string) (id, name string, err error) {
	err = d.sql.QueryRowContext(ctx,
		"SELECT id, name FROM chats WHERE deleted_at IS NULL AND (id = ? OR name = ?) LIMIT 1",
		chat, chat).Scan(&id, &name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", fmt.Errorf("chat %q not found; call list_chats first", chat)
	}
	return id, name, err
}

// Path returns the archive path this DB was opened from.
func (d *DB) Path() string { return d.path }

// Message is a single archived message.
type Message struct {
	Chat       string `json:"chat"`
	ChatName   string `json:"chat_name,omitempty"`
	MessageID  string `json:"message_id,omitempty"`
	Sender     string `json:"sender,omitempty"`
	FromMe     bool   `json:"from_me,omitempty"`
	Time       string `json:"time"`
	Text       string `json:"text,omitempty"`
	TopicID    string `json:"topic_id,omitempty"`
	Type       string `json:"type,omitempty"`
	MediaType  string `json:"media_type,omitempty"`
	MediaTitle string `json:"media_title,omitempty"`
}

// MessageFilter narrows Messages results.
type MessageFilter struct {
	Chat         string // chat jid or exact chat name
	Sender       string // sender jid or exact sender name
	Topic        string
	After        string // RFC3339 or YYYY-MM-DD
	Before       string
	FromMe       *bool
	Kinds        []string // only messages from chats of these kinds
	ExcludeKinds []string // omit messages from chats of these kinds
	Limit        int
	Asc          bool
}

// Messages reads archived messages matching the filter, newest first unless
// f.Asc is set. Excluded chats are omitted; asking for one explicitly is an
// error so callers learn the chat is excluded rather than seeing empty
// results.
func (d *DB) Messages(ctx context.Context, f MessageFilter, excluded ...string) ([]Message, error) {
	limit := clampLimit(f.Limit, 50, primaryLimit)
	if err := checkChatAllowed(ctx, d, f.Chat, excluded); err != nil {
		return nil, err
	}
	if err := d.checkChatKinds(ctx, f.Chat, f.Kinds, f.ExcludeKinds); err != nil {
		return nil, err
	}
	var (
		args  []any
		where = []string{"deleted_at IS NULL"}
	)
	if clause := notIn("chat_jid", excluded, &args); clause != "" {
		where = append(where, clause)
	}
	if clause := chatKindJoin("chat_jid", f.Kinds, f.ExcludeKinds, &args); clause != "" {
		where = append(where, clause)
	}
	if f.Chat != "" {
		where = append(where, "(chat_jid = ? OR chat_name = ?)")
		args = append(args, f.Chat, f.Chat)
	}
	if f.Sender != "" {
		where = append(where, "(sender_jid = ? OR sender_name = ?)")
		args = append(args, f.Sender, f.Sender)
	}
	if f.Topic != "" {
		where = append(where, "topic_id = ?")
		args = append(args, f.Topic)
	}
	if f.After != "" {
		after, err := parseTimeArg(f.After)
		if err != nil {
			return nil, err
		}
		where = append(where, "ts >= ?")
		args = append(args, after)
	}
	if f.Before != "" {
		before, err := parseTimeArg(f.Before)
		if err != nil {
			return nil, err
		}
		where = append(where, "ts <= ?")
		args = append(args, before)
	}
	if f.FromMe != nil {
		where = append(where, "from_me = ?")
		args = append(args, *f.FromMe)
	}
	order := "DESC"
	if f.Asc {
		order = "ASC"
	}
	args = append(args, limit)
	rows, err := d.sql.QueryContext(ctx, `
		SELECT chat_jid, chat_name, msg_id, sender_jid, sender_name, ts, from_me, text,
			message_type, media_type, media_title, topic_id
		FROM messages
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY ts `+order+`, rowid `+order+` LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		var (
			m          Message
			chatName   sql.NullString
			senderJID  sql.NullString
			senderName sql.NullString
			ts         int64
			fromMe     sql.NullInt64
			text       sql.NullString
			msgType    sql.NullString
			mediaType  sql.NullString
			mediaTitle sql.NullString
			topicID    sql.NullString
		)
		if err := rows.Scan(&m.Chat, &chatName, &m.MessageID, &senderJID, &senderName, &ts, &fromMe, &text,
			&msgType, &mediaType, &mediaTitle, &topicID); err != nil {
			return nil, err
		}
		m.ChatName = chatName.String
		m.Sender = senderName.String
		if m.Sender == "" {
			m.Sender = senderJID.String
		}
		m.FromMe = fromMe.Int64 != 0
		m.Time = time.Unix(ts, 0).UTC().Format(time.RFC3339)
		m.Text = truncateText(text.String)
		m.Type = msgType.String
		m.MediaType = mediaType.String
		m.MediaTitle = mediaTitle.String
		m.TopicID = topicID.String
		out = append(out, m)
	}
	return out, rows.Err()
}

// SearchHit is a single full-text search match.
type SearchHit struct {
	Chat     string `json:"chat"`
	ChatName string `json:"chat_name,omitempty"`
	Sender   string `json:"sender,omitempty"`
	FromMe   bool   `json:"from_me,omitempty"`
	Time     string `json:"time"`
	Snippet  string `json:"snippet"`
	Text     string `json:"text,omitempty"`
}

// SearchFilter narrows Search results.
type SearchFilter struct {
	Query        string // FTS5 query; plain words become prefix terms
	Chat         string // restrict to this chat jid or exact chat name
	Kinds        []string
	ExcludeKinds []string
	Limit        int
}

// Search runs a full-text query over message text and returns matches with
// highlighted snippets, newest first. Excluded chats are omitted.
func (d *DB) Search(ctx context.Context, f SearchFilter, excluded ...string) ([]SearchHit, error) {
	if strings.TrimSpace(f.Query) == "" {
		return nil, errors.New("empty search query")
	}
	if err := checkChatAllowed(ctx, d, f.Chat, excluded); err != nil {
		return nil, err
	}
	if err := d.checkChatKinds(ctx, f.Chat, f.Kinds, f.ExcludeKinds); err != nil {
		return nil, err
	}
	limit := clampLimit(f.Limit, 20, searchLimit)
	var (
		args  []any
		where = []string{"messages_fts MATCH ?", "m.deleted_at IS NULL"}
	)
	args = append(args, ftsQuery(f.Query))
	if clause := notIn("m.chat_jid", excluded, &args); clause != "" {
		where = append(where, clause)
	}
	if clause := chatKindJoin("m.chat_jid", f.Kinds, f.ExcludeKinds, &args); clause != "" {
		where = append(where, clause)
	}
	if f.Chat != "" {
		where = append(where, "(m.chat_jid = ? OR m.chat_name = ?)")
		args = append(args, f.Chat, f.Chat)
	}
	args = append(args, limit)
	rows, err := d.sql.QueryContext(ctx, `
		SELECT m.chat_jid, m.chat_name, m.sender_name, m.from_me, m.ts,
			snippet(messages_fts, 0, '⟦', '⟧', '…', 16), m.text
		FROM messages_fts
		JOIN messages m ON m.rowid = messages_fts.rowid
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY m.ts DESC LIMIT ?`, args...)
	if err != nil {
		if isFTSError(err) {
			return nil, fmt.Errorf("invalid FTS query %q: use plain words, prefix*, \"quoted phrase\", or column filters like sender:NAME", f.Query)
		}
		return nil, err
	}
	defer rows.Close()
	out := []SearchHit{}
	for rows.Next() {
		var (
			h          SearchHit
			chatName   sql.NullString
			senderName sql.NullString
			fromMe     sql.NullInt64
			ts         int64
			text       sql.NullString
		)
		if err := rows.Scan(&h.Chat, &chatName, &senderName, &fromMe, &ts, &h.Snippet, &text); err != nil {
			return nil, err
		}
		h.ChatName = chatName.String
		h.Sender = senderName.String
		h.FromMe = fromMe.Int64 != 0
		h.Time = time.Unix(ts, 0).UTC().Format(time.RFC3339)
		h.Text = truncateText(text.String)
		out = append(out, h)
	}
	return out, rows.Err()
}

// Topic is a forum topic of a chat.
type Topic struct {
	TopicID       string `json:"topic_id"`
	Title         string `json:"title,omitempty"`
	UnreadCount   int    `json:"unread_count,omitempty"`
	Pinned        bool   `json:"pinned,omitempty"`
	Closed        bool   `json:"closed,omitempty"`
	LastMessageAt string `json:"last_message_at,omitempty"`
}

// Topics lists forum topics of a chat, pinned first, then most recently
// active. Asking for an excluded chat is an error.
func (d *DB) Topics(ctx context.Context, chat string, limit int, excluded ...string) ([]Topic, error) {
	if strings.TrimSpace(chat) == "" {
		return nil, errors.New("chat is required; call list_chats first")
	}
	chatID, err := d.resolveChat(ctx, chat)
	if err != nil {
		return nil, err
	}
	if err := checkExcluded(chatID, excluded); err != nil {
		return nil, err
	}
	limit = clampLimit(limit, 100, primaryLimit)
	rows, err := d.sql.QueryContext(ctx, `
		SELECT topic_id, title, unread_count, pinned, closed, last_message_at
		FROM topics
		WHERE chat_jid = ? AND deleted_at IS NULL
		ORDER BY pinned DESC, last_message_at DESC LIMIT ?`, chatID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Topic{}
	for rows.Next() {
		var (
			t      Topic
			unread sql.NullInt64
			pinned sql.NullInt64
			closed sql.NullInt64
			last   sql.NullInt64
		)
		if err := rows.Scan(&t.TopicID, &t.Title, &unread, &pinned, &closed, &last); err != nil {
			return nil, err
		}
		t.UnreadCount = int(unread.Int64)
		t.Pinned = pinned.Int64 != 0
		t.Closed = closed.Int64 != 0
		t.LastMessageAt = unixToISO(last)
		out = append(out, t)
	}
	return out, rows.Err()
}

// Status summarizes archive freshness and size.
type Status struct {
	DBPath        string `json:"db_path"`
	SizeBytes     int64  `json:"size_bytes"`
	ModifiedAt    string `json:"modified_at,omitempty"`
	Chats         int    `json:"chats"`
	Messages      int    `json:"messages"`
	Topics        int    `json:"topics"`
	NewestMessage string `json:"newest_message,omitempty"`
	LastImportAt  string `json:"last_import_at,omitempty"`
}

// Status reports archive counts, the newest message time and the last import
// time so clients can judge data freshness.
func (d *DB) Status(ctx context.Context) (*Status, error) {
	st := &Status{DBPath: d.path}
	if info, err := os.Stat(d.path); err == nil {
		st.SizeBytes = info.Size()
		st.ModifiedAt = info.ModTime().UTC().Format(time.RFC3339)
	}
	var (
		lastImport sql.NullString
		newest     sql.NullInt64
	)
	err := d.sql.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM chats WHERE deleted_at IS NULL),
		(SELECT count(*) FROM messages WHERE deleted_at IS NULL),
		(SELECT count(*) FROM topics WHERE deleted_at IS NULL),
		(SELECT value FROM sync_state WHERE key = 'last_import_at'),
		(SELECT max(ts) FROM messages WHERE deleted_at IS NULL)`).
		Scan(&st.Chats, &st.Messages, &st.Topics, &lastImport, &newest)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return st, nil
		}
		return nil, err
	}
	st.NewestMessage = unixToISO(newest)
	if v := strings.TrimSpace(lastImport.String); v != "" {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			st.LastImportAt = t.UTC().Format(time.RFC3339)
		} else {
			st.LastImportAt = v
		}
	}
	return st, nil
}

// parseTimeArg accepts RFC3339 timestamps or YYYY-MM-DD[ HH:MM] dates
// (interpreted in local time) and returns unix seconds.
func parseTimeArg(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	for _, layout := range []string{time.RFC3339, time.RFC3339Nano, "2006-01-02T15:04", "2006-01-02 15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t.Unix(), nil
		}
	}
	return 0, fmt.Errorf("invalid time %q: use RFC3339 or YYYY-MM-DD", s)
}

// isFTSError reports whether err is an FTS5 query parse failure. Driver
// wording varies ("syntax error", "unterminated string", ...), so match the
// known markers rather than one exact string.
func isFTSError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, marker := range []string{"fts5", "syntax error", "unterminated", "malformed"} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// ftsQuery turns a plain word query into FTS5 prefix terms so inflected
// forms match (договор* matches договорённости). Queries that already use
// FTS5 syntax are passed through unchanged.
func ftsQuery(q string) string {
	upper := strings.ToUpper(q)
	hasSyntax := strings.ContainsAny(q, "\"*():^") ||
		strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") ||
		strings.Contains(upper, " NEAR")
	if hasSyntax {
		return q
	}
	words := strings.Fields(q)
	for i, w := range words {
		words[i] = w + "*"
	}
	return strings.Join(words, " ")
}

func truncateText(s string) string {
	r := []rune(s)
	if len(r) <= maxTextRunes {
		return s
	}
	return string(r[:maxTextRunes]) + "…[truncated]"
}

func unixToISO(v sql.NullInt64) string {
	if !v.Valid || v.Int64 == 0 {
		return ""
	}
	return time.Unix(v.Int64, 0).UTC().Format(time.RFC3339)
}

func clampLimit(v, def, max int) int {
	if v <= 0 {
		return def
	}
	if v > max {
		return max
	}
	return v
}

// notIn builds a "<col> NOT IN (?,?,...)" clause and appends its parameters
// to args. It returns "" when there is nothing to exclude.
func notIn(col string, excluded []string, args *[]any) string {
	if len(excluded) == 0 {
		return ""
	}
	placeholders := make([]string, len(excluded))
	for i, id := range excluded {
		placeholders[i] = "?"
		*args = append(*args, id)
	}
	return col + " NOT IN (" + strings.Join(placeholders, ",") + ")"
}

// inListClause filters the chats table's kind column directly: an include
// list when kinds is set, an omit list otherwise. It returns "" when neither
// is given.
func inListClause(kindCol string, kinds, excludeKinds []string, args *[]any) string {
	switch {
	case len(kinds) > 0:
		return membership(kindCol, kinds, "IN", args)
	case len(excludeKinds) > 0:
		return membership(kindCol, excludeKinds, "NOT IN", args)
	default:
		return ""
	}
}

// chatKindJoin filters a message table's chat id column by chat kind through
// a subquery (chat ids are text on the message side, integer in chats).
// Messages whose chat has no live chats row survive an omit list: nothing
// asserts their kind.
func chatKindJoin(jidCol string, kinds, excludeKinds []string, args *[]any) string {
	op := ""
	vals := kinds
	if len(kinds) > 0 {
		op = "IN"
	} else if len(excludeKinds) > 0 {
		op, vals = "NOT IN", excludeKinds
	} else {
		return ""
	}
	return jidCol + " " + op + " (SELECT CAST(id AS TEXT) FROM chats WHERE deleted_at IS NULL AND " +
		membership("kind", vals, "IN", args) + ")"
}

// membership builds "<col> <op> (?,?,...)" and appends its parameters.
func membership(col string, vals []string, op string, args *[]any) string {
	placeholders := make([]string, len(vals))
	for i, v := range vals {
		placeholders[i] = "?"
		*args = append(*args, v)
	}
	return col + " " + op + " (" + strings.Join(placeholders, ",") + ")"
}

// checkChatKinds errors when an explicit chat filter contradicts the kind
// filter, so callers learn why results would be empty.
func (d *DB) checkChatKinds(ctx context.Context, chat string, kinds, excludeKinds []string) error {
	if chat == "" || (len(kinds) == 0 && len(excludeKinds) == 0) {
		return nil
	}
	var kind string
	err := d.sql.QueryRowContext(ctx,
		"SELECT kind FROM chats WHERE deleted_at IS NULL AND (id = ? OR name = ?) LIMIT 1",
		chat, chat).Scan(&kind)
	if err != nil { // unknown chat: let the query return empty results
		return nil
	}
	allowed := func(k string) bool {
		if len(kinds) > 0 {
			return slices.Contains(kinds, k)
		}
		return !slices.Contains(excludeKinds, k)
	}
	if !allowed(kind) {
		return fmt.Errorf("chat %q is a %s, which the kind filter excludes; drop the chat or kind argument", chat, kind)
	}
	return nil
}

// checkExcluded reports a friendly error when chatID is excluded.
func checkExcluded(chatID string, excluded []string) error {
	if slices.Contains(excluded, chatID) {
		return fmt.Errorf("chat %s is excluded from sync; call include_chat to restore it", chatID)
	}
	return nil
}

// checkChatAllowed resolves an explicit chat filter (if given) and errors
// when that chat is excluded, so callers see why results would be empty.
func checkChatAllowed(ctx context.Context, d *DB, chat string, excluded []string) error {
	if chat == "" || len(excluded) == 0 {
		return nil
	}
	chatID, _, err := d.ChatRef(ctx, chat)
	if err != nil {
		return nil // unknown chat: let the query return empty results
	}
	return checkExcluded(chatID, excluded)
}

// PruneResult summarizes a prune run.
type PruneResult struct {
	Chats      int `json:"chats_pruned"`
	Messages   int `json:"messages_deleted"`
	Topics     int `json:"topics_deleted"`
	MediaFiles int `json:"media_files_deleted"`
}

// Prune deletes excluded chats from the archive: messages (with their FTS
// entries), forum topics, folder links, chat rows, and archived media files.
// It opens its own read-write connection and commits one transaction.
func Prune(ctx context.Context, dbPath string, excluded []string) (*PruneResult, error) {
	res := &PruneResult{}
	if len(excluded) == 0 {
		return res, nil
	}
	uri := (&url.URL{Scheme: "file", Path: dbPath}).String()
	sqlDB, err := sql.Open("sqlite", uri+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	defer sqlDB.Close()
	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, err
	}
	mediaDir, err := filepath.Abs(filepath.Join(filepath.Dir(dbPath), "media"))
	if err != nil {
		return nil, err
	}
	var mediaPaths []string
	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	stmts := []struct {
		sql  string
		kind string
	}{
		{"DELETE FROM messages_fts WHERE rowid IN (SELECT rowid FROM messages WHERE chat_jid = ?)", ""},
		{"DELETE FROM messages WHERE chat_jid = ?", "messages"},
		{"DELETE FROM topics WHERE chat_jid = ?", "topics"},
		{"DELETE FROM folder_chats WHERE chat_jid = ?", ""},
		{"DELETE FROM chats WHERE id = ?", "chats"},
	}
	for _, id := range excluded {
		rows, err := tx.QueryContext(ctx,
			"SELECT DISTINCT media_path FROM messages WHERE chat_jid = ? AND media_path IS NOT NULL AND media_path != ''", id)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				rows.Close()
				return nil, err
			}
			mediaPaths = append(mediaPaths, p)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		for _, stmt := range stmts {
			r, err := tx.ExecContext(ctx, stmt.sql, id)
			if err != nil {
				return nil, err
			}
			n, _ := r.RowsAffected()
			switch stmt.kind {
			case "messages":
				res.Messages += int(n)
			case "topics":
				res.Topics += int(n)
			case "chats":
				res.Chats += int(n)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	for _, p := range mediaPaths {
		abs, err := filepath.Abs(p)
		if err != nil || !pathWithin(mediaDir, abs) {
			continue // never touch files outside the media archive
		}
		if err := os.Remove(abs); err == nil {
			res.MediaFiles++
		}
	}
	return res, nil
}

// pathWithin reports whether path is strictly inside dir (not dir itself).
func pathWithin(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	if err != nil || rel == "." || rel == "" || rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
