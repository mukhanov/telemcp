package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestAllCanonicalEntitiesExposeTombstoneMetadata(t *testing.T) {
	t.Parallel()
	st := openTestStore(t, filepath.Join(t.TempDir(), "schema.db"))
	for _, table := range []string{"chats", "folders", "folder_chats", "topics", "contacts", "groups", "group_participants", "messages"} {
		columns, err := columns(context.Background(), st.db, table)
		if err != nil {
			t.Fatal(err)
		}
		for _, column := range []string{"deleted_at", "deletion_source", "deletion_reason"} {
			if !columns[column] {
				t.Fatalf("%s missing %s", table, column)
			}
		}
	}
	messageColumns, err := columns(context.Background(), st.db, "messages")
	if err != nil {
		t.Fatal(err)
	}
	if !messageColumns["event_id"] {
		t.Fatal("messages missing stable event_id")
	}
	for _, index := range []string{"idx_messages_source_identity", "idx_message_revisions_message"} {
		var exists int
		if err := st.db.QueryRowContext(context.Background(), `select exists(select 1 from sqlite_master where type='index' and name=?)`, index).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists == 0 {
			t.Fatalf("missing import/migration support index %s", index)
		}
	}
	revisionColumns, err := columns(context.Background(), st.db, "message_revisions")
	if err != nil {
		t.Fatal(err)
	}
	if !revisionColumns["predecessor_event_id"] {
		t.Fatal("message_revisions missing causal predecessor_event_id")
	}
}

func TestBaselineSeedingUsesBoundedBatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	st := openTestStore(t, filepath.Join(t.TempDir(), "baseline-batches.db"))
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	for i := 1; i <= 1201; i++ {
		messageID := fmt.Sprintf("%d", i)
		if _, err := tx.ExecContext(ctx, `insert into messages(event_id,source_pk,chat_jid,msg_id,ts,from_me,raw_type,starred) values(?,?,?,?,?,?,?,?)`, stableMessageEventID("100", messageID), i, "100", messageID, unix(now), 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := seedMissingMessageBaselines(ctx, tx, now, "test-batch"); err != nil {
		t.Fatal(err)
	}
	var revisions int
	if err := tx.QueryRowContext(ctx, `select count(*) from message_revisions`).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 1201 {
		t.Fatalf("baseline revisions = %d, want 1201", revisions)
	}
}

func TestRevisionIdentityKeepsUntimestampedAndSameTimestampEdits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	stats := ImportStats{SourcePath: t.TempDir(), SourcePathCanonical: true, SourceIdentity: "test:telegram", FinishedAt: now}
	message := Message{SourcePK: 1, ChatJID: "100", MessageID: "1", Timestamp: now, Text: "original"}
	st := openTestStore(t, filepath.Join(t.TempDir(), "same-time-revisions.db"))
	if err := st.ReplaceAll(ctx, stats, nil, []Chat{{JID: "100", Kind: "chat"}}, nil, nil, nil, []Message{message}); err != nil {
		t.Fatal(err)
	}
	editTime := now.Add(time.Minute)
	for i, text := range []string{"same time one", "same time two"} {
		changed := message
		changed.Text = text
		changed.EditTime = editTime
		stats.FinishedAt = now.Add(time.Duration(i+2) * time.Minute)
		if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{changed}); err != nil {
			t.Fatal(err)
		}
	}
	untimestamped := message
	untimestamped.Text = "observable without edit timestamp"
	untimestamped.ReactionsJSON = `[{"emoji":"+1"}]`
	stats.FinishedAt = now.Add(5 * time.Minute)
	if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{untimestamped}); err != nil {
		t.Fatal(err)
	}
	if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{untimestamped}); err != nil {
		t.Fatal(err)
	}
	backToEarlierPayload := message
	backToEarlierPayload.Text = "same time one"
	backToEarlierPayload.EditTime = editTime
	stats.FinishedAt = now.Add(6 * time.Minute)
	if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{backToEarlierPayload}); err != nil {
		t.Fatal(err)
	}
	if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{backToEarlierPayload}); err != nil {
		t.Fatal(err)
	}
	var edits, uniqueEdits int
	if err := st.db.QueryRowContext(ctx, `select count(*),count(distinct event_id) from message_revisions where event_type='message_edited'`).Scan(&edits, &uniqueEdits); err != nil {
		t.Fatal(err)
	}
	if edits != 4 || uniqueEdits != 4 {
		t.Fatalf("edited revisions=%d unique=%d, want four distinct transitions with retries deduped", edits, uniqueEdits)
	}
	var predecessors int
	if err := st.db.QueryRowContext(ctx, `select count(*) from message_revisions where coalesce(predecessor_event_id,'')<>''`).Scan(&predecessors); err != nil {
		t.Fatal(err)
	}
	if predecessors != 4 {
		t.Fatalf("causal predecessor links = %d, want one for each edit transition", predecessors)
	}
}

func TestParentDeletionRevisionPropagationSpansMultipleBatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	stats := ImportStats{SourcePath: t.TempDir(), SourcePathCanonical: true, SourceIdentity: "test:telegram", FinishedAt: now}
	messages := make([]Message, 1001)
	for i := range messages {
		messages[i] = Message{SourcePK: int64(i + 1), ChatJID: "100", MessageID: strconv.Itoa(i + 1), Timestamp: now, Text: "child"}
	}
	st := openTestStore(t, filepath.Join(t.TempDir(), "propagation-batches.db"))
	if err := st.ReplaceAll(ctx, stats, nil, []Chat{{JID: "100", Kind: "chat"}}, nil, nil, nil, messages); err != nil {
		t.Fatal(err)
	}
	stats.FinishedAt = now.Add(2 * time.Minute)
	deleted := Chat{JID: "100", Kind: "chat", Tombstone: Tombstone{DeletedAt: now.Add(time.Minute), DeletionSource: "telegram", DeletionReason: "explicit-chat-delete"}}
	if err := st.MergeAll(ctx, stats, nil, []Chat{deleted}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var deletedMessages, deleteRevisions int
	if err := st.db.QueryRowContext(ctx, `select count(*) from messages where deleted_at is not null`).Scan(&deletedMessages); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(ctx, `select count(*) from message_revisions where event_type='message_deleted'`).Scan(&deleteRevisions); err != nil {
		t.Fatal(err)
	}
	if deletedMessages != len(messages) || deleteRevisions != len(messages) {
		t.Fatalf("batched propagation messages=%d revisions=%d want=%d", deletedMessages, deleteRevisions, len(messages))
	}
}

func TestRepeatedParentPropagationRecordsNewCausalDeletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	stats := ImportStats{SourcePath: t.TempDir(), SourcePathCanonical: true, SourceIdentity: "test:telegram", FinishedAt: now}
	chat := Chat{JID: "100", Kind: "chat"}
	message := Message{SourcePK: 1, ChatJID: "100", MessageID: "1", Timestamp: now, Text: "child"}
	st := openTestStore(t, filepath.Join(t.TempDir(), "repeat-parent-delete.db"))
	if err := st.ReplaceAll(ctx, stats, nil, []Chat{chat}, nil, nil, nil, []Message{message}); err != nil {
		t.Fatal(err)
	}
	deletedAt := now.Add(time.Minute)
	chat.Tombstone = Tombstone{DeletedAt: deletedAt, DeletionSource: "telegram", DeletionReason: "explicit-chat-delete"}
	stats.FinishedAt = now.Add(2 * time.Minute)
	if err := st.MergeAll(ctx, stats, nil, []Chat{chat}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	message.Text = "transient authoritative resurrection"
	message.EditTime = now.Add(3 * time.Minute)
	stats.FinishedAt = now.Add(4 * time.Minute)
	if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{message}); err != nil {
		t.Fatal(err)
	}
	var deleteRevisions int
	if err := st.db.QueryRowContext(ctx, `select count(*) from message_revisions where message_event_id=? and event_type='message_deleted'`, stableMessageEventID("100", "1")).Scan(&deleteRevisions); err != nil {
		t.Fatal(err)
	}
	if deleteRevisions != 2 {
		t.Fatalf("repeated parent propagation recorded %d delete revisions, want 2 causal transitions", deleteRevisions)
	}
	tx, err := st.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	tip, _, err := messageRevisionPredecessor(ctx, tx, stableMessageEventID("100", "1"), "")
	if err != nil {
		t.Fatal(err)
	}
	if tip.eventType != "message_deleted" || !tip.eventAt.Equal(deletedAt) {
		t.Fatalf("causal tip after repeated propagation = %#v, want parent deletion", tip)
	}
}

func TestMessageRevisionEventsAreStableAcrossEditsDeletesAndRetries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	stats := ImportStats{SourcePath: t.TempDir(), SourcePathCanonical: true, SourceIdentity: "test:telegram", FinishedAt: now}
	chat := Chat{JID: "100", Kind: "chat", Name: "chat"}
	message := Message{SourcePK: 11, ChatJID: "100", MessageID: "22", Timestamp: now, Text: "original"}
	st := openTestStore(t, filepath.Join(t.TempDir(), "revisions.db"))
	if err := st.ReplaceAll(ctx, stats, nil, []Chat{chat}, nil, nil, nil, []Message{message}); err != nil {
		t.Fatal(err)
	}
	edited := message
	edited.Text = "edited"
	edited.EditTime = now.Add(time.Minute)
	stats.FinishedAt = now.Add(2 * time.Minute)
	if err := st.MergeAll(ctx, stats, nil, []Chat{chat}, nil, nil, nil, []Message{edited}); err != nil {
		t.Fatal(err)
	}
	if err := st.MergeAll(ctx, stats, nil, []Chat{chat}, nil, nil, nil, []Message{edited}); err != nil {
		t.Fatal(err)
	}
	deleted := edited
	deleted.Tombstone = Tombstone{DeletedAt: now.Add(3 * time.Minute), DeletionSource: "telegram", DeletionReason: "explicit-message-delete"}
	stats.FinishedAt = now.Add(4 * time.Minute)
	if err := st.MergeAll(ctx, stats, nil, []Chat{chat}, nil, nil, nil, []Message{deleted}); err != nil {
		t.Fatal(err)
	}
	if err := st.MergeAll(ctx, stats, nil, []Chat{chat}, nil, nil, nil, []Message{deleted}); err != nil {
		t.Fatal(err)
	}
	revisions, err := queryAllMessageRevisions(ctx, st.db)
	if err != nil {
		t.Fatal(err)
	}
	if len(revisions) != 3 {
		t.Fatalf("revision events = %#v, want created + edited + deleted", revisions)
	}
	wantTypes := []string{"message_created", "message_edited", "message_deleted"}
	gotTypes := make([]string, 0, len(revisions))
	ids := make(map[string]struct{}, len(revisions))
	for _, revision := range revisions {
		gotTypes = append(gotTypes, revision.EventType)
		ids[revision.EventID] = struct{}{}
		if revision.MessageEventID != stableMessageEventID("100", "22") {
			t.Fatalf("message event identity changed: %#v", revision)
		}
	}
	if !slices.Equal(gotTypes, wantTypes) || len(ids) != 3 {
		t.Fatalf("revision types=%v unique_ids=%d", gotTypes, len(ids))
	}
	var canonicalRows int
	if err := st.db.QueryRowContext(ctx, `select count(*) from messages where event_id=?`, stableMessageEventID("100", "22")).Scan(&canonicalRows); err != nil {
		t.Fatal(err)
	}
	if canonicalRows != 1 {
		t.Fatalf("canonical rows = %d, want stable single identity", canonicalRows)
	}
}

// allMessagesRaw reads every messages row, tombstoned included, for
// assertions the public query API cannot express.
func allMessagesRaw(t *testing.T, st *Store) []Message {
	t.Helper()
	ctx := context.Background()
	rows, err := st.db.QueryContext(ctx, `select event_id,source_pk,coalesce(text,''),coalesce(deleted_at,0) from messages order by source_pk`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []Message
	for rows.Next() {
		var m Message
		var deletedAt int64
		if err := rows.Scan(&m.EventID, &m.SourcePK, &m.Text, &deletedAt); err != nil {
			t.Fatal(err)
		}
		if deletedAt != 0 {
			m.Tombstone = Tombstone{DeletedAt: fromUnix(deletedAt)}
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDirectImportDisambiguatesDuplicateTelegramIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	stats := ImportStats{SourcePath: t.TempDir(), SourcePathCanonical: true, SourceIdentity: "test:telegram", FinishedAt: now}
	st := openTestStore(t, filepath.Join(t.TempDir(), "direct-duplicates.db"))
	messages := []Message{
		{SourcePK: 20, ChatJID: "100", MessageID: "duplicate", Timestamp: now, Text: "source pk 20"},
		{SourcePK: 10, ChatJID: "100", MessageID: "duplicate", Timestamp: now, Text: "source pk 10"},
	}
	if err := st.ReplaceAll(ctx, stats, nil, []Chat{{JID: "100", Kind: "chat"}}, nil, nil, nil, messages); err != nil {
		t.Fatal(err)
	}
	stored := allMessagesRaw(t, st)
	if len(stored) != 2 || stored[0].EventID == stored[1].EventID {
		t.Fatalf("direct import collapsed duplicate semantic rows: %#v", stored)
	}
	eventIDs := map[int64]string{}
	for _, message := range stored {
		eventIDs[message.SourcePK] = message.EventID
		if message.SourcePK == 10 && message.EventID != stableLegacyMessageEventID("100", "duplicate", 10, 0) {
			t.Fatalf("lowest source_pk event = %q", message.EventID)
		}
	}
	partial := messages[0]
	partial.Text = "source pk 20 updated"
	stats.FinishedAt = now.Add(time.Minute)
	if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{partial}); err != nil {
		t.Fatal(err)
	}
	stored = allMessagesRaw(t, st)
	if len(stored) != 2 {
		t.Fatalf("partial merge changed duplicate family size: %#v", stored)
	}
	for _, message := range stored {
		if message.EventID != eventIDs[message.SourcePK] {
			t.Fatalf("partial merge remapped source_pk %d from %q to %q", message.SourcePK, eventIDs[message.SourcePK], message.EventID)
		}
		if message.SourcePK == 10 && message.Text != "source pk 10" {
			t.Fatalf("partial merge overwrote base duplicate: %#v", message)
		}
	}
	discoveryStore := openTestStore(t, filepath.Join(t.TempDir(), "partial-discovery.db"))
	if err := discoveryStore.ReplaceAll(ctx, stats, nil, []Chat{{JID: "100", Kind: "chat"}}, nil, nil, nil, []Message{messages[0]}); err != nil {
		t.Fatal(err)
	}
	if err := discoveryStore.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{messages[1]}); err != nil {
		t.Fatal(err)
	}
	discovered := allMessagesRaw(t, discoveryStore)
	if len(discovered) != 1 || discovered[0].EventID != stableMessageEventID("100", "duplicate") || discovered[0].SourcePK != 10 {
		t.Fatalf("single-row discovery should reconcile source-pk drift by Telegram identity: %#v", discovered)
	}
}

func TestDirectImportReconcilesSourcePKDriftByTelegramIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	stats := ImportStats{SourcePath: t.TempDir(), SourcePathCanonical: true, SourceIdentity: "test:telegram", FinishedAt: now}
	st := openTestStore(t, filepath.Join(t.TempDir(), "source-pk-drift.db"))
	message := Message{SourcePK: 10, ChatJID: "100", MessageID: "7", Timestamp: now, Text: "first cache"}
	if err := st.ReplaceAll(ctx, stats, nil, []Chat{{JID: "100", Kind: "chat"}}, nil, nil, nil, []Message{message}); err != nil {
		t.Fatal(err)
	}
	message.SourcePK = 9001
	message.Text = "rebuilt cache"
	message.EditTime = now.Add(time.Minute)
	stats.FinishedAt = now.Add(2 * time.Minute)
	if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{message}); err != nil {
		t.Fatal(err)
	}
	stored := allMessagesRaw(t, st)
	if len(stored) != 1 || stored[0].EventID != stableMessageEventID("100", "7") || stored[0].SourcePK != 9001 || stored[0].Text != "rebuilt cache" {
		t.Fatalf("source-pk drift split Telegram identity: %#v", stored)
	}
	message.SourcePK = 42
	message.Tombstone = Tombstone{DeletedAt: now.Add(3 * time.Minute), DeletionSource: "telegram", DeletionReason: "explicit-delete"}
	stats.FinishedAt = now.Add(4 * time.Minute)
	if err := st.MergeAll(ctx, stats, nil, nil, nil, nil, nil, []Message{message}); err != nil {
		t.Fatal(err)
	}
	stored = allMessagesRaw(t, st)
	if len(stored) != 1 || stored[0].Tombstone.DeletedAt.IsZero() || stored[0].Text != "rebuilt cache" {
		t.Fatalf("source-pk-drifted deletion missed or erased canonical row: %#v", stored)
	}
}
