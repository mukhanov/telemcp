package store

import (
	"context"
	"database/sql"
	"sort"
)

// Event-id normalization for imported messages. Imported batches can contain
// messages without a stable event id (live sync converts updates that carry no
// identity beyond chat+message id). writeImport assigns ids through these
// helpers so repeated imports converge on the same rows instead of
// duplicating them.

// normalizeLegacyMessageEventIDs upgrades pre-v5 archives in memory. Legacy
// archives keyed messages by source_pk, so duplicate Telegram identities are
// disambiguated deterministically instead of colliding.
func normalizeLegacyMessageEventIDs(messages []Message) {
	type telegramKey struct {
		chatJID   string
		messageID string
	}
	used := make(map[string]struct{}, len(messages))
	missingCounts := make(map[telegramKey]int)
	for _, message := range messages {
		if message.EventID != "" {
			used[message.EventID] = struct{}{}
		} else {
			missingCounts[telegramKey{chatJID: message.ChatJID, messageID: message.MessageID}]++
		}
	}
	indexes := make([]int, 0, len(messages))
	for i := range messages {
		if messages[i].EventID == "" {
			indexes = append(indexes, i)
		}
	}
	sort.SliceStable(indexes, func(i, j int) bool {
		left, right := messages[indexes[i]], messages[indexes[j]]
		if left.SourcePK != right.SourcePK {
			return left.SourcePK < right.SourcePK
		}
		if left.ChatJID != right.ChatJID {
			return left.ChatJID < right.ChatJID
		}
		return left.MessageID < right.MessageID
	})
	for _, index := range indexes {
		message := &messages[index]
		key := telegramKey{chatJID: message.ChatJID, messageID: message.MessageID}
		eventID := stableMessageEventID(message.ChatJID, message.MessageID)
		if missingCounts[key] > 1 {
			eventID = stableLegacyMessageEventID(message.ChatJID, message.MessageID, message.SourcePK, 0)
		}
		for suffix := 1; ; suffix++ {
			if _, exists := used[eventID]; !exists {
				break
			}
			eventID = stableLegacyMessageEventID(message.ChatJID, message.MessageID, message.SourcePK, suffix)
		}
		message.EventID = eventID
		used[eventID] = struct{}{}
	}
}

func normalizeImportedMessageEventIDs(ctx context.Context, tx *sql.Tx, messages []Message) error {
	used := make(map[string]struct{}, len(messages))
	type telegramKey struct {
		chatJID   string
		messageID string
	}
	groups := make(map[telegramKey][]int)
	for i := range messages {
		if messages[i].EventID != "" {
			used[messages[i].EventID] = struct{}{}
			continue
		}
		key := telegramKey{chatJID: messages[i].ChatJID, messageID: messages[i].MessageID}
		groups[key] = append(groups[key], i)
	}
	indexes := make([]int, 0, len(messages))
	for _, group := range groups {
		indexes = append(indexes, group...)
	}
	sort.SliceStable(indexes, func(i, j int) bool {
		left, right := messages[indexes[i]], messages[indexes[j]]
		if left.ChatJID != right.ChatJID {
			return left.ChatJID < right.ChatJID
		}
		if left.MessageID != right.MessageID {
			return left.MessageID < right.MessageID
		}
		return left.SourcePK < right.SourcePK
	})
	for _, index := range indexes {
		message := &messages[index]
		key := telegramKey{chatJID: message.ChatJID, messageID: message.MessageID}
		rows, err := tx.QueryContext(ctx, `select event_id,source_pk from messages where chat_jid=? and msg_id=? order by event_id`, message.ChatJID, message.MessageID)
		if err != nil {
			return err
		}
		type existingIdentity struct {
			eventID  string
			sourcePK int64
		}
		var existing []existingIdentity
		for rows.Next() {
			var identity existingIdentity
			if err := rows.Scan(&identity.eventID, &identity.sourcePK); err != nil {
				_ = rows.Close()
				return err
			}
			existing = append(existing, identity)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(existing) == 1 && len(groups[key]) == 1 {
			message.EventID = existing[0].eventID
			used[message.EventID] = struct{}{}
			continue
		}
		for _, identity := range existing {
			if identity.sourcePK == message.SourcePK {
				message.EventID = identity.eventID
				used[message.EventID] = struct{}{}
				break
			}
		}
		if message.EventID != "" {
			continue
		}
		candidate := stableMessageEventID(message.ChatJID, message.MessageID)
		if len(groups[key]) > 1 || len(existing) > 1 {
			candidate = stableLegacyMessageEventID(message.ChatJID, message.MessageID, message.SourcePK, 0)
		}
		for suffix := 1; ; suffix++ {
			_, batchCollision := used[candidate]
			var databaseCollision int
			if err := tx.QueryRowContext(ctx, `select exists(select 1 from messages where event_id=?)`, candidate).Scan(&databaseCollision); err != nil {
				return err
			}
			if !batchCollision && databaseCollision == 0 {
				break
			}
			candidate = stableLegacyMessageEventID(message.ChatJID, message.MessageID, message.SourcePK, suffix)
		}
		message.EventID = candidate
		used[candidate] = struct{}{}
	}
	return nil
}
