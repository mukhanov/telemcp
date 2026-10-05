package archive

import (
	"context"
	"strings"
	"testing"
	"time"
)

// setMedia attaches media fields to an existing fixture message.
func setMedia(t *testing.T, db *DB, chatJID, msgID, mediaType, title, path string, size int64) {
	t.Helper()
	if _, err := db.sql.Exec(`UPDATE messages SET media_type = ?, media_title = ?, media_path = ?, media_size = ?
		WHERE chat_jid = ? AND msg_id = ?`,
		mediaType, title, path, size, chatJID, msgID); err != nil {
		t.Fatalf("set media on %s/%s: %v", chatJID, msgID, err)
	}
}

func mediaFixture(t *testing.T) *DB {
	t.Helper()
	db := newTestDB(t)
	setMedia(t, db, "-100123456", "101", "photo", "Фото отчёта", "/archive/media/aa/101.jpg", 1200)
	setMedia(t, db, "-100123456", "102", "document", "budget.pdf", "/archive/media/bb/102.pdf", 34000)
	setMedia(t, db, "-100123456", "104", "document", "deleted.pdf", "/archive/media/dd/104.pdf", 77) // tombstoned
	setMedia(t, db, "-100777888", "401", "photo", "channel photo", "/archive/media/cc/401.jpg", 900)
	return db
}

func TestMediaMessages(t *testing.T) {
	db := mediaFixture(t)
	ctx := context.Background()

	// All media of Work Chat, newest first; the tombstoned 104 and the
	// media-less 103 stay out.
	files, err := db.MediaMessages(ctx, "Work Chat", nil, 0) // by exact chat name
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].MessageID != "102" || files[1].MessageID != "101" {
		t.Fatalf("media messages = %+v", files)
	}
	want := MediaFile{
		Chat:       "-100123456",
		ChatName:   "Work Chat",
		MessageID:  "102",
		Time:       fixtureBase.Add(200 * time.Second).UTC().Format(time.RFC3339),
		MediaType:  "document",
		MediaTitle: "budget.pdf",
		MediaPath:  "/archive/media/bb/102.pdf",
		MediaSize:  34000,
	}
	if files[0] != want {
		t.Fatalf("newest media = %+v, want %+v", files[0], want)
	}

	// Limit caps the list after ordering.
	capped, err := db.MediaMessages(ctx, "-100123456", nil, 1) // by chat jid
	if err != nil {
		t.Fatal(err)
	}
	if len(capped) != 1 || capped[0].MessageID != "102" {
		t.Fatalf("capped media messages = %+v", capped)
	}

	// Type filters match stored media_type strings exactly.
	photos, err := db.MediaMessages(ctx, "-100123456", []string{"photo"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(photos) != 1 || photos[0].MessageID != "101" || photos[0].MediaType != "photo" ||
		photos[0].MediaPath != "/archive/media/aa/101.jpg" || photos[0].MediaSize != 1200 {
		t.Fatalf("photo filter = %+v", photos)
	}
	both, err := db.MediaMessages(ctx, "-100123456", []string{"document", "photo"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(both) != 2 || both[0].MessageID != "102" || both[1].MessageID != "101" {
		t.Fatalf("document+photo filter = %+v", both)
	}
	videos, err := db.MediaMessages(ctx, "-100123456", []string{"video"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(videos) != 0 {
		t.Fatalf("video filter = %+v, want empty", videos)
	}

	// Other chats keep their own media.
	channel, err := db.MediaMessages(ctx, "News Channel", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(channel) != 1 || channel[0].MessageID != "401" || channel[0].Chat != "-100777888" {
		t.Fatalf("channel media = %+v", channel)
	}
}

func TestMediaMessagesExcluded(t *testing.T) {
	db := mediaFixture(t)
	if _, err := db.MediaMessages(context.Background(), "Work Chat", nil, 0, "-100123456"); err == nil || !strings.Contains(err.Error(), "excluded") {
		t.Fatalf("media messages for excluded chat: %v", err)
	}
}

func TestMediaMessage(t *testing.T) {
	db := mediaFixture(t)
	ctx := context.Background()

	// Message with media, resolved by exact chat name.
	got, err := db.MediaMessage(ctx, "Work Chat", "101")
	if err != nil {
		t.Fatal(err)
	}
	want := MediaFile{
		Chat:       "-100123456",
		ChatName:   "Work Chat",
		MessageID:  "101",
		Time:       fixtureBase.Add(100 * time.Second).UTC().Format(time.RFC3339),
		MediaType:  "photo",
		MediaTitle: "Фото отчёта",
		MediaPath:  "/archive/media/aa/101.jpg",
		MediaSize:  1200,
	}
	if *got != want {
		t.Fatalf("media message = %+v, want %+v", *got, want)
	}

	// Existing message without media: empty MediaType, no error.
	textOnly, err := db.MediaMessage(ctx, "-100123456", "103")
	if err != nil {
		t.Fatal(err)
	}
	if textOnly.MediaType != "" || textOnly.MediaPath != "" || textOnly.MediaSize != 0 ||
		textOnly.MessageID != "103" || textOnly.Chat != "-100123456" {
		t.Fatalf("text-only media message = %+v", textOnly)
	}

	// Missing and tombstoned messages are errors.
	for _, msgID := range []string{"999", "104"} {
		_, err := db.MediaMessage(ctx, "Work Chat", msgID)
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("message %q lookup: %v, want not-found error", msgID, err)
		}
	}

	// Excluded chat is an error.
	if _, err := db.MediaMessage(ctx, "Work Chat", "101", "-100123456"); err == nil || !strings.Contains(err.Error(), "excluded") {
		t.Fatalf("media message for excluded chat: %v", err)
	}
}
