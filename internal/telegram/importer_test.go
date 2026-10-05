package telegram

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	querymessages "github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
	"github.com/mukhanov/telemcp/internal/store"
)

func TestSourceIdentityIsOrderIndependent(t *testing.T) {
	t.Parallel()
	left := sourceIdentity("postbox", "2", "1", "1")
	right := sourceIdentity("postbox", "1", "2")
	if left != right || left == sourceIdentity("postbox", "1", "3") {
		t.Fatalf("source identities = left %q right %q", left, right)
	}
}

func TestTDataDialogPeerHandlesPeerlessDialogs(t *testing.T) {
	t.Parallel()
	want := &tg.PeerUser{UserID: 42}
	got, ok := tdataDialogPeer(&tg.Dialog{Peer: want})
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("dialog peer = %#v, %v; want %#v, true", got, ok, want)
	}
	if got, ok := tdataDialogPeer(&tg.DialogCommunity{}); ok || got != nil {
		t.Fatalf("community peer = %#v, %v; want nil, false", got, ok)
	}
}

func TestCopyImportedMediaArchivesByContentHash(t *testing.T) {
	t.Parallel()
	source := filepath.Join(t.TempDir(), "source-media")
	if err := os.WriteFile(source, []byte("fixture media"), 0o600); err != nil {
		t.Fatal(err)
	}
	messages := []store.Message{
		{SourcePK: 1, MediaPath: source},
		{SourcePK: 2, MediaPath: source},
	}
	var stats store.ImportStats
	archiveDir := filepath.Join(t.TempDir(), "media")

	if err := copyImportedMedia(messages, archiveDir, &stats, filepath.Dir(source)); err != nil {
		t.Fatal(err)
	}
	if messages[0].MediaPath == source {
		t.Fatal("media path still points at source cache")
	}
	if messages[1].MediaPath != messages[0].MediaPath {
		t.Fatalf("duplicate media archived to different paths: %q != %q", messages[1].MediaPath, messages[0].MediaPath)
	}
	if messages[0].MediaSize != int64(len("fixture media")) {
		t.Fatalf("media size = %d, want %d", messages[0].MediaSize, len("fixture media"))
	}
	if stats.MediaFiles != 1 || stats.MediaBytes != int64(len("fixture media")) {
		t.Fatalf("media stats = %+v", stats)
	}
	data, err := os.ReadFile(messages[0].MediaPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "fixture media" {
		t.Fatalf("archived media = %q", data)
	}
	if !strings.HasPrefix(messages[0].MediaPath, archiveDir+string(os.PathSeparator)) {
		t.Fatalf("media path %q is not under archive dir %q", messages[0].MediaPath, archiveDir)
	}
}

func TestCopyImportedContactAvatarsArchivesByContentHash(t *testing.T) {
	t.Parallel()
	source := filepath.Join(t.TempDir(), "source-avatar")
	if err := os.WriteFile(source, []byte("fixture avatar"), 0o600); err != nil {
		t.Fatal(err)
	}
	contacts := []store.Contact{
		{JID: "1", AvatarPath: source},
		{JID: "2", AvatarPath: source},
		{JID: "3", AvatarPath: filepath.Join(filepath.Dir(source), "missing-avatar")},
	}
	archiveDir := filepath.Join(t.TempDir(), "media")

	if err := copyImportedContactAvatars(contacts, archiveDir, filepath.Dir(source)); err != nil {
		t.Fatal(err)
	}
	if contacts[0].AvatarPath == source {
		t.Fatal("avatar path still points at source cache")
	}
	if contacts[1].AvatarPath != contacts[0].AvatarPath {
		t.Fatalf("duplicate avatar archived to different paths: %q != %q", contacts[1].AvatarPath, contacts[0].AvatarPath)
	}
	if contacts[2].AvatarPath != "" {
		t.Fatalf("missing avatar path = %q, want cleared", contacts[2].AvatarPath)
	}
	data, err := os.ReadFile(contacts[0].AvatarPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "fixture avatar" {
		t.Fatalf("archived avatar = %q", data)
	}
	if !strings.HasPrefix(contacts[0].AvatarPath, archiveDir+string(os.PathSeparator)) {
		t.Fatalf("avatar path %q is not under archive dir %q", contacts[0].AvatarPath, archiveDir)
	}
}

func TestCopyImportedMediaKeepsExistingArchiveRef(t *testing.T) {
	t.Parallel()
	archiveDir := filepath.Join(t.TempDir(), "media")
	archivedPath := filepath.Join(archiveDir, "ab", "already-archived")
	if err := os.MkdirAll(filepath.Dir(archivedPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archivedPath, []byte("already archived"), 0o600); err != nil {
		t.Fatal(err)
	}
	messages := []store.Message{{SourcePK: 1, MediaPath: archivedPath}}
	var stats store.ImportStats

	if err := copyImportedMedia(messages, archiveDir, &stats); err != nil {
		t.Fatal(err)
	}
	if messages[0].MediaPath != archivedPath {
		t.Fatalf("media path = %q, want existing archive path %q", messages[0].MediaPath, archivedPath)
	}
	if messages[0].MediaSize != int64(len("already archived")) {
		t.Fatalf("media size = %d, want %d", messages[0].MediaSize, len("already archived"))
	}
	if stats.MediaFiles != 1 || stats.MediaBytes != int64(len("already archived")) {
		t.Fatalf("media stats = %+v", stats)
	}
}

func TestCopyImportedMediaSkipsMissingSourceCache(t *testing.T) {
	t.Parallel()
	messages := []store.Message{
		{SourcePK: 1, MediaPath: filepath.Join(t.TempDir(), "missing-cache-file"), MediaSize: 99},
	}
	var stats store.ImportStats

	if err := copyImportedMedia(messages, filepath.Join(t.TempDir(), "media"), &stats, filepath.Dir(messages[0].MediaPath)); err != nil {
		t.Fatal(err)
	}
	if messages[0].MediaPath != "" || messages[0].MediaSize != 0 {
		t.Fatalf("missing media ref = path %q size %d, want cleared", messages[0].MediaPath, messages[0].MediaSize)
	}
	if stats.MediaFiles != 0 || stats.MediaBytes != 0 {
		t.Fatalf("media stats = %+v, want zero", stats)
	}
}

func TestTDataStableSourcePKMatchesLegacyBridge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		chatID    string
		messageID int
		want      int64
	}{
		{"-10042", 7, 7461722351030121860},
		{"123", 456, 6879695626693156840},
		{"-1000000000042", 99, 3163150813737854790},
	}
	for _, tc := range tests {
		if got := stableTDataSourcePK(tc.chatID, tc.messageID); got != tc.want {
			t.Fatalf("stableTDataSourcePK(%q, %d) = %d, want %d", tc.chatID, tc.messageID, got, tc.want)
		}
	}
}

func TestTDataPeerIDsMatchLegacyShape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		peer tg.PeerClass
		want string
	}{
		{"user", &tg.PeerUser{UserID: 123}, "123"},
		{"chat", &tg.PeerChat{ChatID: 456}, "-456"},
		{"channel", &tg.PeerChannel{ChannelID: 789}, "-1000000000789"},
	}
	for _, tc := range tests {
		if got := tdataPeerIDString(tc.peer, 0); got != tc.want {
			t.Fatalf("%s peer id = %q, want %q", tc.name, got, tc.want)
		}
	}
	inputs := map[tg.InputPeerClass]string{
		&tg.InputPeerSelf{}:                  "42",
		&tg.InputPeerUser{UserID: 123}:       "123",
		&tg.InputPeerChat{ChatID: 456}:       "-456",
		&tg.InputPeerChannel{ChannelID: 789}: "-1000000000789",
	}
	for input, want := range inputs {
		if got := tdataInputPeerIDString(input, 42); got != want {
			t.Fatalf("input peer id = %q, want %q", got, want)
		}
	}
}

func TestTDataChatFilterMatchesStoredAndRawIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		chatID string
		filter string
		want   bool
	}{
		{"user stored", "123", "123", true},
		{"group stored", "-456", "-456", true},
		{"group raw", "-456", "456", true},
		{"channel stored", "-1000000000789", "-1000000000789", true},
		{"channel no dash", "-1000000000789", "1000000000789", true},
		{"channel raw", "-1000000000789", "789", true},
		{"channel padded raw", "-1000000000789", "0000000789", true},
		{"different", "-1000000000789", "790", false},
	}
	for _, tc := range tests {
		if got := tdataChatFilterMatches(tc.chatID, tc.filter); got != tc.want {
			t.Fatalf("%s: tdataChatFilterMatches(%q, %q) = %v, want %v", tc.name, tc.chatID, tc.filter, got, tc.want)
		}
	}
}

func TestTDataMediaMapping(t *testing.T) {
	t.Parallel()
	documentMessage := &tg.Message{Media: &tg.MessageMediaDocument{Document: &tg.Document{
		Size: 1234,
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{FileName: "fixture.pdf"},
		},
	}}}
	if got := tdataMediaType(documentMessage); got != "document" {
		t.Fatalf("document media type = %q", got)
	}
	if got := tdataMediaTitle(documentMessage); got != "fixture.pdf" {
		t.Fatalf("document media title = %q", got)
	}
	if got := tdataMediaSize(documentMessage); got != 1234 {
		t.Fatalf("document media size = %d", got)
	}
	webPage := &tg.WebPage{URL: "https://example.test/article"}
	webPage.SetTitle("Fixture Article")
	webMessage := &tg.Message{Media: &tg.MessageMediaWebPage{Webpage: webPage}}
	if got := tdataMediaType(webMessage); got != "webpage" {
		t.Fatalf("web media type = %q", got)
	}
	if got := tdataMediaTitle(webMessage); got != "Fixture Article" {
		t.Fatalf("web media title = %q", got)
	}
}

func TestTDataWebpageMediaFileFallback(t *testing.T) {
	t.Parallel()
	docPage := &tg.WebPage{}
	docPage.SetDocument(&tg.Document{
		ID:         1001,
		MimeType:   "application/pdf",
		AccessHash: 22,
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{FileName: "preview.pdf"},
		},
	})
	docFile, ok := telegramMessageFile(querymessages.Elem{Msg: &tg.Message{Media: &tg.MessageMediaWebPage{Webpage: docPage}}})
	if !ok || docFile.Name != "preview.pdf" || docFile.MIMEType != "application/pdf" {
		t.Fatalf("webpage document file = %#v ok=%v", docFile, ok)
	}
	docLocation, ok := docFile.Location.(*tg.InputDocumentFileLocation)
	if !ok {
		t.Fatalf("webpage document location = %T", docFile.Location)
	}
	if docLocation.ThumbSize != "" {
		t.Fatalf("full webpage document thumb size = %q, want empty", docLocation.ThumbSize)
	}

	photoPage := &tg.WebPage{}
	photoPage.SetPhoto(&tg.Photo{
		ID:            2002,
		AccessHash:    33,
		FileReference: []byte{1, 2, 3},
		Date:          1_800_000_000,
		Sizes: []tg.PhotoSizeClass{
			&tg.PhotoSize{Type: "s", W: 90, H: 90},
			&tg.PhotoSize{Type: "x", W: 800, H: 600},
		},
	})
	photoFile, ok := telegramMessageFile(querymessages.Elem{Msg: &tg.Message{Media: &tg.MessageMediaWebPage{Webpage: photoPage}}})
	if !ok || photoFile.MIMEType != "image/jpeg" {
		t.Fatalf("webpage photo file = %#v ok=%v", photoFile, ok)
	}
	location, ok := photoFile.Location.(*tg.InputPhotoFileLocation)
	if !ok {
		t.Fatalf("webpage photo location = %T", photoFile.Location)
	}
	if location.ThumbSize != "x" {
		t.Fatalf("thumb size = %q, want x", location.ThumbSize)
	}
}

func TestTDataExistingMediaRefsRequireFetchAndSameSource(t *testing.T) {
	t.Parallel()
	source := filepath.Join(t.TempDir(), "tdata")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	ref := ExistingMediaRef{SourcePK: 42, MediaPath: "/tmp/already-archived", MediaSize: 12}
	if refs := tdataExistingMediaRefs(ImportOptions{ExistingMediaRefs: []ExistingMediaRef{ref}}, source); refs != nil {
		t.Fatalf("refs without fetch = %#v, want nil", refs)
	}
	if refs := tdataExistingMediaRefs(ImportOptions{FetchMedia: true, ExistingMediaSourcePath: filepath.Join(t.TempDir(), "other"), ExistingMediaRefs: []ExistingMediaRef{ref}}, source); refs != nil {
		t.Fatalf("refs for different source = %#v, want nil", refs)
	}
	refs := tdataExistingMediaRefs(ImportOptions{FetchMedia: true, ExistingMediaSourcePath: source, ExistingMediaRefs: []ExistingMediaRef{ref}}, source)
	if !reflect.DeepEqual(refs, map[int64]ExistingMediaRef{42: ref}) {
		t.Fatalf("refs = %#v", refs)
	}
}

func TestTDataReplyTopicMapping(t *testing.T) {
	t.Parallel()
	reply := &tg.MessageReplyHeader{}
	reply.SetReplyToMsgID(10)
	reply.SetReplyToTopID(5)
	reply.SetReplyToPeerID(&tg.PeerChannel{ChannelID: 99})
	msg := &tg.Message{}
	msg.SetReplyTo(reply)
	replyTo, threadID, replyChat, topicID := tdataReplyFields(msg, 0)
	if replyTo != "10" || threadID != "5" || topicID != "5" || replyChat != "-1000000000099" {
		t.Fatalf("reply fields = %q %q %q %q", replyTo, threadID, replyChat, topicID)
	}
}
