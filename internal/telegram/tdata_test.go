package telegram

import (
	"testing"

	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/tg"
)

// A MessageMediaDocument can arrive with a nil Document interface (for example
// an expired/TTL document or a service-style media row). Calling AsNotEmpty on
// the nil interface panicked with a nil pointer dereference during tdata import;
// these helpers must degrade to zero values instead of crashing.
func TestTdataMediaDocumentNilDocument(t *testing.T) {
	msg := &tg.Message{Media: &tg.MessageMediaDocument{}} // Document left nil

	if got := tdataMediaTitle(msg); got != "" {
		t.Fatalf("tdataMediaTitle(nil document) = %q, want empty string", got)
	}
	if got := tdataMediaSize(msg); got != 0 {
		t.Fatalf("tdataMediaSize(nil document) = %d, want 0", got)
	}
}

// Chat classification drives the kinds/exclude_kinds MCP filters: bots must
// not surface as regular users, and supergroups (channels with the Megagroup
// flag) must count as groups, not broadcast channels.
func TestTdataPeerInfoKinds(t *testing.T) {
	megagroup := &tg.Channel{ID: 4, Title: "Team"}
	megagroup.SetMegagroup(true)
	bot := &tg.User{ID: 2, FirstName: "Reminder"}
	bot.SetBot(true)
	ents := peer.NewEntities(
		map[int64]*tg.User{
			1: {ID: 1, FirstName: "Анна"},
			2: bot,
		},
		nil,
		map[int64]*tg.Channel{
			3: {ID: 3, Title: "News", Broadcast: true},
			4: megagroup,
		},
	)
	cases := []struct {
		peer tg.PeerClass
		kind string
	}{
		{&tg.PeerUser{UserID: 1}, "user"},
		{&tg.PeerUser{UserID: 2}, "bot"},
		{&tg.PeerChannel{ChannelID: 3}, "channel"},
		{&tg.PeerChannel{ChannelID: 4}, "group"},
		{&tg.PeerChat{ChatID: 5}, "group"}, // basic group, no entity needed
	}
	for _, c := range cases {
		if got := tdataPeerInfo(c.peer, ents, 0).kind; got != c.kind {
			t.Errorf("tdataPeerInfo(%T) kind = %q, want %q", c.peer, got, c.kind)
		}
	}
}
