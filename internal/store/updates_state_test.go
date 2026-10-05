package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func openUpdatesStateStore(t *testing.T) (*UpdatesStateStore, *Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(context.Background(), filepath.Join(dir, "telecrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return NewUpdatesStateStore(st), st
}

func TestUpdatesStateCRUD(t *testing.T) {
	ctx := context.Background()
	u, _ := openUpdatesStateStore(t)

	if _, found, err := u.GetUpdatesState(ctx, 7); err != nil || found {
		t.Fatalf("expected missing state, got found=%v err=%v", found, err)
	}

	state := UpdatesState{Pts: 10, Qts: 3, Date: 1000, Seq: 5}
	if err := u.SetUpdatesState(ctx, 7, state); err != nil {
		t.Fatalf("set state: %v", err)
	}
	got, found, err := u.GetUpdatesState(ctx, 7)
	if err != nil || !found {
		t.Fatalf("get state: found=%v err=%v", found, err)
	}
	if got != state {
		t.Fatalf("state mismatch: got %+v want %+v", got, state)
	}

	// Upsert replaces the full state.
	state = UpdatesState{Pts: 20, Qts: 4, Date: 2000, Seq: 6}
	if err := u.SetUpdatesState(ctx, 7, state); err != nil {
		t.Fatalf("re-set state: %v", err)
	}
	if got, _, _ = u.GetUpdatesState(ctx, 7); got != state {
		t.Fatalf("re-set mismatch: got %+v", got)
	}

	if err := u.SetUpdatesPts(ctx, 7, 21); err != nil {
		t.Fatalf("set pts: %v", err)
	}
	if err := u.SetUpdatesQts(ctx, 7, 5); err != nil {
		t.Fatalf("set qts: %v", err)
	}
	if err := u.SetUpdatesDate(ctx, 7, 2100); err != nil {
		t.Fatalf("set date: %v", err)
	}
	if err := u.SetUpdatesSeq(ctx, 7, 7); err != nil {
		t.Fatalf("set seq: %v", err)
	}
	if err := u.SetUpdatesDateSeq(ctx, 7, 2200, 8); err != nil {
		t.Fatalf("set date/seq: %v", err)
	}
	got, _, _ = u.GetUpdatesState(ctx, 7)
	want := UpdatesState{Pts: 21, Qts: 5, Date: 2200, Seq: 8}
	if got != want {
		t.Fatalf("column updates mismatch: got %+v want %+v", got, want)
	}

	// Partial updates on a missing state must fail (manager contract).
	for _, err := range []error{
		u.SetUpdatesPts(ctx, 8, 1),
		u.SetUpdatesQts(ctx, 8, 1),
		u.SetUpdatesDate(ctx, 8, 1),
		u.SetUpdatesSeq(ctx, 8, 1),
		u.SetUpdatesDateSeq(ctx, 8, 1, 1),
	} {
		if err == nil {
			t.Fatalf("expected error updating state of unknown user")
		}
	}
}

func TestUpdatesChannelState(t *testing.T) {
	ctx := context.Background()
	u, _ := openUpdatesStateStore(t)

	if _, found, err := u.GetChannelPts(ctx, 7, 100); err != nil || found {
		t.Fatalf("expected missing channel pts, got found=%v err=%v", found, err)
	}

	if err := u.SetChannelAccessHash(ctx, 7, 100, 0xabc); err != nil {
		t.Fatalf("set channel hash: %v", err)
	}
	if err := u.SetChannelPts(ctx, 7, 100, 42); err != nil {
		t.Fatalf("set channel pts: %v", err)
	}

	pts, found, err := u.GetChannelPts(ctx, 7, 100)
	if err != nil || !found || pts != 42 {
		t.Fatalf("get channel pts: got %d found=%v err=%v", pts, found, err)
	}
	hash, found, err := u.GetChannelAccessHash(ctx, 7, 100)
	if err != nil || !found || hash != 0xabc {
		t.Fatalf("get channel hash: got %#x found=%v err=%v", hash, found, err)
	}

	// pts writes preserve the hash, hash writes preserve pts.
	if err := u.SetChannelPts(ctx, 7, 100, 43); err != nil {
		t.Fatalf("bump channel pts: %v", err)
	}
	if hash, _, _ := u.GetChannelAccessHash(ctx, 7, 100); hash != 0xabc {
		t.Fatalf("hash lost after pts bump: %#x", hash)
	}
	if err := u.SetChannelAccessHash(ctx, 7, 100, 0xdef); err != nil {
		t.Fatalf("update channel hash: %v", err)
	}
	if pts, _, _ = u.GetChannelPts(ctx, 7, 100); pts != 43 {
		t.Fatalf("pts lost after hash update: %d", pts)
	}

	// Hash-only insert keeps a zero pts row, but it must not surface as
	// tracked state: a zero pts kills the manager's channel worker on its
	// first getChannelDifference (PERSISTENT_TIMESTAMP_EMPTY).
	if err := u.SetChannelAccessHash(ctx, 7, 200, 5); err != nil {
		t.Fatalf("insert hash-only: %v", err)
	}
	if pts, found, _ := u.GetChannelPts(ctx, 7, 200); found || pts != 0 {
		t.Fatalf("hash-only row surfaced as state: found=%v pts=%d", found, pts)
	}

	var seen []int64
	err = u.ForEachUpdatesChannels(ctx, 7, func(ctx context.Context, channelID int64, pts int) error {
		seen = append(seen, channelID)
		return nil
	})
	if err != nil {
		t.Fatalf("for each channels: %v", err)
	}
	if len(seen) != 1 || seen[0] != 100 {
		t.Fatalf("iterated %v channels, want only [100] (hash-only excluded)", seen)
	}

	stop := errors.New("stop")
	if err := u.ForEachUpdatesChannels(ctx, 7, func(ctx context.Context, channelID int64, pts int) error {
		return stop
	}); !errors.Is(err, stop) {
		t.Fatalf("iteration callback error not propagated: %v", err)
	}

	// Regression: the callback must be able to query the store again. With a
	// single connection, issuing rows inside the iteration deadlocked (the
	// gotd updates manager's loadChannels does exactly this).
	if err := u.ForEachUpdatesChannels(ctx, 7, func(ctx context.Context, channelID int64, pts int) error {
		_, _, err := u.GetChannelAccessHash(ctx, 7, channelID)
		return err
	}); err != nil {
		t.Fatalf("nested query during iteration: %v", err)
	}
}

func TestUpdatesUserHash(t *testing.T) {
	ctx := context.Background()
	u, _ := openUpdatesStateStore(t)

	if _, found, err := u.GetUserAccessHash(ctx, 7, 9); err != nil || found {
		t.Fatalf("expected missing user hash, got found=%v err=%v", found, err)
	}
	if err := u.SetUserAccessHash(ctx, 7, 9, 0x11); err != nil {
		t.Fatalf("set user hash: %v", err)
	}
	if err := u.SetUserAccessHash(ctx, 7, 9, 0x22); err != nil {
		t.Fatalf("update user hash: %v", err)
	}
	hash, found, err := u.GetUserAccessHash(ctx, 7, 9)
	if err != nil || !found || hash != 0x22 {
		t.Fatalf("get user hash: got %#x found=%v err=%v", hash, found, err)
	}
	if _, found, _ := u.GetUserAccessHash(ctx, 8, 9); found {
		t.Fatalf("user hash leaked across watcher users")
	}
}
