package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// UpdatesState is the persisted Telegram update-stream position for one
// account: the common state (pts/qts/date/seq) or a channel-local pts.
type UpdatesState struct {
	Pts  int
	Qts  int
	Date int
	Seq  int
}

// UpdatesStateStore persists the gotd updates.Manager state so a restarted
// watch daemon resumes from its saved position instead of refetching.
// It uses plain types on purpose: the store must not import gotd; the
// telegram package adapts these methods to the manager interfaces.
type UpdatesStateStore struct {
	st *Store
}

// NewUpdatesStateStore returns the update-state store for an open archive.
func NewUpdatesStateStore(st *Store) *UpdatesStateStore {
	return &UpdatesStateStore{st: st}
}

// GetUpdatesState returns the saved common state for userID.
func (u *UpdatesStateStore) GetUpdatesState(ctx context.Context, userID int64) (UpdatesState, bool, error) {
	var state UpdatesState
	err := u.st.db.QueryRowContext(ctx,
		"SELECT pts, qts, date, seq FROM updates_state WHERE user_id = ?", userID).
		Scan(&state.Pts, &state.Qts, &state.Date, &state.Seq)
	if errors.Is(err, sql.ErrNoRows) {
		return UpdatesState{}, false, nil
	}
	if err != nil {
		return UpdatesState{}, false, err
	}
	return state, true, nil
}

// SetUpdatesState upserts the common state for userID.
func (u *UpdatesStateStore) SetUpdatesState(ctx context.Context, userID int64, state UpdatesState) error {
	_, err := u.st.db.ExecContext(ctx, `
		INSERT INTO updates_state (user_id, pts, qts, date, seq, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET
			pts = excluded.pts, qts = excluded.qts, date = excluded.date,
			seq = excluded.seq, updated_at = excluded.updated_at`,
		userID, state.Pts, state.Qts, state.Date, state.Seq, time.Now().Unix())
	return err
}

// updateStateColumn applies a single-column UPDATE; per the manager contract
// (and SetPts/SetQts/... semantics), updating a state that does not exist is
// an error rather than a silent write of partial state.
func (u *UpdatesStateStore) updateStateColumn(ctx context.Context, userID int64, column string, value int) error {
	res, err := u.st.db.ExecContext(ctx,
		"UPDATE updates_state SET "+column+" = ?, updated_at = ? WHERE user_id = ?",
		value, time.Now().Unix(), userID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return errors.New("updates state for user does not exist: " + column)
	}
	return nil
}

// SetUpdatesPts advances the saved pts.
func (u *UpdatesStateStore) SetUpdatesPts(ctx context.Context, userID int64, pts int) error {
	return u.updateStateColumn(ctx, userID, "pts", pts)
}

// SetUpdatesQts advances the saved qts.
func (u *UpdatesStateStore) SetUpdatesQts(ctx context.Context, userID int64, qts int) error {
	return u.updateStateColumn(ctx, userID, "qts", qts)
}

// SetUpdatesDate advances the saved date.
func (u *UpdatesStateStore) SetUpdatesDate(ctx context.Context, userID int64, date int) error {
	return u.updateStateColumn(ctx, userID, "date", date)
}

// SetUpdatesSeq advances the saved seq.
func (u *UpdatesStateStore) SetUpdatesSeq(ctx context.Context, userID int64, seq int) error {
	return u.updateStateColumn(ctx, userID, "seq", seq)
}

// SetUpdatesDateSeq advances the saved date and seq together.
func (u *UpdatesStateStore) SetUpdatesDateSeq(ctx context.Context, userID int64, date, seq int) error {
	res, err := u.st.db.ExecContext(ctx,
		"UPDATE updates_state SET date = ?, seq = ?, updated_at = ? WHERE user_id = ?",
		date, seq, time.Now().Unix(), userID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return errors.New("updates state for user does not exist: date/seq")
	}
	return nil
}

// GetChannelPts returns the saved channel-local pts. A zero pts means the row
// only carries an access hash: the updates manager would boot the channel's
// sequence box from 0 and its very first getChannelDifference fails with
// PERSISTENT_TIMESTAMP_EMPTY, killing that channel's worker. Reporting such
// rows as missing lets the manager derive the starting pts from the first
// channel update instead.
func (u *UpdatesStateStore) GetChannelPts(ctx context.Context, userID, channelID int64) (int, bool, error) {
	var pts int
	err := u.st.db.QueryRowContext(ctx,
		"SELECT pts FROM updates_channel_state WHERE user_id = ? AND channel_id = ?", userID, channelID).Scan(&pts)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if pts <= 0 {
		return 0, false, nil
	}
	return pts, true, nil
}

// SetChannelPts upserts the channel-local pts, preserving the stored access
// hash.
func (u *UpdatesStateStore) SetChannelPts(ctx context.Context, userID, channelID int64, pts int) error {
	_, err := u.st.db.ExecContext(ctx, `
		INSERT INTO updates_channel_state (user_id, channel_id, access_hash, pts)
		VALUES (?, ?, 0, ?)
		ON CONFLICT(user_id, channel_id) DO UPDATE SET pts = excluded.pts`,
		userID, channelID, pts)
	return err
}

// ForEachUpdatesChannels iterates every tracked channel of userID until f
// returns an error. Rows are drained before f runs: the store uses a single
// connection, so a callback that queries again would otherwise wait for the
// connection its own iteration still holds. Hash-only rows (pts = 0) are
// skipped for the same reason GetChannelPts reports them missing.
func (u *UpdatesStateStore) ForEachUpdatesChannels(ctx context.Context, userID int64, f func(ctx context.Context, channelID int64, pts int) error) error {
	rows, err := u.st.db.QueryContext(ctx,
		"SELECT channel_id, pts FROM updates_channel_state WHERE user_id = ? AND pts > 0", userID)
	if err != nil {
		return err
	}
	var channels []struct {
		id  int64
		pts int
	}
	for rows.Next() {
		var c struct {
			id  int64
			pts int
		}
		if err := rows.Scan(&c.id, &c.pts); err != nil {
			rows.Close()
			return err
		}
		channels = append(channels, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, c := range channels {
		if err := f(ctx, c.id, c.pts); err != nil {
			return err
		}
	}
	return nil
}

// UpdatesChannel is one stored channel row: identity plus the last known
// channel-local pts (zero when the row only carries an access hash).
type UpdatesChannel struct {
	ChannelID  int64
	AccessHash int64
	Pts        int
}

// Channels returns every stored channel row for userID, including hash-only
// rows that ForEachUpdatesChannels deliberately hides from the updates
// manager. The watch daemon uses it to repair stale pts values before the
// manager loads its state.
func (u *UpdatesStateStore) Channels(ctx context.Context, userID int64) ([]UpdatesChannel, error) {
	rows, err := u.st.db.QueryContext(ctx,
		"SELECT channel_id, access_hash, pts FROM updates_channel_state WHERE user_id = ? AND access_hash != 0", userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var channels []UpdatesChannel
	for rows.Next() {
		var c UpdatesChannel
		if err := rows.Scan(&c.ChannelID, &c.AccessHash, &c.Pts); err != nil {
			return nil, err
		}
		channels = append(channels, c)
	}
	return channels, rows.Err()
}

// SetChannelAccessHash upserts a channel access hash without touching pts.
func (u *UpdatesStateStore) SetChannelAccessHash(ctx context.Context, userID, channelID, accessHash int64) error {
	_, err := u.st.db.ExecContext(ctx, `
		INSERT INTO updates_channel_state (user_id, channel_id, access_hash, pts)
		VALUES (?, ?, ?, 0)
		ON CONFLICT(user_id, channel_id) DO UPDATE SET access_hash = excluded.access_hash`,
		userID, channelID, accessHash)
	return err
}

// GetChannelAccessHash returns a stored channel access hash.
func (u *UpdatesStateStore) GetChannelAccessHash(ctx context.Context, userID, channelID int64) (int64, bool, error) {
	var hash int64
	err := u.st.db.QueryRowContext(ctx,
		"SELECT access_hash FROM updates_channel_state WHERE user_id = ? AND channel_id = ?", userID, channelID).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return hash, true, nil
}

// SetUserAccessHash upserts a user access hash.
func (u *UpdatesStateStore) SetUserAccessHash(ctx context.Context, userID, targetUserID, accessHash int64) error {
	_, err := u.st.db.ExecContext(ctx, `
		INSERT INTO updates_user_hash (user_id, target_user_id, access_hash)
		VALUES (?, ?, ?)
		ON CONFLICT(user_id, target_user_id) DO UPDATE SET access_hash = excluded.access_hash`,
		userID, targetUserID, accessHash)
	return err
}

// GetUserAccessHash returns a stored user access hash.
func (u *UpdatesStateStore) GetUserAccessHash(ctx context.Context, userID, targetUserID int64) (int64, bool, error) {
	var hash int64
	err := u.st.db.QueryRowContext(ctx,
		"SELECT access_hash FROM updates_user_hash WHERE user_id = ? AND target_user_id = ?", userID, targetUserID).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return hash, true, nil
}
