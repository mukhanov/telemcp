package telegram

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const (
	telegramFloodWaitMaxRetries  = 3
	telegramFloodWaitMaxDelay    = 5 * time.Minute
	telegramFloodWaitSafetyDelay = time.Second
	telegramFloodWaitRetryBudget = telegramFloodWaitMaxRetries * (telegramFloodWaitMaxDelay + telegramFloodWaitSafetyDelay)
	telegramMediaDownloadTimeout = 3*time.Minute + telegramFloodWaitRetryBudget
)

// telegramRequestInterval spaces the starts of API requests: ten a second.
// Telegram answers a burst with FLOOD_WAIT, and a connection that keeps
// tripping it stops getting answers at all — which the daemon reads as a
// stall. Sequential work (the channel bootstrap, a reconcile pass) stays
// under it by itself; the updates manager starting a request per channel
// does not.
const telegramRequestInterval = 100 * time.Millisecond

type telegramFloodWaitPolicy struct {
	progress io.Writer
	sleep    func(context.Context, time.Duration) error
	// interval is the least time between two request starts; zero disables
	// the pacing.
	interval time.Duration

	mu        sync.Mutex
	gate      sync.Mutex
	nextStart time.Time // earliest start of the next request
}

func newTelegramFloodWaitPolicy(progress io.Writer) *telegramFloodWaitPolicy {
	return &telegramFloodWaitPolicy{
		progress: progress,
		sleep:    telegramFloodWaitSleep,
		interval: telegramRequestInterval,
	}
}

func (p *telegramFloodWaitPolicy) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		for retries := 0; ; retries++ {
			if err := p.pace(ctx, input); err != nil {
				return err
			}
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			delay, ok := tgerr.AsFloodWait(err)
			if !ok {
				return err
			}
			if retries >= telegramFloodWaitMaxRetries {
				return fmt.Errorf("telegram flood-wait retry limit exceeded after %d retries: %v", telegramFloodWaitMaxRetries, err)
			}
			if delay > telegramFloodWaitMaxDelay {
				return fmt.Errorf("telegram flood wait of %s exceeds %s limit: %v", delay, telegramFloodWaitMaxDelay, err)
			}
			wait := delay + telegramFloodWaitSafetyDelay
			p.reportWait(wait, retries+1)
			if err := p.sleep(ctx, wait); err != nil {
				return fmt.Errorf("wait for telegram rate limit: %w", err)
			}
		}
	}
}

// pace holds a request until interval has passed since the previous one
// started. Two kinds of request skip the queue: a download's file parts are
// a stream with a timeout of its own, and the liveness probe must not wait
// behind the updates manager's backlog.
func (p *telegramFloodWaitPolicy) pace(ctx context.Context, input bin.Encoder) error {
	switch input.(type) {
	case *tg.UploadGetFileRequest, *tg.UpdatesGetStateRequest:
		return nil
	}
	if p.interval <= 0 {
		return nil
	}
	p.gate.Lock()
	defer p.gate.Unlock()
	if wait := time.Until(p.nextStart); wait > 0 {
		if err := p.sleep(ctx, wait); err != nil {
			return err
		}
	}
	p.nextStart = time.Now().Add(p.interval)
	return nil
}

func (p *telegramFloodWaitPolicy) reportWait(wait time.Duration, retry int) {
	if p.progress == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, _ = fmt.Fprintf(p.progress, "telemcp: Telegram rate limit; waiting %s before retry %d/%d\n", wait, retry, telegramFloodWaitMaxRetries)
}

func telegramFloodWaitSleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
