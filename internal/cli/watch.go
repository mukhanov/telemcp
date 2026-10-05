package cli

import (
	"context"
	"errors"
	"flag"
	"io"
	"time"

	"telemcp/internal/store"
	"telemcp/internal/telegram"
)

// runWatch starts the resident live-sync daemon: incoming updates are merged
// into the archive as they arrive, and periodic full passes cover everything
// the live path defers (edits, counters, media, chat metadata).
func (r *runtime) runWatch(args []string) error {
	fs := flag.NewFlagSet("telemcp watch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("path", r.source, "")
	dialogsLimit := fs.Int("dialogs-limit", 200, "")
	messagesLimit := fs.Int("messages-limit", 500, "")
	fetchMedia := fs.Bool("fetch-media", false, "")
	fetchMediaMaxAge := fs.Duration("fetch-media-max-age", 0, "")
	fetchMediaMaxMB := fs.Int64("fetch-media-max-mb", 0, "")
	reconcileEvery := fs.Duration("reconcile-every", 30*time.Minute, "")
	if err := parseFlagsOnly(fs, args); err != nil {
		return err
	}
	if *fetchMediaMaxAge < 0 {
		return usageErr(errors.New("--fetch-media-max-age must be non-negative"))
	}
	if *fetchMediaMaxMB < 0 || *fetchMediaMaxMB > (1<<63-1)/(1024*1024) {
		return usageErr(errors.New("--fetch-media-max-mb must be between 0 and 8796093022207"))
	}
	if *reconcileEvery < 0 {
		return usageErr(errors.New("--reconcile-every must be non-negative (0 disables periodic passes)"))
	}
	return r.withStore(func(st *store.Store) error {
		mediaCache := &mediaRefCache{}
		var (
			existingMediaSourcePath string
			existingMediaRefs       []telegram.ExistingMediaRef
		)
		if *fetchMedia {
			var err error
			existingMediaSourcePath, existingMediaRefs, err = existingMediaRefsForImport(r.ctx, st, mediaCache)
			if err != nil {
				return err
			}
		}
		hooks := telegram.WatchHooks{
			Live: func(ctx context.Context, st *store.Store, stats store.ImportStats, chats []store.Chat, messages []store.Message) error {
				return st.MergeAll(ctx, stats, nil, chats, nil, nil, nil, messages)
			},
			Reconcile: func(ctx context.Context, st *store.Store, result telegram.ImportResult, mediaStage string) error {
				return r.mergeImportResult(st, &result, mediaStage, mediaCache)
			},
		}
		err := telegram.Watch(r.ctx, telegram.WatchOptions{
			ImportOptions: telegram.ImportOptions{
				Path:                    *path,
				DialogsLimit:            *dialogsLimit,
				MessagesLimit:           *messagesLimit,
				FetchMedia:              *fetchMedia,
				FetchMediaMaxAge:        *fetchMediaMaxAge,
				FetchMediaMaxBytes:      *fetchMediaMaxMB * 1024 * 1024,
				Progress:                r.stderr,
				ExistingMediaSourcePath: existingMediaSourcePath,
				ExistingMediaRefs:       existingMediaRefs,
			},
			ReconcileEvery: *reconcileEvery,
		}, st, hooks)
		if errors.Is(err, context.Canceled) {
			return nil // graceful shutdown (SIGINT/SIGTERM): not an error
		}
		return err
	})
}
