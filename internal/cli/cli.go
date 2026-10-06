package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mukhanov/telemcp/internal/store"
	"github.com/mukhanov/telemcp/internal/telegram"
)

type cliError struct {
	code int
	err  error
}

func (e *cliError) Error() string {
	return e.err.Error()
}

func (e *cliError) Unwrap() error {
	return e.err
}

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		return 1
	}
	var codeErr *cliError
	if errors.As(err, &codeErr) {
		return codeErr.code
	}
	return 1
}

type runtime struct {
	ctx    context.Context
	stdout io.Writer
	stderr io.Writer
	json   bool
	dbPath string
	source string
}

// Run executes the subcommands (import, watch, login). args includes the
// subcommand name, e.g. []string{"watch", "--reconcile-every", "30m"}.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageErr(errors.New("missing command"))
	}
	global := flag.NewFlagSet("telemcp", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	jsonOut := global.Bool("json", false, "")
	dbPath := global.String("db", defaultDBPath(), "")
	source := global.String("source", "", "")
	if err := global.Parse(args); err != nil {
		return usageErr(err)
	}
	rest := global.Args()
	if len(rest) == 0 {
		return usageErr(errors.New("missing command"))
	}
	r := &runtime{ctx: ctx, stdout: stdout, stderr: stderr, json: *jsonOut, dbPath: *dbPath, source: *source}
	switch rest[0] {
	case "import", "sync":
		return r.runImport(rest[1:])
	case "watch":
		return r.runWatch(rest[1:])
	case "login":
		return r.runLogin(rest[1:])
	default:
		return usageErr(fmt.Errorf("unknown command %q", rest[0]))
	}
}

func (r *runtime) withStore(fn func(*store.Store) error) error {
	st, err := store.Open(r.ctx, r.dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	return fn(st)
}

func parseFlagsOnly(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("%s takes flags only", strings.TrimPrefix(fs.Name(), "telemcp ")))
	}
	return nil
}

// importFlow is the seam the import tests stub out.
var importFlow = telegram.Import

func (r *runtime) runImport(args []string) error {
	fs := flag.NewFlagSet("telemcp import", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("path", r.source, "")
	session := fs.String("session", "", "")
	dialogsLimit := fs.Int("dialogs-limit", 200, "")
	messagesLimit := fs.Int("messages-limit", 500, "")
	fetchMedia := fs.Bool("fetch-media", false, "")
	fetchMediaMaxAge := fs.Duration("fetch-media-max-age", 0, "")
	fetchMediaMaxMB := fs.Int64("fetch-media-max-mb", 0, "")
	if err := parseFlagsOnly(fs, args); err != nil {
		return err
	}
	if *fetchMediaMaxAge < 0 {
		return usageErr(errors.New("--fetch-media-max-age must be non-negative"))
	}
	if *fetchMediaMaxMB < 0 || *fetchMediaMaxMB > (1<<63-1)/(1024*1024) {
		return usageErr(errors.New("--fetch-media-max-mb must be between 0 and 8796093022207"))
	}
	return r.withStore(func(st *store.Store) error {
		mediaStage, err := os.MkdirTemp(filepath.Dir(st.Path()), ".telemcp-import-media-*")
		if err != nil {
			return err
		}
		defer func() { _ = os.RemoveAll(mediaStage) }()
		var existingMediaSourcePath string
		var existingMediaRefs []telegram.ExistingMediaRef
		mediaCache := &mediaRefCache{}
		if *fetchMedia {
			existingMediaSourcePath, existingMediaRefs, err = existingMediaRefsForImport(r.ctx, st, mediaCache)
			if err != nil {
				return err
			}
		}
		result, err := importFlow(r.ctx, telegram.ImportOptions{
			Path:                    *path,
			SessionPath:             *session,
			DialogsLimit:            *dialogsLimit,
			MessagesLimit:           *messagesLimit,
			FetchMedia:              *fetchMedia,
			FetchMediaMaxAge:        *fetchMediaMaxAge,
			FetchMediaMaxBytes:      *fetchMediaMaxMB * 1024 * 1024,
			Progress:                r.stderr,
			ExistingMediaSourcePath: existingMediaSourcePath,
			ExistingMediaRefs:       existingMediaRefs,
			MediaArchiveDir:         mediaStage,
		}, st.Path())
		if err != nil {
			return err
		}
		return r.mergeImportResult(st, &result, mediaStage, mediaCache)
	})
}

// mergeImportResult is the shared import tail: source preparation, merge
// validation, media staging promotion and the store merge. Used by runImport
// and the watch daemon's reconcile passes alike.
func (r *runtime) mergeImportResult(st *store.Store, result *telegram.ImportResult, mediaStage string, mediaCache *mediaRefCache) error {
	if err := prepareImportResultSource(result); err != nil {
		return err
	}
	if err := st.ValidateMergeSource(r.ctx, result.Stats, result.Messages); err != nil {
		return err
	}
	if err := preserveExistingMediaRefs(r.ctx, st, result.Stats.SourcePath, result.Messages, true, mediaCache); err != nil {
		return err
	}
	if err := promoteImportMedia(result, mediaStage, filepath.Join(filepath.Dir(st.Path()), "media")); err != nil {
		return err
	}
	if err := storeImportResultCached(r.ctx, st, result, mediaCache); err != nil {
		return err
	}
	return r.print(result.Stats)
}

func storeImportResultCached(ctx context.Context, st *store.Store, result *telegram.ImportResult, cache *mediaRefCache) error {
	if err := prepareImportResultSource(result); err != nil {
		return err
	}
	if err := preserveExistingMediaRefs(ctx, st, result.Stats.SourcePath, result.Messages, true, cache); err != nil {
		return err
	}
	if err := validateImportMediaRefs(result, filepath.Join(filepath.Dir(st.Path()), "media")); err != nil {
		return err
	}
	refreshImportMediaStats(result)
	return st.MergeAll(ctx, result.Stats, result.Contacts, result.Chats, result.Folders, result.FolderChats, result.Topics, result.Messages)
}

func prepareImportResultSource(result *telegram.ImportResult) error {
	sourcePath := strings.TrimSpace(result.Stats.SourcePath)
	if sourcePath == "" {
		return errors.New("import source path is required")
	}
	if result.Stats.SourcePathCanonical {
		if !filepath.IsAbs(sourcePath) {
			return errors.New("canonical import source path is not absolute")
		}
		result.Stats.SourcePath = filepath.Clean(sourcePath)
		return nil
	}
	sourcePath, err := filepath.Abs(filepath.Clean(sourcePath))
	if err != nil {
		return fmt.Errorf("resolve import source path: %w", err)
	}
	sourcePath, err = filepath.EvalSymlinks(sourcePath)
	if err != nil {
		return fmt.Errorf("resolve import source target: %w", err)
	}
	result.Stats.SourcePath = sourcePath
	result.Stats.SourcePathCanonical = true
	return nil
}

func promoteImportMedia(result *telegram.ImportResult, stageDir, archiveDir string) error {
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		return err
	}
	archiveInfo, err := os.Lstat(archiveDir)
	if err != nil {
		return err
	}
	if !archiveInfo.IsDir() || archiveInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("media archive %q is not a regular directory", archiveDir)
	}
	resolvedArchiveDir, err := filepath.EvalSymlinks(archiveDir)
	if err != nil {
		return err
	}
	promoted := make(map[string]string)
	promote := func(path string) (string, error) {
		path = strings.TrimSpace(path)
		if path == "" {
			return "", nil
		}
		if destination, ok := promoted[path]; ok {
			return destination, nil
		}
		if resolvedPathWithin(resolvedArchiveDir, path) {
			validated, err := validateArchivedMedia(path, resolvedArchiveDir)
			if err != nil {
				return "", err
			}
			promoted[path] = validated
			return validated, nil
		}
		stagedPath := path
		relative, err := filepath.Rel(stageDir, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
			return "", fmt.Errorf("imported media path %q is outside staging directory", path)
		}
		validatedStage, err := validateArchivedMedia(path, stageDir)
		if err != nil {
			return "", err
		}
		path = validatedStage
		digest := filepath.Base(path)
		expectedRelative := filepath.Join(digest[:2], digest)
		if filepath.Clean(relative) != expectedRelative {
			return "", fmt.Errorf("imported media path %q has unexpected archive layout", stagedPath)
		}
		destinationDir := filepath.Join(resolvedArchiveDir, digest[:2])
		if err := os.Mkdir(destinationDir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
		destinationInfo, err := os.Lstat(destinationDir)
		if err != nil {
			return "", err
		}
		if !destinationInfo.IsDir() || destinationInfo.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("media archive directory %q is not a regular directory", destinationDir)
		}
		destination := filepath.Join(destinationDir, digest)
		if err := os.Link(path, destination); err == nil {
			if err := os.Remove(path); err != nil {
				return "", err
			}
			validated, err := validateArchivedMedia(destination, resolvedArchiveDir)
			if err != nil {
				return "", err
			}
			destination = validated
		} else if errors.Is(err, os.ErrExist) {
			validated, err := validateArchivedMedia(destination, resolvedArchiveDir)
			if err != nil {
				return "", err
			}
			stagedDigest, err := fileSHA256(path)
			if err != nil {
				return "", err
			}
			if stagedDigest != filepath.Base(validated) {
				return "", fmt.Errorf("staged media %q does not match existing archive file %q", path, validated)
			}
			if err := os.Remove(path); err != nil {
				return "", err
			}
			destination = validated
		} else {
			return "", err
		}
		promoted[stagedPath] = destination
		return destination, nil
	}
	for i := range result.Messages {
		path, err := promote(result.Messages[i].MediaPath)
		if err != nil {
			return err
		}
		result.Messages[i].MediaPath = path
	}
	for i := range result.Contacts {
		path, err := promote(result.Contacts[i].AvatarPath)
		if err != nil {
			return err
		}
		result.Contacts[i].AvatarPath = path
	}
	return nil
}

func pathWithin(root, path string) bool {
	root, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return false
	}
	path, err = filepath.Abs(filepath.Clean(path))
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))
}

func resolvedPathWithin(root, path string) bool {
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	return pathWithin(resolvedRoot, resolvedPath)
}

func validateArchivedMedia(path, archiveDir string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("archived media %q is not a regular file", path)
	}
	resolvedArchive, err := filepath.EvalSymlinks(archiveDir)
	if err != nil {
		return "", err
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if !pathWithin(resolvedArchive, resolvedPath) {
		return "", fmt.Errorf("archived media %q resolves outside archive directory", path)
	}
	digest, err := fileSHA256(resolvedPath)
	if err != nil {
		return "", err
	}
	if filepath.Base(resolvedPath) != digest || filepath.Base(filepath.Dir(resolvedPath)) != digest[:2] {
		return "", fmt.Errorf("archived media %q does not match its content hash", path)
	}
	return resolvedPath, nil
}

func validateImportMediaRefs(result *telegram.ImportResult, archiveDir string) error {
	for i := range result.Messages {
		path := strings.TrimSpace(result.Messages[i].MediaPath)
		if path == "" {
			continue
		}
		validated, err := validateArchivedMedia(path, archiveDir)
		if err != nil {
			return err
		}
		result.Messages[i].MediaPath = validated
	}
	for i := range result.Contacts {
		path := strings.TrimSpace(result.Contacts[i].AvatarPath)
		if path == "" {
			continue
		}
		validated, err := validateArchivedMedia(path, archiveDir)
		if err != nil {
			return err
		}
		result.Contacts[i].AvatarPath = validated
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func refreshImportMediaStats(result *telegram.ImportResult) {
	result.Stats.MediaMessages = 0
	result.Stats.MediaFiles = 0
	result.Stats.MediaBytes = 0
	mediaFiles := map[string]int64{}
	for _, message := range result.Messages {
		if strings.TrimSpace(message.MediaType) != "" {
			result.Stats.MediaMessages++
		}
		path := strings.TrimSpace(message.MediaPath)
		if path == "" {
			continue
		}
		if _, ok := mediaFiles[path]; !ok {
			mediaFiles[path] = message.MediaSize
		}
	}
	for _, size := range mediaFiles {
		result.Stats.MediaFiles++
		result.Stats.MediaBytes += size
	}
}

type mediaRefCache struct {
	loaded     bool
	sourcePath string
	refs       map[int64]telegram.ExistingMediaRef
	loads      int
}

func (c *mediaRefCache) get(ctx context.Context, st *store.Store) (string, map[int64]telegram.ExistingMediaRef, error) {
	if c != nil && c.loaded {
		return c.sourcePath, c.refs, nil
	}
	sourcePath, refs, err := existingMediaRefs(ctx, st)
	if err != nil {
		return "", nil, err
	}
	if c != nil {
		c.sourcePath = sourcePath
		c.refs = refs
		c.loaded = true
		c.loads++
	}
	return sourcePath, refs, nil
}

func existingMediaRefsForImport(ctx context.Context, st *store.Store, cache *mediaRefCache) (string, []telegram.ExistingMediaRef, error) {
	sourcePath, refsByPK, err := cache.get(ctx, st)
	if err != nil || len(refsByPK) == 0 {
		return sourcePath, nil, err
	}
	refs := make([]telegram.ExistingMediaRef, 0, len(refsByPK))
	for _, ref := range refsByPK {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].SourcePK < refs[j].SourcePK })
	return sourcePath, refs, nil
}

func preserveExistingMediaRefs(ctx context.Context, st *store.Store, sourcePath string, messages []store.Message, allowLegacySource bool, cache *mediaRefCache) error {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return nil
	}
	existingSourcePath, refs, err := cache.get(ctx, st)
	if err != nil || (!allowLegacySource && existingSourcePath != sourcePath) {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	for i := range messages {
		if strings.TrimSpace(messages[i].MediaPath) != "" {
			continue
		}
		ref, ok := refs[messages[i].SourcePK]
		if !ok {
			continue
		}
		if messages[i].MediaType == "" {
			messages[i].MediaType = ref.MediaType
		}
		if messages[i].MediaTitle == "" {
			messages[i].MediaTitle = ref.MediaTitle
		}
		messages[i].MediaPath = ref.MediaPath
		messages[i].MediaSize = ref.MediaSize
	}
	return nil
}

func existingMediaRefs(ctx context.Context, st *store.Store) (string, map[int64]telegram.ExistingMediaRef, error) {
	status, err := st.Status(ctx)
	if err != nil {
		return "", nil, err
	}
	sourcePath := strings.TrimSpace(status.LastSource)
	if sourcePath == "" {
		return "", nil, nil
	}
	existing, err := st.MediaRefs(ctx)
	if err != nil {
		return "", nil, err
	}
	refs := make(map[int64]telegram.ExistingMediaRef)
	for _, msg := range existing {
		path := strings.TrimSpace(msg.MediaPath)
		if path == "" {
			continue
		}
		if info, err := os.Stat(path); err != nil || info.IsDir() {
			continue
		}
		refs[msg.SourcePK] = telegram.ExistingMediaRef{
			SourcePK:   msg.SourcePK,
			MediaType:  msg.MediaType,
			MediaTitle: msg.MediaTitle,
			MediaPath:  path,
			MediaSize:  msg.MediaSize,
		}
	}
	return sourcePath, refs, nil
}

func (r *runtime) print(v any) error {
	enc := json.NewEncoder(r.stdout)
	if r.json {
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	}
	switch value := v.(type) {
	case store.ImportStats:
		if _, err := fmt.Fprintf(r.stdout, "source_path: %s\ndb_path: %s\nchats: %d\nmessages: %d\nmedia_messages: %d\nmedia_files: %d\nmedia_bytes: %d\nstarted_at: %s\nfinished_at: %s\n",
			value.SourcePath, value.DBPath, value.Chats, value.Messages, value.MediaMessages, value.MediaFiles, value.MediaBytes, value.StartedAt.Format(time.RFC3339), value.FinishedAt.Format(time.RFC3339)); err != nil {
			return err
		}
		if hasRemoteMediaStats(value) {
			if _, err := fmt.Fprintf(
				r.stdout,
				"remote_media_candidates: %d\nremote_media_attempted: %d\nremote_media_downloads: %d\nremote_media_missing: %d\nremote_media_unavailable: %d\nremote_media_timeouts: %d\nremote_media_errors: %d\n",
				value.RemoteMediaCandidates,
				value.RemoteMediaAttempted,
				value.RemoteMediaDownloads,
				value.RemoteMediaMissing,
				value.RemoteMediaUnavailable,
				value.RemoteMediaTimeouts,
				value.RemoteMediaErrors,
			); err != nil {
				return err
			}
		}
		return nil
	default:
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	}
}

func hasRemoteMediaStats(stats store.ImportStats) bool {
	return stats.RemoteMediaCandidates != 0 ||
		stats.RemoteMediaAttempted != 0 ||
		stats.RemoteMediaDownloads != 0 ||
		stats.RemoteMediaMissing != 0 ||
		stats.RemoteMediaUnavailable != 0 ||
		stats.RemoteMediaTimeouts != 0 ||
		stats.RemoteMediaErrors != 0
}

func usageErr(err error) error {
	return &cliError{code: 2, err: err}
}

func defaultDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "telemcp.db"
	}
	return filepath.Join(home, ".telemcp", "telemcp.db")
}
