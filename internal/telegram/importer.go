package telegram

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mukhanov/telemcp/internal/localfile"
	"github.com/mukhanov/telemcp/internal/store"
)

type ImportOptions struct {
	Path string
	// SessionPath authorizes via a dedicated telemcp session file instead of
	// the Telegram Desktop tdata: an explicit path must be an existing
	// session file; empty prefers the default session file
	// (~/.telemcp/session.json) when present and falls back to Path (the
	// tdata directory). WatchOptions inherits the field.
	SessionPath   string
	DialogsLimit  int
	MessagesLimit int
	ChatID        string
	FetchMedia    bool
	// FetchMediaMaxAge skips remote fetches for messages older than this; zero fetches any age.
	FetchMediaMaxAge time.Duration
	// FetchMediaMaxBytes skips remote fetches whose declared size exceeds this; zero fetches any size.
	FetchMediaMaxBytes      int64
	Progress                io.Writer
	ExistingMediaSourcePath string
	ExistingMediaRefs       []ExistingMediaRef
	MediaArchiveDir         string
}

type ExistingMediaRef struct {
	SourcePK   int64  `json:"source_pk"`
	MediaType  string `json:"media_type,omitempty"`
	MediaTitle string `json:"media_title,omitempty"`
	MediaPath  string `json:"media_path"`
	MediaSize  int64  `json:"media_size,omitempty"`
}

type ImportResult struct {
	mediaRoots  []string
	Stats       store.ImportStats
	Contacts    []store.Contact
	Chats       []store.Chat
	Folders     []store.Folder
	FolderChats []store.FolderChat
	Topics      []store.Topic
	Messages    []store.Message
}

func Import(ctx context.Context, opts ImportOptions, dbPath string) (ImportResult, error) {
	// A second connection on the same Telegram auth key would race the watch
	// daemon (AUTH_KEY_DUPLICATED); refuse to start instead.
	if WatchLockHeld(dbPath) {
		return ImportResult{}, errors.New("telemcp watch is running for this archive; stop it before a manual import")
	}
	source, err := ResolveAuthSource(opts.SessionPath)
	if err != nil {
		return ImportResult{}, fmt.Errorf("resolve Telegram auth source: %w", err)
	}
	canonicalPath, err := canonicalImportSourcePath(source.Path)
	if err != nil {
		return ImportResult{}, fmt.Errorf("resolve Telegram source target: %w", err)
	}
	source.Path = canonicalPath
	archiveRoot := mediaArchiveDir(dbPath)
	var verifiedRefs []ExistingMediaRef
	if sameImportSourcePath(opts.ExistingMediaSourcePath, source.Path) {
		for _, ref := range opts.ExistingMediaRefs {
			f, err := localfile.OpenRegular(archiveRoot, ref.MediaPath)
			if err != nil {
				continue
			}
			_ = f.Close()
			verifiedRefs = append(verifiedRefs, ref)
		}
	}
	opts.ExistingMediaRefs = verifiedRefs
	var mediaTempDir string
	if opts.FetchMedia {
		mediaTempDir, err = os.MkdirTemp("", "telemcp-telegram-media-*")
		if err != nil {
			return ImportResult{}, err
		}
		defer func() { _ = os.RemoveAll(mediaTempDir) }()
	}
	result, err := importTDataGo(ctx, source, opts, dbPath, mediaTempDir)
	if err != nil {
		return ImportResult{}, err
	}
	result.Stats.SourcePathCanonical = true
	archiveDir := importMediaArchiveDir(opts, dbPath)
	roots := []string{mediaTempDir}
	if len(verifiedRefs) != 0 {
		roots = append(roots, archiveRoot)
	}
	if err := copyImportedContactAvatars(result.Contacts, archiveDir, roots...); err != nil {
		return ImportResult{}, err
	}
	if err := copyImportedMedia(result.Messages, archiveDir, &result.Stats, roots...); err != nil {
		return ImportResult{}, err
	}
	return result, nil
}

func importMediaArchiveDir(opts ImportOptions, dbPath string) string {
	if path := strings.TrimSpace(opts.MediaArchiveDir); path != "" {
		return path
	}
	return mediaArchiveDir(dbPath)
}

func sourceIdentity(kind string, values ...string) string {
	return store.SourceIdentity(kind, values...)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

type importSource struct {
	path string
}

// resolveImportSource picks the import source: an explicit path, or the
// Telegram Desktop tdata directory by default.
func resolveImportSource(path string) importSource {
	if path == "" {
		path = DefaultPath()
	}
	return importSource{path: path}
}

func sameImportSourcePath(left, right string) bool {
	leftPath, err := canonicalImportSourcePath(left)
	if err != nil {
		return false
	}
	rightPath, err := canonicalImportSourcePath(right)
	if err != nil {
		return false
	}
	return leftPath == rightPath
}

func canonicalImportSourcePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("source path is required")
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(absolute)
}

func mediaArchiveDir(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "media")
}

func copyImportedMedia(messages []store.Message, archiveDir string, stats *store.ImportStats, roots ...string) error {
	type archivedMedia struct {
		path string
		size int64
	}
	copiedSources := make(map[string]archivedMedia)
	archivedFiles := make(map[string]int64)
	for i := range messages {
		sourcePath := strings.TrimSpace(messages[i].MediaPath)
		if sourcePath == "" {
			continue
		}
		archived, ok := copiedSources[sourcePath]
		if !ok {
			archivedPath, size, alreadyArchived, err := existingArchivedMedia(sourcePath, archiveDir)
			if err != nil {
				return err
			}
			if !alreadyArchived {
				archivedPath, size, err = copyMediaFile(sourcePath, archiveDir, roots...)
			}
			if err != nil {
				if isMediaSourceUnavailable(err) {
					copiedSources[sourcePath] = archivedMedia{}
					messages[i].MediaPath = ""
					messages[i].MediaSize = 0
					continue
				}
				return err
			}
			archived = archivedMedia{path: archivedPath, size: size}
			copiedSources[sourcePath] = archived
		}
		messages[i].MediaPath = archived.path
		messages[i].MediaSize = archived.size
		if archived.path == "" {
			continue
		}
		if _, ok := archivedFiles[archived.path]; !ok {
			archivedFiles[archived.path] = archived.size
		}
	}
	if stats != nil {
		for _, size := range archivedFiles {
			stats.MediaFiles++
			stats.MediaBytes += size
		}
	}
	return nil
}

func copyImportedContactAvatars(contacts []store.Contact, archiveDir string, roots ...string) error {
	copiedSources := make(map[string]string)
	for i := range contacts {
		sourcePath := strings.TrimSpace(contacts[i].AvatarPath)
		if sourcePath == "" {
			continue
		}
		archivedPath, ok := copiedSources[sourcePath]
		if !ok {
			path, _, alreadyArchived, err := existingArchivedMedia(sourcePath, archiveDir)
			if err != nil {
				return err
			}
			if !alreadyArchived {
				path, _, err = copyMediaFile(sourcePath, archiveDir, roots...)
			}
			if err != nil {
				if isMediaSourceUnavailable(err) {
					copiedSources[sourcePath] = ""
					contacts[i].AvatarPath = ""
					continue
				}
				return err
			}
			archivedPath = path
			copiedSources[sourcePath] = archivedPath
		}
		contacts[i].AvatarPath = archivedPath
	}
	return nil
}

func existingArchivedMedia(sourcePath, archiveDir string) (string, int64, bool, error) {
	if filepath.Clean(sourcePath) != sourcePath {
		return "", 0, false, errors.New("imported media path contains unresolved traversal")
	}
	sourceAbs, err := filepath.Abs(filepath.Clean(sourcePath))
	if err != nil {
		return "", 0, false, fmt.Errorf("resolve media source: %w", err)
	}
	archiveAbs, err := filepath.Abs(filepath.Clean(archiveDir))
	if err != nil {
		return "", 0, false, fmt.Errorf("resolve media archive: %w", err)
	}
	rel, err := filepath.Rel(archiveAbs, sourceAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", 0, false, nil
	}
	f, err := localfile.OpenRegular(archiveAbs, sourceAbs)
	if os.IsNotExist(err) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return "", 0, false, err
	}
	return sourceAbs, info.Size(), true, nil
}

type mediaSourceUnavailableError struct {
	path string
	err  error
}

func (e mediaSourceUnavailableError) Error() string {
	return fmt.Sprintf("read media %s: %v", e.path, e.err)
}

func (e mediaSourceUnavailableError) Unwrap() error {
	return e.err
}

func isMediaSourceUnavailable(err error) bool {
	var sourceErr mediaSourceUnavailableError
	return errors.As(err, &sourceErr)
}

func copyMediaFile(sourcePath, archiveDir string, roots ...string) (string, int64, error) {
	source, err := openMediaSource(sourcePath, roots)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = source.Close() }()
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		return "", 0, fmt.Errorf("mkdir media archive: %w", err)
	}

	tmp, err := os.CreateTemp(archiveDir, ".media-*")
	if err != nil {
		return "", 0, fmt.Errorf("create media temp: %w", err)
	}
	tmpPath := tmp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(tmp, hash), source)
	closeErr := tmp.Close()
	if copyErr != nil {
		return "", 0, fmt.Errorf("copy media %s: %w", sourcePath, copyErr)
	}
	if closeErr != nil {
		return "", 0, fmt.Errorf("close media temp: %w", closeErr)
	}

	digest := fmt.Sprintf("%x", hash.Sum(nil))
	finalDir := filepath.Join(archiveDir, digest[:2])
	if err := os.MkdirAll(finalDir, 0o700); err != nil {
		return "", 0, fmt.Errorf("mkdir media shard: %w", err)
	}
	finalPath := filepath.Join(finalDir, digest)
	if _, err := os.Stat(finalPath); err == nil {
		return finalPath, size, nil
	} else if !os.IsNotExist(err) {
		return "", 0, fmt.Errorf("stat media archive: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return "", 0, fmt.Errorf("archive media %s: %w", sourcePath, err)
	}
	removeTemp = false
	return finalPath, size, nil
}

func openMediaSource(path string, roots []string) (*os.File, error) {
	for _, root := range roots {
		if root == "" || !localfile.Within(root, path) {
			continue
		}
		f, err := localfile.OpenRegular(root, path)
		if os.IsNotExist(err) {
			return nil, mediaSourceUnavailableError{path: path, err: err}
		}
		return f, err
	}
	return nil, errors.New("imported media is outside the selected cache, download or verified archive roots")
}

func parseTime(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t.UTC()
	}
	return time.Time{}
}
