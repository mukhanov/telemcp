package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	"github.com/mukhanov/telemcp/internal/telegram"
)

// loginFlow is the seam the login tests stub out.
var loginFlow = telegram.Login

// runLogin authorizes a fresh telemcp-owned Telegram session interactively:
// --session picks the session file, --api-id/--api-hash override the app
// credentials, and the telegram login flow prompts on stdout while reading
// answers from stdin.
func (r *runtime) runLogin(args []string) error {
	fs := flag.NewFlagSet("telemcp login", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	session := fs.String("session", telegram.DefaultSessionPath(), "")
	apiID := fs.Int64("api-id", 0, "")
	apiHash := fs.String("api-hash", "", "")
	if err := parseFlagsOnly(fs, args); err != nil {
		return err
	}
	if err := applyCredentialFlags(*apiID, *apiHash); err != nil {
		return err
	}
	return loginFlow(r.ctx, telegram.LoginOptions{
		SessionPath: *session,
		In:          os.Stdin,
		Out:         r.stdout,
	})
}

// applyCredentialFlags maps explicit --api-id/--api-hash onto the
// TELEMCP_API_ID/TELEMCP_API_HASH environment variables the telegram package
// resolves credentials from. Both empty (the zero flags) leaves the
// environment — and with it the borrowed Desktop defaults — alone.
func applyCredentialFlags(apiID int64, apiHash string) error {
	if apiID == 0 && apiHash == "" {
		return nil
	}
	if apiID == 0 || apiHash == "" {
		return usageErr(errors.New("--api-id and --api-hash must be set together"))
	}
	if apiID <= 0 || apiID > math.MaxInt32 {
		return usageErr(fmt.Errorf("--api-id must be between 1 and %d", int64(math.MaxInt32)))
	}
	if err := os.Setenv("TELEMCP_API_ID", strconv.FormatInt(apiID, 10)); err != nil {
		return err
	}
	return os.Setenv("TELEMCP_API_HASH", apiHash)
}
