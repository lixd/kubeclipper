package sudo

import (
	"strings"
	"testing"
	"time"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

// A non-interactive stdin can never answer the sudo password prompt; the
// precheck used to spin there forever burning CPU and log (R21: 1.5GB of
// prompt spam in four minutes). It must fail fast with the reason instead.
func TestPreCheckErrorFailsFastOnNonInteractiveStdin(t *testing.T) {
	old := time.Now()
	sshConfig := &sshutils.SSH{User: "kcprobe"} // non-root, no password
	streams := options.IOStreams{In: strings.NewReader(""), Out: &strings.Builder{}, ErrOut: &strings.Builder{}}
	err := PreCheckError("sudo", sshConfig, streams, []string{"172.16.131.146"})
	if err == nil {
		t.Fatal("non-interactive sudo precheck returned nil, want an error")
	}
	if !strings.Contains(err.Error(), "non-interactive") {
		t.Fatalf("error %q does not mention the non-interactive fallback", err)
	}
	if time.Since(old) > 5*time.Second {
		t.Fatal("precheck spun instead of failing fast")
	}
}
