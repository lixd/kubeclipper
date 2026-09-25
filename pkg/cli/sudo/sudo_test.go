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
// prompt spam in four minutes). It must fail fast with the reason instead of
// looping, and it must fail rather than hang when the target is unreachable.
func TestPreCheckErrorFailsFastOnUnreachableHost(t *testing.T) {
	old := time.Now()
	sshConfig := &sshutils.SSH{User: "kcprobe", PkFile: "/nonexistent/id_test"} // non-root; the bogus key path forces the real SSH path (no local fallback)
	streams := options.IOStreams{In: strings.NewReader(""), Out: &strings.Builder{}, ErrOut: &strings.Builder{}}
	// An unresolvable host fails the SSH dial instantly; the refusal must
	// surface as a precheck failure, not a prompt loop.
	err := PreCheckError("sudo", sshConfig, streams, []string{"kc-sudo-test-host.invalid"})
	if err == nil {
		t.Fatal("sudo precheck against a refused host returned nil, want an error")
	}
	if strings.Contains(err.Error(), "a password is required") {
		t.Fatalf("dial failure misclassified as password prompt: %v", err)
	}
	if time.Since(old) > 30*time.Second {
		t.Fatal("precheck spun instead of failing fast")
	}
}
