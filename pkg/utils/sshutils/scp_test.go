package sshutils

import (
	"path/filepath"
	"strings"
	"testing"
)

// The non-root transit path must stay flat under the temp dir: a middle path
// mirroring the target path would mkdir into a root-owned directory (e.g.
// /tmp/etc from an earlier root join) and fail the whole join (R21).
func TestMiddleFileNameIsFlatAndCollisionFree(t *testing.T) {
	remote := "/etc/kubeclipper-agent/delivery/package-registry.json"
	name := middleFileName(remote)
	if strings.Contains(name, "/") {
		t.Fatalf("middle file name %q is not flat", name)
	}
	if !strings.HasSuffix(name, filepath.Base(remote)) {
		t.Fatalf("middle file name %q lost the base name", name)
	}
	other := middleFileName("/etc/kubeclipper-agent/other.json")
	if name == other {
		t.Fatalf("different remote paths share one middle name: %q", name)
	}
}
