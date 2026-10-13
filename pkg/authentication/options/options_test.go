package options

import (
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/constatns"
)

// A deploy-config that spells an authentication section replaces the decoded
// struct; nested option structs then come back nil and Validate used to
// panic on them (R21 deploy panic).
func TestValidateToleratesNilNestedOptions(t *testing.T) {
	a := &AuthenticationOptions{InitialPassword: constatns.DefaultAdminUserPass}
	// The panic is the defect; surfacing the remaining validation errors
	// (empty JWT secret etc.) is correct behaviour.
	errs := a.Validate()
	_ = errs
	if a.MFAOptions == nil || a.OAuthOptions == nil {
		t.Fatal("Validate did not normalize nil nested options")
	}
}

func TestValidateReportsInvalidInitialPassword(t *testing.T) {
	a := &AuthenticationOptions{InitialPassword: "short"}
	errs := a.Validate()
	if len(errs) == 0 {
		t.Fatal("expected a format error for a weak initial password")
	}
	for _, err := range errs {
		if strings.Contains(err.Error(), a.InitialPassword) {
			t.Fatalf("validation error leaked the rejected password: %q", err)
		}
	}
}
