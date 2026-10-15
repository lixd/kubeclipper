package options

import (
	"crypto/rand"
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

func TestAuthenticationOptionsValidateJWTSecret(t *testing.T) {
	for _, tt := range []struct {
		name    string
		secret  string
		wantErr bool
	}{
		{name: "missing", wantErr: true},
		{name: "public default", secret: "kubeclipper", wantErr: true},
		{name: "configured", secret: rand.Text()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewAuthenticateOptions()
			if opts.JwtSecret != "" {
				t.Fatal("authentication options must not provide a default JWT secret")
			}
			opts.JwtSecret = tt.secret
			errs := opts.Validate()
			if (len(errs) > 0) != tt.wantErr {
				t.Fatalf("Validate() errors = %v, want error = %v", errs, tt.wantErr)
			}
		})
	}
}
