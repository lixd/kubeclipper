/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package validation

import (
	"strings"
	"testing"

	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/iam/v1"
)

// The account password policy is the rule the admin initial password already
// had to satisfy; user creation and password changes used to accept anything
// non-empty, so weak passwords were let in (R28).
func TestValidatePassword(t *testing.T) {
	tests := []struct {
		name     string
		password string
		ok       bool
	}{
		{name: "policy compliant", password: "Abcd1234", ok: true},
		{name: "digits and mixed case at the bounds", password: "Abc12345", ok: true},
		{name: "too short", password: "Ab1", ok: false},
		{name: "too long", password: "Abcdefghijklmnop1", ok: false},
		{name: "no digit", password: "Abcdefgh", ok: false},
		{name: "no lower case", password: "ABCD1234", ok: false},
		{name: "no upper case", password: "abcd1234", ok: false},
		{name: "empty", password: "", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePassword(tt.password)
			if tt.ok && err != nil {
				t.Fatalf("ValidatePassword(%q) = %v, want nil", tt.password, err)
			}
			if !tt.ok && err == nil {
				t.Fatalf("ValidatePassword(%q) = nil, want a policy error", tt.password)
			}
		})
	}
}

// A user create carrying a weak password must be rejected by the scheme
// validation itself, whatever the transport (CLI or API).
func TestValidateUserSpecRejectsWeakPassword(t *testing.T) {
	spec := &corev1.UserSpec{Email: "u@example.com", EncryptedPassword: "123456"}
	errs := ValidateUserSpec(spec, nil)
	if len(errs) == 0 {
		t.Fatal("weak password accepted")
	}
	if !strings.Contains(errs[0].Error(), "8-16 characters") {
		t.Fatalf("error %q does not explain the policy", errs[0].Error())
	}
	spec.EncryptedPassword = "Abcd1234"
	if errs := ValidateUserSpec(spec, nil); len(errs) != 0 {
		t.Fatalf("compliant password rejected: %v", errs)
	}
}
