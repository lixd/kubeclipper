/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emicklei/go-restful"
	"github.com/golang/mock/gomock"
	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	mock "github.com/kubeclipper/kubeclipper/pkg/models/iam/mock"
	iamv1 "github.com/kubeclipper/kubeclipper/pkg/scheme/iam/v1"
)

func notFoundRole(name string) error {
	return apimachineryErrors.NewNotFound(schema.GroupResource{Group: iamv1.GroupName, Resource: "globalroles"}, name)
}

func alreadyExistsRole(name string) error {
	return apimachineryErrors.NewAlreadyExists(schema.GroupResource{Group: iamv1.GroupName, Resource: "globalroles"}, name)
}

func newJSONRequest(t *testing.T, method, target, body string) (*restful.Request, *restful.Response, *httptest.ResponseRecorder) {
	t.Helper()
	httpReq := httptest.NewRequest(method, target, strings.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	recorder := httptest.NewRecorder()
	resp := restful.NewResponse(recorder)
	// A Response built outside the container has no request attached, so it
	// would reject every representation with 406; tell it what the request
	// accepts (restful exposes this for tests).
	resp.SetRequestAccepts("application/json")
	return restful.NewRequest(httpReq), resp, recorder
}

// A duplicate role create used to surface as a 500 while the equivalent user
// create already returned 400; both are the same "already exists" condition (R24).
func TestCreateRolesDuplicateIsBadRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	op := mock.NewMockOperator(ctrl)
	op.EXPECT().CreateRole(gomock.Any(), gomock.Any()).Return(nil, alreadyExistsRole("r24-role"))

	h := newHandler(op, nil, nil)
	req, resp, recorder := newJSONRequest(t, http.MethodPost, "/roles", `{"metadata":{"name":"r24-role"}}`)
	h.CreateRoles(req, resp)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("duplicate role create = HTTP %d, want 400", recorder.Code)
	}
}

// An update body without a resourceVersion used to hit the storage as an
// unconditional write and failed with a 500. The handler must bind it to the
// resourceVersion of the object it just read (R24).
func TestUpdateRoleUsesExistingResourceVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	op := mock.NewMockOperator(ctrl)
	existing := &iamv1.GlobalRole{}
	existing.Name = "r24-role"
	existing.ResourceVersion = "42"
	op.EXPECT().GetRoleEx(gomock.Any(), "r24-role", "0").Return(existing, nil)
	op.EXPECT().UpdateRole(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, role *iamv1.GlobalRole) (*iamv1.GlobalRole, error) {
			if role.ResourceVersion != "42" {
				t.Errorf("UpdateRole resourceVersion = %q, want the existing 42", role.ResourceVersion)
			}
			return role, nil
		})

	h := newHandler(op, nil, nil)
	req, resp, recorder := newJSONRequest(t, http.MethodPut, "/roles/r24-role", `{"metadata":{"name":"r24-role"}}`)
	req.PathParameters()["name"] = "r24-role"
	h.UpdateRole(req, resp)

	if recorder.Code != http.StatusOK {
		t.Fatalf("role update = HTTP %d, want 200 (body %s)", recorder.Code, recorder.Body.String())
	}
}

// Creating a user that points at a role which does not exist used to be
// accepted: the binding was created anyway, dangling, and GET
// /users/{name}/roles later answered 404. Reject it before any object is
// written (R24).
func TestCreateUsersRejectsUnknownRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	op := mock.NewMockOperator(ctrl)
	op.EXPECT().GetRoleEx(gomock.Any(), "r24-nope", "0").Return(nil, notFoundRole("r24-nope"))
	// CreateUser must not be reached; gomock fails the test on an unexpected call.

	h := newHandler(op, nil, nil)
	body := `{"metadata":{"name":"r24-u","annotations":{"iam.kubeclipper.io/role":"r24-nope"}},
		"spec":{"email":"r24@example.com","password":"R24pass1"}}`
	req, resp, recorder := newJSONRequest(t, http.MethodPost, "/users", body)
	h.CreateUsers(req, resp)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unknown role at user create = HTTP %d, want 400 (body %s)", recorder.Code, recorder.Body.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if reason, _ := parsed["reason"].(string); !strings.Contains(reason, "r24-nope") {
		t.Fatalf("error reason %q does not name the missing role (body %s)", parsed["reason"], recorder.Body.String())
	}
}
