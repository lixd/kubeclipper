/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package tokencontroller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	iamListerV1 "github.com/kubeclipper/kubeclipper/pkg/client/lister/iam/v1"
	ctrl "github.com/kubeclipper/kubeclipper/pkg/controller-runtime"
	"github.com/kubeclipper/kubeclipper/pkg/models/iam"
	iamv1 "github.com/kubeclipper/kubeclipper/pkg/scheme/iam/v1"
)

// staleLister returns a snapshot that never includes the status the reconciler
// just wrote — exactly what the informer cache does for a moment after an
// update.
type staleLister struct {
	iamListerV1.TokenLister
	token *iamv1.Token
}

func (s *staleLister) Get(name string) (*iamv1.Token, error) {
	out := s.token.DeepCopy()
	out.Status = iamv1.TokenStatus{}
	return out, nil
}

type recordingWriter struct {
	iam.TokenWriter
	updates int
	deletes []string
}

func (r *recordingWriter) CreateToken(_ context.Context, token *iamv1.Token) (*iamv1.Token, error) {
	return token, nil
}
func (r *recordingWriter) UpdateToken(_ context.Context, token *iamv1.Token) (*iamv1.Token, error) {
	r.updates++
	return token, nil
}
func (r *recordingWriter) DeleteToken(_ context.Context, name string) error {
	r.deletes = append(r.deletes, name)
	return nil
}

// A token whose TTL has passed must be deleted even when the lister keeps
// returning a copy without status: the old code returned right after writing
// the status, so with a stale read no requeue was ever scheduled and expired
// temporary tokens (MFA codes, rate-limit counters) survived indefinitely (R24).
func TestReconcileDeletesExpiredTokenDespiteStaleLister(t *testing.T) {
	token := &iamv1.Token{}
	token.Name = "fake_sms-13800000000"
	token.CreationTimestamp = metav1.NewTime(time.Now().Add(-5 * time.Minute))
	ttl := int64(60)
	token.Spec.TTL = &ttl

	writer := &recordingWriter{}
	r := &TokenReconciler{TokenLister: &staleLister{token: token}, TokenWriter: writer}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: token.Name}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %s, want the expired token deleted immediately", res.RequeueAfter)
	}
	if len(writer.deletes) != 1 || writer.deletes[0] != token.Name {
		t.Fatalf("deletes = %v, want the expired token to be deleted", writer.deletes)
	}
	if writer.updates != 1 {
		t.Fatalf("updates = %d, want the status written once", writer.updates)
	}
}

// A token still inside its TTL must be requeued for its deadline, again
// without relying on the cached status.
func TestReconcileRequeuesLiveToken(t *testing.T) {
	token := &iamv1.Token{}
	token.Name = "token-1"
	token.CreationTimestamp = metav1.NewTime(time.Now())
	ttl := int64(120)
	token.Spec.TTL = &ttl

	writer := &recordingWriter{}
	r := &TokenReconciler{TokenLister: &staleLister{token: token}, TokenWriter: writer}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: token.Name}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > 2*time.Minute {
		t.Fatalf("RequeueAfter = %s, want a positive delay within the TTL", res.RequeueAfter)
	}
	if len(writer.deletes) != 0 {
		t.Fatalf("deletes = %v, want none for a live token", writer.deletes)
	}
}
