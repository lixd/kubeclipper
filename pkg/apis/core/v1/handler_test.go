/*
 *
 *  * Copyright 2021 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package v1

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"

	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	mock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
)

func newBackupHandler(t *testing.T, backups []v1.Backup) (*handler, *mock.MockOperator) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := mock.NewMockOperator(ctrl)
	list := &v1.BackupList{Items: backups}
	m.EXPECT().ListBackups(gomock.Any(), gomock.Any()).Return(list, nil).AnyTimes()
	h := newHandler(nil, m, nil, nil, nil, nil, nil)
	return h, m
}

// Regression for the R7 finding: the describe endpoint previously passed
// resourceVersion into GetBackupEx as the backup name, so every existing
// backup was reported as NotFound.
func TestFindBackupExisting(t *testing.T) {
	backups := []v1.Backup{}
	b1 := v1.Backup{}
	b1.Name = "cluster-a-manual-1"
	backups = append(backups, b1)
	b2 := v1.Backup{}
	b2.Name = "cluster-b-cron-2"
	backups = append(backups, b2)

	h, _ := newBackupHandler(t, backups)

	got, err := h.findBackup(context.Background(), "cluster-b-cron-2")
	if err != nil {
		t.Fatalf("findBackup existing backup failed: %v", err)
	}
	if got.Name != "cluster-b-cron-2" {
		t.Fatalf("got backup %q, want cluster-b-cron-2", got.Name)
	}
}

func TestFindBackupNotFound(t *testing.T) {
	backups := []v1.Backup{}
	b1 := v1.Backup{}
	b1.Name = "cluster-a-manual-1"
	backups = append(backups, b1)

	h, _ := newBackupHandler(t, backups)

	_, err := h.findBackup(context.Background(), "no-such-backup")
	if err == nil {
		t.Fatalf("expected NotFound for missing backup")
	}
	if !apimachineryErrors.IsNotFound(err) {
		t.Fatalf("error = %v, want NotFound", err)
	}
}
