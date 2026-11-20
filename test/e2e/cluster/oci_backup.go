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

package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	apierror "github.com/kubeclipper/kubeclipper/pkg/errors"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sysutil"
)

type ociBackupMode int

const (
	ociBackupRestore ociBackupMode = iota
	ociBackupReplay
	ociBackupDeleteRetry
	ociBackupScheduled
)

func checkOCIFSRecovery(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, agent corev1.Node, probe ociWorkload, mode ociBackupMode, setCleanup func(func())) {
	host, err := sysutil.HostInfo()
	Expect(err).NotTo(HaveOccurred(), "FS evidence must be read on the assigned Linux Agent")
	Expect(host.HostID).NotTo(BeEmpty())
	Expect(host.HostID).To(Equal(agent.Status.NodeInfo.HostID))
	dir, err := os.MkdirTemp("", target.Name+"-backup-")
	Expect(err).NotTo(HaveOccurred())
	point := initBackUpPoint(target.Name + "-fs-" + string(uuid.NewUUID())[:8])
	point.FsConfig.BackupRootDir = dir

	const timeout = time.Duration(corev1.DefaultBackupTimeoutSec) * time.Second
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*timeout)
		defer cancel()
		err := wait.PollImmediateWithContext(cleanupCtx, 2*time.Second, timeout, func(ctx context.Context) (bool, error) {
			backups, err := client.ListBackupsWithCluster(ctx, target.Name)
			if err != nil {
				return false, err
			}
			empty := true
			for _, backup := range backups.Items {
				if backup.BackupPointName != point.Name {
					continue
				}
				empty = false
				switch backup.Status.ClusterBackupStatus {
				case corev1.ClusterBackupDeleting, corev1.ClusterBackupCreating, corev1.ClusterBackupRestoring:
					continue
				case corev1.ClusterBackupDeleteFailed:
					if mode != ociBackupDeleteRetry {
						return false, fmt.Errorf("snapshot deletion failed for %s", backup.Name)
					}
				}
				if err := client.DeleteBackup(ctx, target.Name, backup.Name); err != nil && !apierror.IsNotFound(err) {
					return false, err
				}
			}
			return empty, nil
		})
		Expect(err).NotTo(HaveOccurred(), "cleanup of snapshots at %s", point.Name)
		latest, err := client.DescribeCluster(cleanupCtx, target.Name)
		if !apierror.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
			current := latest.Items[0]
			if current.Labels[common.LabelBackupPoint] == point.Name {
				delete(current.Labels, common.LabelBackupPoint)
				Expect(client.UpdateCluster(cleanupCtx, &current)).To(Succeed())
			}
		}
		Expect(client.DeleteBackupPoint(cleanupCtx, point.Name)).To(Succeed())
		Expect(os.Remove(dir)).To(Succeed(), "remove the empty test backup directory %s", dir)
	}
	setCleanup(cleanup)

	ginkgo.By("[2.5-01] Creating an FS backup point and checking storage type admission")
	invalid := point.DeepCopy()
	invalid.Name += "-invalid"
	invalid.StorageType = "unsupported"
	_, err = client.CreateBackupPoint(ctx, invalid)
	Expect(apierror.CodeForError(err)).To(Equal(int32(http.StatusBadRequest)))

	created, err := client.CreateBackupPoint(ctx, point)
	Expect(err).NotTo(HaveOccurred())
	Expect(created.Items).To(HaveLen(1))
	Expect(created.Items[0].Name).To(Equal(point.Name))

	fresh, err := client.DescribeCluster(ctx, target.Name)
	Expect(err).NotTo(HaveOccurred())
	current := fresh.Items[0]
	SetClusterBackupPoint(point.Name)(&current)
	Expect(client.UpdateCluster(ctx, &current)).To(Succeed())
	if mode == ociBackupDeleteRetry {
		checkOCIFSCreateFailure(ctx, client, target, agent, point.Name, dir, timeout)
		probe.check(ctx, k8s, config, "before")
	}

	ginkgo.By("Taking an etcd snapshot and checking its real bytes")
	request := initBackup(&agent, "snapshot", point.Name)
	var backup corev1.Backup
	if mode == ociBackupReplay {
		backup = submitOCIBackupReplay(ctx, client, target.Name, request)
	} else {
		response, err := client.CreateBackup(ctx, target.Name, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Items).To(HaveLen(1))
		backup = response.Items[0]
	}
	backupName := backup.Name
	Expect(backupName).NotTo(BeEmpty())
	Expect(backup.BackupPointName).To(Equal(point.Name))
	Expect(backup.UID).NotTo(BeEmpty())
	op := backup.Labels[common.LabelOperationName]
	Expect(op).NotTo(BeEmpty())
	waitForOCIOperation(ctx, client, target.UID, corev1.OperationBackupCluster, op, timeout)
	Eventually(func() error {
		fresh, err := client.DescribeBackup(ctx, backupName)
		if err != nil {
			return err
		}
		backup = fresh.Items[0]
		if backup.Status.ClusterBackupStatus != corev1.ClusterBackupAvailable {
			return fmt.Errorf("backup %s is %s", backupName, backup.Status.ClusterBackupStatus)
		}
		return nil
	}, time.Minute, 2*time.Second).Should(Succeed())
	Expect(backup.Status.FileName).To(Equal(backupName))
	file, err := os.Open(filepath.Join(dir, backup.Status.FileName))
	Expect(err).NotTo(HaveOccurred())
	hash := md5.New() // Compare the checksum reported by the backup API.
	size, err := io.Copy(hash, file)
	closeErr := file.Close()
	Expect(err).NotTo(HaveOccurred())
	Expect(closeErr).NotTo(HaveOccurred())
	Expect(size).To(BeNumerically(">", 0))
	Expect(backup.Status.BackupFileSize).To(Equal(size))
	Expect(backup.Status.BackupFileMD5).To(Equal(fmt.Sprintf("%x", hash.Sum(nil))))
	fmt.Fprintf(ginkgo.GinkgoWriter, "backup=%s backupUID=%s size=%d checksum=%s\n", backup.Name, backup.UID, size, backup.Status.BackupFileMD5)
	if mode == ociBackupReplay {
		backups, err := client.ListBackupsWithCluster(ctx, target.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(backups.Items).To(HaveLen(1))
		Expect(backups.Items[0].UID).To(Equal(backup.UID))
		ops, err := client.ListOperations(ctx, kc.OperationListOptions{FieldSelector: "spec.targetRef.uid=" + string(target.UID)})
		Expect(err).NotTo(HaveOccurred())
		var names []string
		for _, operation := range ops.Items {
			if operation.Spec.Action == corev1.OperationBackupCluster {
				names = append(names, operation.Name)
			}
		}
		Expect(names).To(Equal([]string{op}))
		files, err := os.ReadDir(dir)
		Expect(err).NotTo(HaveOccurred())
		Expect(files).To(ConsistOf(WithTransform(func(file os.DirEntry) string { return file.Name() }, Equal(backup.Status.FileName))))
	}

	ginkgo.By("[2.4-04] Checking normal cluster deletion is rejected while a backup remains")
	err = client.DeleteClusterWithQuery(ctx, target.Name, nil)
	Expect(apierror.CodeForError(err)).To(Equal(int32(http.StatusBadRequest)))
	Expect(err.Error()).To(ContainSubstring("delete the cluster backup file first"))
	if mode == ociBackupScheduled {
		checkOCIFSBackupSchedule(ctx, client, target, backup, dir)
		probe.check(ctx, k8s, config, "before")
		cleanup()
		setCleanup(nil)
		return
	}
	if mode == ociBackupDeleteRetry {
		checkOCIFSDeleteRetry(ctx, client, target, agent, backup, dir, timeout)
		probe.check(ctx, k8s, config, "before")
		cleanup()
		setCleanup(nil)
		return
	}

	ginkgo.By("[2.5-03] Changing the application marker and restoring the selected backup")
	markers := k8s.CoreV1().ConfigMaps(probe.namespace)
	marker, err := markers.Get(ctx, "marker", metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())
	Expect(marker.Data["value"]).To(Equal("before"))
	marker.Data["value"] = "after"
	_, err = markers.Update(ctx, marker, metav1.UpdateOptions{})
	Expect(err).NotTo(HaveOccurred())
	probe.check(ctx, k8s, config, "after")

	recovery, err := client.CreateRecovery(ctx, target.Name, initRecovery(backupName))
	Expect(err).NotTo(HaveOccurred())
	Expect(recovery.UseBackupName).To(Equal(backupName))
	op = recovery.Labels[common.LabelOperationName]
	Expect(op).NotTo(BeEmpty())
	waitForOCIOperation(ctx, client, target.UID, corev1.OperationRecoverCluster, op, time.Duration(corev1.DefaultRecoveryTimeoutSec)*time.Second)
	waitForOCIClusterRunning(ctx, client, target.Name, backup.Status.KubernetesVersion, 2*time.Minute)
	probe.check(ctx, k8s, config, "before")
	cleanup()
	setCleanup(nil)
}

func checkOCIBackupReplay(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, nodes map[string]corev1.Node, probe ociWorkload, setCleanup func(func())) {
	checkOCIFSRecovery(ctx, client, k8s, config, target, nodes[target.Masters[0].ID], probe, ociBackupReplay, setCleanup)
}

func checkOCIBackupDeleteRetry(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, nodes map[string]corev1.Node, probe ociWorkload, setCleanup func(func())) {
	checkOCIFSRecovery(ctx, client, k8s, config, target, nodes[target.Masters[0].ID], probe, ociBackupDeleteRetry, setCleanup)
}

func checkOCIFSCreateFailure(ctx context.Context, client *kc.Client, target *corev1.Cluster, agent corev1.Node, point, dir string, timeout time.Duration) {
	ginkgo.By("[2.5-10] Failing a real backup against an unavailable FS directory")
	held := dir + ".held"
	Expect(os.Rename(dir, held)).To(Succeed())
	defer func() {
		Expect(os.Rename(held, dir)).To(Succeed())
	}()
	created, err := client.CreateBackup(ctx, target.Name, initBackup(&agent, "unavailable", point))
	Expect(err).NotTo(HaveOccurred())
	Expect(created.Items).To(HaveLen(1))
	backup := created.Items[0]
	opName := backup.Labels[common.LabelOperationName]
	Expect(opName).NotTo(BeEmpty())
	Eventually(func() error {
		current, err := client.DescribeBackup(ctx, backup.Name)
		if err != nil {
			return err
		}
		state := current.Items[0].Status
		if state.ClusterBackupStatus != corev1.ClusterBackupError || state.BackupFileSize != 0 || state.BackupFileMD5 != "" {
			return fmt.Errorf("expected an unusable backup without snapshot metadata, got phase %s size %d", state.ClusterBackupStatus, state.BackupFileSize)
		}
		return nil
	}, timeout, 2*time.Second).Should(Succeed())
	checkOCIFSFailedTask(ctx, client, target, agent, opName, corev1.OperationBackupCluster, "directory does not exist")
	files, err := os.ReadDir(held)
	Expect(err).NotTo(HaveOccurred())
	Expect(files).To(BeEmpty(), "the unavailable backend must contain no snapshot")
}

func checkOCIFSFailedTask(ctx context.Context, client *kc.Client, target *corev1.Cluster, agent corev1.Node, name, action, reason string) *operations.Operation {
	op, err := client.DescribeOperation(ctx, name)
	Expect(err).NotTo(HaveOccurred())
	Expect(op.Spec.TargetRef.UID).To(Equal(target.UID))
	Expect(op.Spec.Action).To(Equal(action))
	Expect(op.Status.Phase).To(Equal(operations.OperationFailed))
	tasks, err := client.ListOperationTasks(ctx, string(op.UID))
	Expect(err).NotTo(HaveOccurred())
	Expect(tasks.Items).To(HaveLen(1))
	task := tasks.Items[0]
	Expect(task.Spec.NodeRef.UID).To(Equal(agent.UID))
	Expect(task.Status.Phase).To(Equal(operations.TaskFailed))
	Expect(task.Status.Result).NotTo(BeNil())
	Expect(task.Status.Result.Message).To(ContainSubstring(reason))
	fmt.Fprintf(ginkgo.GinkgoWriter, "failed action=%s operationUID=%s taskUID=%s node=%s reason=%s\n", action, op.UID, task.UID, task.Spec.NodeRef.Name, reason)
	return op
}

func checkOCIFSDeleteRetry(ctx context.Context, client *kc.Client, target *corev1.Cluster, agent corev1.Node, backup corev1.Backup, dir string, timeout time.Duration) {
	ginkgo.By("[2.5-04] Preserving the backup and its failed Operation when its real storage deletion fails")
	err := client.DeleteBackupPoint(ctx, backup.BackupPointName)
	Expect(err).To(HaveOccurred())
	Expect(err.Error()).To(ContainSubstring("in use"))

	path := filepath.Join(dir, backup.Status.FileName)
	saved := path + ".saved"
	blocker := filepath.Join(path, "block-delete")
	Expect(os.Rename(path, saved)).To(Succeed())
	repaired := false
	repair := func() {
		if !repaired {
			if err := os.Remove(blocker); !os.IsNotExist(err) {
				Expect(err).NotTo(HaveOccurred())
			}
			if err := os.Remove(path); !os.IsNotExist(err) {
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(os.Rename(saved, path)).To(Succeed())
			repaired = true
		}
	}
	defer repair()
	Expect(os.Mkdir(path, 0700)).To(Succeed())
	Expect(os.WriteFile(blocker, []byte("non-empty"), 0600)).To(Succeed())
	Expect(client.DeleteBackup(ctx, target.Name, backup.Name)).To(Succeed())
	var failedName string
	Eventually(func() error {
		current, err := client.DescribeBackup(ctx, backup.Name)
		if err != nil {
			return err
		}
		if len(current.Items) != 1 || current.Items[0].UID != backup.UID {
			return fmt.Errorf("original backup record was not retained")
		}
		b := current.Items[0]
		if b.Status.ClusterBackupStatus != corev1.ClusterBackupDeleteFailed {
			return fmt.Errorf("backup deletion is %s", b.Status.ClusterBackupStatus)
		}
		failedName = b.Labels[common.LabelDeleteOperationName]
		if failedName == "" {
			return fmt.Errorf("failed deletion has no Operation")
		}
		return nil
	}, timeout, 2*time.Second).Should(Succeed())
	failed := checkOCIFSFailedTask(ctx, client, target, agent, failedName, corev1.OperationDeleteBackup, "directory not empty")
	info, err := os.Stat(saved)
	Expect(err).NotTo(HaveOccurred())
	Expect(info.Size()).To(Equal(backup.Status.BackupFileSize))

	ginkgo.By("[2.5-04] Retrying against the repaired backend and observing the record and file disappear")
	repair()
	Expect(client.DeleteBackup(ctx, target.Name, backup.Name)).To(Succeed())
	var retryName string
	Eventually(func() error {
		result, err := client.ListOperations(ctx, kc.OperationListOptions{FieldSelector: "spec.targetRef.uid=" + string(target.UID)})
		if err != nil {
			return err
		}
		for _, op := range result.Items {
			if op.Spec.Action == corev1.OperationDeleteBackup && op.UID != failed.UID {
				retryName = op.Name
				return nil
			}
		}
		return fmt.Errorf("no new deletion Operation")
	}, time.Minute, 2*time.Second).Should(Succeed())
	waitForOCIOperation(ctx, client, target.UID, corev1.OperationDeleteBackup, retryName, timeout)
	Eventually(func() bool {
		_, err := client.DescribeBackup(ctx, backup.Name)
		return apierror.IsNotFound(err)
	}, time.Minute, 2*time.Second).Should(BeTrue())
	_, err = os.Stat(path)
	Expect(os.IsNotExist(err)).To(BeTrue(), "the actual backup file must be deleted")
}

func submitOCIBackupReplay(ctx context.Context, client *kc.Client, clusterName string, backup *corev1.Backup) corev1.Backup {
	ginkgo.By("[5-13] Replaying concurrent backup requests with the same idempotency key")
	key := string(uuid.NewUUID())
	endpoint := client.Scheme() + "://" + client.Host() + "/api/core.kubeclipper.io/v1/clusters/" + clusterName + "/backups"
	type response struct {
		backup    corev1.Backup
		status    int
		operation string
		err       error
	}
	submit := func(payload []byte) (result response) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			result.err = err
			return result
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+client.Token())
		req.Header.Set("Idempotency-Key", key)
		res, err := client.HTTPClient().Do(req)
		if err != nil {
			result.err = err
			return result
		}
		defer res.Body.Close()
		result.status, result.operation = res.StatusCode, res.Header.Get("X-Operation-Name")
		if result.status == http.StatusOK {
			result.err = json.NewDecoder(res.Body).Decode(&result.backup)
		}
		return result
	}

	body, err := json.Marshal(backup)
	Expect(err).NotTo(HaveOccurred())
	var results [3]response
	var requests sync.WaitGroup
	start := make(chan struct{})
	for i := range results {
		requests.Add(1)
		go func() {
			defer requests.Done()
			<-start
			results[i] = submit(body)
		}()
	}
	close(start)
	requests.Wait()
	first := results[0]
	for _, result := range results {
		Expect(result.err).NotTo(HaveOccurred())
		Expect(result.status).To(Equal(http.StatusOK))
		Expect(result.backup.Name).NotTo(BeEmpty())
		Expect(result.backup.UID).NotTo(BeEmpty())
		Expect(result.operation).NotTo(BeEmpty())
		Expect(result.backup.UID).To(Equal(first.backup.UID))
		Expect(result.backup.Name).To(Equal(first.backup.Name))
		Expect(result.operation).To(Equal(first.operation))
		Expect(result.operation).To(Equal(result.backup.Labels[common.LabelOperationName]))
	}
	changed := backup.DeepCopy()
	changed.Name += "-different"
	body, err = json.Marshal(changed)
	Expect(err).NotTo(HaveOccurred())
	conflict := submit(body)
	Expect(conflict.err).NotTo(HaveOccurred())
	Expect(conflict.status).To(Equal(http.StatusConflict), "the same key cannot authorize a different backup")
	return first.backup
}
