package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

func checkOCICronBackup(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, nodes map[string]corev1.Node, probe ociWorkload, setCleanup func(func())) {
	checkOCIFSRecovery(ctx, client, k8s, config, target, nodes[target.Masters[0].ID], probe, ociBackupScheduled, setCleanup)
}

func checkOCIFSBackupSchedule(ctx context.Context, client *kc.Client, target *corev1.Cluster, manual corev1.Backup, dir string) {
	endpoint := client.Scheme() + "://" + client.Host() + "/api/core.kubeclipper.io/v1/cronbackups"
	request := func(ctx context.Context, method, path string, body *corev1.CronBackup, out any) error {
		var payload []byte
		if body != nil {
			var err error
			payload, err = json.Marshal(body)
			if err != nil {
				return err
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint+path, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+client.Token())
		req.Header.Set("Content-Type", "application/json")
		res, err := client.HTTPClient().Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("cron backup %s %s returned HTTP %d", method, path, res.StatusCode)
		}
		if out != nil {
			return json.NewDecoder(res.Body).Decode(out)
		}
		return nil
	}
	oneShotAt := metav1.NewTime(time.Now().Add(20 * time.Second))
	oneShot := corev1.CronBackup{
		ObjectMeta: metav1.ObjectMeta{Name: target.Name + "-once"},
		Spec:       corev1.CronBackupSpec{ClusterName: target.Name, RunAt: &oneShotAt},
	}
	periodic := corev1.CronBackup{
		ObjectMeta: metav1.ObjectMeta{Name: target.Name + "-minute"},
		Spec:       corev1.CronBackupSpec{ClusterName: target.Name, Schedule: "* * * * *", MaxBackupNum: 2},
	}
	create := func(spec *corev1.CronBackup) corev1.CronBackup {
		var created corev1.CronBackup
		Expect(request(ctx, http.MethodPost, "", spec, &created)).To(Succeed())
		Expect(created.Name).To(Equal(spec.Name))
		Expect(created.UID).NotTo(BeEmpty())
		return created
	}
	list := func(job corev1.CronBackup) ([]corev1.Backup, error) {
		var backups []corev1.Backup
		err := request(ctx, http.MethodGet, "/"+job.Name+"/backups", nil, &backups)
		return backups, err
	}
	checkSnapshot := func(job corev1.CronBackup, backup corev1.Backup) error {
		if !slices.ContainsFunc(backup.OwnerReferences, func(owner metav1.OwnerReference) bool { return owner.UID == job.UID }) {
			return fmt.Errorf("backup %s does not belong to cron UID %s", backup.Name, job.UID)
		}
		if backup.Status.ClusterBackupStatus != corev1.ClusterBackupAvailable {
			return fmt.Errorf("backup %s is %s", backup.Name, backup.Status.ClusterBackupStatus)
		}
		if backup.Status.FileName != backup.Name {
			return fmt.Errorf("snapshot filename does not match backup %s", backup.Name)
		}
		file, err := os.Stat(filepath.Join(dir, backup.Status.FileName))
		if err != nil {
			return err
		}
		if file.Size() <= 0 || file.Size() != backup.Status.BackupFileSize || backup.Status.BackupFileMD5 == "" {
			return fmt.Errorf("backup %s has incomplete snapshot metadata", backup.Name)
		}
		return nil
	}
	stop := func(name string) {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		Expect(request(cleanup, http.MethodDelete, "/"+name, nil, nil)).To(Succeed())
	}
	ginkgo.By("[2.5-05] Waiting for one scheduled real snapshot")
	defer stop(oneShot.Name)
	once := create(&oneShot)
	var onceBackup corev1.Backup
	Eventually(func() error {
		backups, err := list(once)
		if err != nil {
			return err
		}
		if len(backups) != 1 {
			return fmt.Errorf("one-shot backup count is %d", len(backups))
		}
		onceBackup = backups[0]
		return checkSnapshot(once, onceBackup)
	}, time.Minute, 2*time.Second).Should(Succeed())

	ginkgo.By("[2.5-06, 2.5-08] Observing at least three distinct periodic backups and the removal of the oldest")
	defer stop(periodic.Name)
	job := create(&periodic)
	seen := map[string]corev1.Backup{}
	var latest []corev1.Backup
	Eventually(func() error {
		backups, err := list(job)
		if err != nil {
			return err
		}
		for _, backup := range backups {
			if err := checkSnapshot(job, backup); err == nil {
				seen[backup.Name] = backup
			}
		}
		if len(backups) != 2 || len(seen) < 3 {
			return fmt.Errorf("seen %d available snapshots, %d records remain", len(seen), len(backups))
		}
		for _, backup := range backups {
			if err := checkSnapshot(job, backup); err != nil {
				return err
			}
		}
		for name := range seen {
			if !slices.ContainsFunc(backups, func(backup corev1.Backup) bool { return backup.Name == name }) {
				if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
					return fmt.Errorf("rotated snapshot %s still exists or cannot be checked: %v", name, err)
				}
			}
		}
		latest = backups
		return nil
	}, 5*time.Minute, 2*time.Second).Should(Succeed())

	ginkgo.By("[2.5-07] Disabling for an entire trigger window and observing a new snapshot after enable")
	var disabled corev1.CronBackup
	Expect(request(ctx, http.MethodPatch, "/"+job.Name+"/disable", nil, &disabled)).To(Succeed())
	Expect(disabled.Labels).To(HaveKey(common.LabelCronBackupDisable))
	Expect(disabled.Status.NextScheduleTime).NotTo(BeNil())
	window := time.Until(disabled.Status.NextScheduleTime.Time) + 5*time.Second
	Expect(window).To(BeNumerically(">", 0))
	Consistently(func() ([]string, error) {
		backups, err := list(job)
		var names []string
		for _, backup := range backups {
			names = append(names, backup.Name)
		}
		return names, err
	}, window, 2*time.Second).Should(ConsistOf(latest[0].Name, latest[1].Name))
	Expect(request(ctx, http.MethodPatch, "/"+job.Name+"/enable", nil, nil)).To(Succeed())
	Eventually(func() error {
		backups, err := list(job)
		if err != nil {
			return err
		}
		for _, backup := range backups {
			if _, existed := seen[backup.Name]; !existed && checkSnapshot(job, backup) == nil {
				return nil
			}
		}
		return fmt.Errorf("no new available snapshot after enable")
	}, 2*time.Minute, 2*time.Second).Should(Succeed())
	again, err := list(once)
	Expect(err).NotTo(HaveOccurred())
	Expect(again).To(HaveLen(1))
	Expect(again[0].UID).To(Equal(onceBackup.UID))
	saved, err := client.DescribeBackup(ctx, manual.Name)
	Expect(err).NotTo(HaveOccurred())
	Expect(saved.Items).To(HaveLen(1))
	Expect(saved.Items[0].UID).To(Equal(manual.UID))
	_, err = os.Stat(filepath.Join(dir, manual.Name))
	Expect(err).NotTo(HaveOccurred())
}
