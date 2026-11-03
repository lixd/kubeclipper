/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dnscontroller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	listerv1 "github.com/kubeclipper/kubeclipper/pkg/client/lister/core/v1"
	ctrl "github.com/kubeclipper/kubeclipper/pkg/controller-runtime"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/client"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/manager"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

type dnsTestManager struct {
	manager.Manager
	cluster client.Client
}

func (m dnsTestManager) GetClusterClientSet(string) (client.Client, bool) {
	return m.cluster, true
}

type dnsTestClient struct {
	client.Client
	kube kubernetes.Interface
}

func (c dnsTestClient) Kubernetes() kubernetes.Interface { return c.kube }

func TestDeletedDomainRemovesSyncedDNSRecords(t *testing.T) {
	for _, keepOtherDomain := range []bool{false, true} {
		name := "last-domain"
		if keepOtherDomain {
			name = "preserve-other-domain"
		}
		t.Run(name, func(t *testing.T) {
			domain := func(name, ip string) *v1.Domain {
				return &v1.Domain{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1.DomainSpec{
					SyncCluster: []string{"cluster"}, Records: map[string]v1.Record{"api." + name: {
						Domain: name, RR: "api", ParseRecord: []v1.ParseRecord{{Type: "A", IP: ip}},
					}},
				}}
			}
			deleted := domain("deleted.invalid", "192.0.2.47")
			before := []*v1.Domain{deleted}
			domains := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if keepOtherDomain {
				other := domain("kept.invalid", "192.0.2.48")
				if err := domains.Add(other); err != nil {
					t.Fatal(err)
				}
				before = append(before, other)
			}
			corefile, err := renderCorefile(before, "10.96.0.10", "cluster.local")
			if err != nil {
				t.Fatal(err)
			}
			kube := fake.NewSimpleClientset(
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "coredns", Namespace: "kube-system"}, Data: map[string]string{"Corefile": corefile}},
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "kube-dns", Namespace: "kube-system"}, Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.10"}},
			)
			clusters := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if err := clusters.Add(&v1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster"}, Networking: v1.Networking{DNSDomain: "cluster.local"}}); err != nil {
				t.Fatal(err)
			}
			r := DNSReconciler{DomainLister: listerv1.NewDomainLister(domains), ClusterLister: listerv1.NewClusterLister(clusters),
				mgr: dnsTestManager{cluster: dnsTestClient{kube: kube}}}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: deleted.Name}}); err != nil {
				t.Fatal(err)
			}
			actual, err := kube.CoreV1().ConfigMaps("kube-system").Get(context.Background(), "coredns", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(actual.Data["Corefile"], deleted.Name) {
				t.Fatal("deleted Domain still has records in the cluster CoreDNS configuration")
			}
			if keepOtherDomain && !strings.Contains(actual.Data["Corefile"], "192.0.2.48") {
				t.Fatal("unrelated Domain records were removed")
			}
		})
	}
}
