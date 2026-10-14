package kubeadm

import (
	"context"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestProviderPreservesBorrowedServiceAccount(t *testing.T) {
	for _, existingOwner := range []string{"", "another-provider"} {
		t.Run(existingOwner, func(t *testing.T) {
			annotations := map[string]string{}
			if existingOwner != "" {
				annotations[providerOwnerAnnotation] = existingOwner
			}
			sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "kc-server", Namespace: "kube-system", UID: "source-sa", Annotations: annotations}, Secrets: []corev1.ObjectReference{{Name: "kc-server-secret"}}}
			binding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "kc-server", UID: "source-binding", Annotations: annotations}}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "kc-server-secret", Namespace: "kube-system"}}
			client := fake.NewSimpleClientset([]runtime.Object{sa, binding, secret}...)
			ctx := context.Background()
			if err := syncProviderServiceAccount(ctx, client, "import-provider", v1.ActionInstall); err != nil {
				t.Fatal(err)
			}
			if err := syncProviderServiceAccount(ctx, client, "import-provider", v1.ActionUninstall); err != nil {
				t.Fatal(err)
			}
			got, err := client.CoreV1().ServiceAccounts("kube-system").Get(ctx, "kc-server", metav1.GetOptions{})
			if err != nil || got.UID != "source-sa" || got.Secrets[0].Name != "kc-server-secret" {
				t.Fatalf("borrowed SA changed: %v %v", got, err)
			}
			if _, err = client.RbacV1().ClusterRoleBindings().Get(ctx, "kc-server", metav1.GetOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err = client.CoreV1().Secrets("kube-system").Get(ctx, "kc-server-secret", metav1.GetOptions{}); err != nil {
				t.Fatal(err)
			}
			for _, a := range client.Actions() {
				if a.GetVerb() == "delete" || a.GetVerb() == "update" {
					t.Fatalf("mutated borrowed credentials: %v", a)
				}
			}
		})
	}
}

func TestProviderCreatesAndDeletesOnlyOwnedAccess(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	if err := syncProviderServiceAccount(ctx, client, "provider-uid", v1.ActionInstall); err != nil {
		t.Fatal(err)
	}
	sa, err := client.CoreV1().ServiceAccounts("kube-system").Get(ctx, "kc-server", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if sa.Annotations[providerOwnerAnnotation] != "provider-uid" || len(sa.Secrets) != 1 {
		t.Fatalf("missing owned token: %+v", sa)
	}
	secret, err := client.CoreV1().Secrets("kube-system").Get(ctx, sa.Secrets[0].Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if secret.Annotations[providerOwnerAnnotation] != "provider-uid" || len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].Name != sa.Name {
		t.Fatalf("missing secret ownership: %+v", secret)
	}
	if err = syncProviderServiceAccount(ctx, client, "provider-uid", v1.ActionUninstall); err != nil {
		t.Fatal(err)
	}
	if _, err = client.CoreV1().ServiceAccounts("kube-system").Get(ctx, "kc-server", metav1.GetOptions{}); err == nil {
		t.Fatal("owned SA remains")
	}
	if _, err = client.RbacV1().ClusterRoleBindings().Get(ctx, "kc-server", metav1.GetOptions{}); err == nil {
		t.Fatal("owned binding remains")
	}
}

func TestProviderTerminalPreservesBorrowedResources(t *testing.T) {
	ctx := context.Background()
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "kc-kubectl", Namespace: "kube-system", UID: "source-deployment"}}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "kc-kubectl", Namespace: "kube-system", UID: "source-terminal-sa"}}
	binding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "kc-kubectl-rolebind", UID: "source-terminal-binding"}}
	client := fake.NewSimpleClientset(deployment, sa, binding)
	manifest := `apiVersion: v1
kind: ServiceAccount
metadata:
  name: kc-kubectl
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kc-kubectl-rolebind
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kc-kubectl
`
	if err := createProviderTerminal(ctx, client, "provider-uid", manifest); err != nil {
		t.Fatal(err)
	}
	if err := deleteProviderTerminal(ctx, client, "provider-uid"); err != nil {
		t.Fatal(err)
	}
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" || a.GetVerb() == "update" {
			t.Fatalf("mutated borrowed terminal: %v", a)
		}
	}
	owned := fake.NewSimpleClientset()
	if err := createProviderTerminal(ctx, owned, "provider-uid", manifest); err != nil {
		t.Fatal(err)
	}
	dep, err := owned.AppsV1().Deployments("kube-system").Get(ctx, "kc-kubectl", metav1.GetOptions{})
	if err != nil || dep.Annotations[providerOwnerAnnotation] != "provider-uid" {
		t.Fatalf("missing terminal ownership: %v %v", dep, err)
	}
	if err = deleteProviderTerminal(ctx, owned, "provider-uid"); err != nil {
		t.Fatal(err)
	}
	if _, err = owned.AppsV1().Deployments("kube-system").Get(ctx, "kc-kubectl", metav1.GetOptions{}); err == nil {
		t.Fatal("owned terminal deployment remains")
	}
	if _, err = owned.CoreV1().ServiceAccounts("kube-system").Get(ctx, "kc-kubectl", metav1.GetOptions{}); err == nil {
		t.Fatal("owned terminal SA remains")
	}
	if _, err = owned.RbacV1().ClusterRoleBindings().Get(ctx, "kc-kubectl-rolebind", metav1.GetOptions{}); err == nil {
		t.Fatal("owned terminal binding remains")
	}
}
