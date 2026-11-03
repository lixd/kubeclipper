package server

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"

	"github.com/kubeclipper/kubeclipper/pkg/authorization/authorizer"
	"github.com/kubeclipper/kubeclipper/pkg/authorization/rbac"
	mock "github.com/kubeclipper/kubeclipper/pkg/models/iam/mock"
	iamv1 "github.com/kubeclipper/kubeclipper/pkg/scheme/iam/v1"
)

func TestBuiltinViewerCannotReadDeploymentCredentials(t *testing.T) {
	op := mock.NewMockOperator(gomock.NewController(t))
	roles := map[string]*iamv1.GlobalRole{}
	for i := range Roles {
		roles[Roles[i].Name] = Roles[i].DeepCopy()
	}
	op.EXPECT().GetRoleEx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, name, _ string) (*iamv1.GlobalRole, error) { return roles[name].DeepCopy(), nil }).AnyTimes()
	bindings := &iamv1.GlobalRoleBindingList{}
	for _, name := range []string{"platform-view", "platform-admin"} {
		bindings.Items = append(bindings.Items, iamv1.GlobalRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, Subjects: []rbacv1.Subject{{Kind: "User", Name: name}}, RoleRef: rbacv1.RoleRef{Kind: "GlobalRole", Name: name}})
	}
	op.EXPECT().ListRoleBindings(gomock.Any(), gomock.Any()).Return(bindings, nil).AnyTimes()
	auth := rbac.NewAuthorizer(op, nil)
	for _, test := range []struct {
		role, verb, resource, name string
		allowed                    bool
	}{
		{"platform-view", "get", "configmaps", "deploy-config", false},
		{"platform-view", "list", "configmaps", "", false},
		{"platform-view", "watch", "configmaps", "", false},
		{"platform-view", "list", "clusters", "", true},
		{"platform-view", "list", "registries", "", true},
		{"platform-admin", "get", "configmaps", "deploy-config", true},
		{"platform-admin", "update", "configmaps", "deploy-config", true},
	} {
		decision, _, err := auth.Authorize(context.Background(), &authorizer.AttributesRecord{User: &user.DefaultInfo{Name: test.role}, Verb: test.verb, APIGroup: "core.kubeclipper.io", Resource: test.resource, Name: test.name, ResourceRequest: true})
		if err != nil || (decision == authorizer.DecisionAllow) != test.allowed {
			t.Errorf("%s %s %s/%s: decision=%v err=%v", test.role, test.verb, test.resource, test.name, decision, err)
		}
	}
}

func TestMigrateExistingViewerRemovesLegacyWildcardOnce(t *testing.T) {
	op := mock.NewMockOperator(gomock.NewController(t))
	existing := &iamv1.GlobalRoleList{}
	for i := range Roles {
		existing.Items = append(existing.Items, *Roles[i].DeepCopy())
	}
	legacy := rbacv1.PolicyRule{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"watch", "get", "list"}}
	custom := rbacv1.PolicyRule{APIGroups: []string{"example.invalid"}, Resources: []string{"reports"}, Verbs: []string{"get"}}
	for i := range existing.Items {
		if existing.Items[i].Name == "platform-view" {
			existing.Items[i].Rules = []rbacv1.PolicyRule{legacy, custom}
		}
	}
	op.EXPECT().ListRoles(gomock.Any(), gomock.Any()).Return(existing, nil).Times(2)
	op.EXPECT().UpdateRole(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, role *iamv1.GlobalRole) (*iamv1.GlobalRole, error) {
		if role.Name != "platform-view" || len(role.Rules) != 1 || role.Rules[0].Resources[0] != "reports" {
			t.Fatalf("migration must remove only the legacy wildcard: %s", role.Name)
		}
		return role, nil
	}).Times(1)
	server := &APIServer{}
	for i := 0; i < 2; i++ {
		if err := server.migrateRole(op); err != nil {
			t.Fatal(err)
		}
	}
}
