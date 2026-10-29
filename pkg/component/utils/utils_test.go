/*
 *
 *  * Copyright 2024 KubeClipper Authors.
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

package utils

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mockcluster "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestMissingClusterImageRegistryIsRejected(t *testing.T) {
	if _, err := ResolveClusterImageRegistry(context.Background(), &v1.Cluster{}, nil); err == nil {
		t.Fatal("expected missing registry error")
	}
	if _, err := GetClusterCRIRegistriesWithContext(context.Background(), &v1.Cluster{}, nil); err == nil {
		t.Fatal("expected missing registry error")
	}
}

func TestGetClusterCRIRegistries(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := mockcluster.NewMockOperator(ctrl)
	auth := &v1.RegistryAuth{Username: "user", Password: "secret"}
	op.EXPECT().GetRegistry(gomock.Any(), "business").Return(&v1.Registry{
		ObjectMeta:   metav1.ObjectMeta{Name: "business"},
		RegistrySpec: v1.RegistrySpec{Scheme: "https", Host: "registry.business.example.com", CA: "ca", RegistryAuth: auth},
	}, nil)
	op.EXPECT().GetRegistry(gomock.Any(), "image-auth").Return(&v1.Registry{
		ObjectMeta:   metav1.ObjectMeta{Name: "image-auth"},
		RegistrySpec: v1.RegistrySpec{Scheme: "https", Host: "harbor.example.com", SkipVerify: true, RegistryAuth: auth},
	}, nil)
	business := "business"
	cluster := &v1.Cluster{
		ImageRegistry: "image-auth",
		ContainerRuntime: v1.ContainerRuntime{Registries: []v1.CRIRegistry{
			{RegistryRef: &business},
		}},
	}
	got, err := GetClusterCRIRegistriesWithContext(context.Background(), cluster, op)
	if err != nil {
		t.Fatal(err)
	}
	want := []v1.RegistrySpec{
		{Scheme: "https", Host: "harbor.example.com", SkipVerify: true, RegistryAuth: auth},
		{Scheme: "https", Host: "registry.business.example.com", CA: "ca", RegistryAuth: auth},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("registries = %#v, want %#v", got, want)
	}
	if cluster.ResolvedImageRegistry != "harbor.example.com" {
		t.Fatalf("resolved image registry = %q", cluster.ResolvedImageRegistry)
	}
}

func TestGetClusterCRIRegistriesRejectsConflictingResources(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := mockcluster.NewMockOperator(ctrl)
	op.EXPECT().GetRegistry(gomock.Any(), "first").Return(&v1.Registry{RegistrySpec: v1.RegistrySpec{Scheme: "https", Host: "registry.example.com"}}, nil).Times(2)
	op.EXPECT().GetRegistry(gomock.Any(), "second").Return(&v1.Registry{RegistrySpec: v1.RegistrySpec{Scheme: "http", Host: "registry.example.com"}}, nil)
	first, second := "first", "second"
	cluster := &v1.Cluster{ImageRegistry: "first", ContainerRuntime: v1.ContainerRuntime{Registries: []v1.CRIRegistry{{RegistryRef: &first}, {RegistryRef: &second}}}}
	if _, err := GetClusterCRIRegistriesWithContext(context.Background(), cluster, op); err == nil {
		t.Fatal("expected conflicting registry configuration error")
	}
}

func TestRegistryRepositoryPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := mockcluster.NewMockOperator(ctrl)
	var registry v1.Registry
	if err := json.Unmarshal([]byte(`{"host":"registry.example.com:5251","repositoryPrefix":"qualified/team","scheme":"https","auth":{"username":"u","password":"p"}}`), &registry); err != nil {
		t.Fatal(err)
	}
	op.EXPECT().GetRegistry(gomock.Any(), "default").Return(&registry, nil)
	c := &v1.Cluster{ImageRegistry: "default"}
	regs, err := GetClusterCRIRegistriesWithContext(context.Background(), c, op)
	if err != nil {
		t.Fatal(err)
	}
	if c.ResolvedImageRegistry != "registry.example.com:5251/qualified/team" {
		t.Fatalf("image repository = %q, prefix lost", c.ResolvedImageRegistry)
	}
	if len(regs) != 1 || regs[0].Host != "registry.example.com:5251" {
		t.Fatalf("CRI host must not contain prefix: %#v", regs)
	}
}

func TestCRIRegistriesIgnoreImagePrefixes(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := mockcluster.NewMockOperator(ctrl)
	for _, entry := range []struct{ name, prefix string }{{"one", "team"}, {"two", "other"}, {"image", "images"}} {
		op.EXPECT().GetRegistry(gomock.Any(), entry.name).Return(&v1.Registry{RegistrySpec: v1.RegistrySpec{Host: "registry.example.com", RepositoryPrefix: entry.prefix, Scheme: "https"}}, nil)
	}
	one, two := "one", "two"
	c := &v1.Cluster{ImageRegistry: "image", ContainerRuntime: v1.ContainerRuntime{Registries: []v1.CRIRegistry{{RegistryRef: &one}, {RegistryRef: &two}}}}
	regs, err := GetClusterCRIRegistriesWithContext(context.Background(), c, op)
	if err != nil {
		t.Fatal(err)
	}
	if len(regs) != 1 || regs[0].RepositoryPrefix != "" || c.ResolvedImageRegistry != "registry.example.com/images" {
		t.Fatalf("unexpected registry separation: %#v %s", regs, c.ResolvedImageRegistry)
	}
}
