/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package main

import (
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
)

func TestBuiltinAddons(t *testing.T) {
	if _, ok := component.Load("nfs-csi/v1"); !ok {
		t.Error("NFS CSI must remain available")
	}
	if _, ok := component.Load("metallb/v1"); ok {
		t.Error("MetalLB must not be registered")
	}
}
