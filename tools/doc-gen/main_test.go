/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-openapi/spec"
)

func TestGenerateSwaggerJSON(t *testing.T) {
	previousOutput := output
	output = filepath.Join(t.TempDir(), "swagger.json")
	t.Cleanup(func() { output = previousOutput })

	data, err := generateSwaggerJSON()
	if err != nil {
		t.Fatal(err)
	}
	var swagger spec.Swagger
	if err := json.Unmarshal(data, &swagger); err != nil {
		t.Fatal(err)
	}
	if swagger.Swagger != "2.0" || swagger.Paths == nil || len(swagger.Paths.Paths) == 0 {
		t.Fatal("generated Swagger document must include the registered API routes")
	}
	written, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != string(data) {
		t.Fatal("written Swagger document differs from the generated document")
	}
}

func TestValidateSpecRejectsInvalidDocument(t *testing.T) {
	data := []byte(`{"swagger":"2.0","info":{"title":"invalid","version":"1"},"paths":{"/missing":{"get":{"responses":{"200":{"description":"ok","schema":{"$ref":"#/definitions/Missing"}}}}}}}`)
	if err := validateSpec(data); err == nil {
		t.Fatal("Swagger validation accepted an undefined response model")
	}
}

func TestMainExitsOnGenerationError(t *testing.T) {
	if os.Getenv("KC_DOC_GEN_TEST_MAIN") == "1" {
		main()
		return
	}
	invalidOutput := filepath.Join(t.TempDir(), "missing", "swagger.json")
	command := exec.Command(os.Args[0], "-test.run=^TestMainExitsOnGenerationError$", "-output", invalidOutput)
	command.Env = append(os.Environ(), "KC_DOC_GEN_TEST_MAIN=1")
	data, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("doc-gen exited successfully after a generation error")
	}
	if !strings.Contains(string(data), "write Swagger document") {
		t.Fatalf("doc-gen failed before the expected output error: %s", data)
	}
}
