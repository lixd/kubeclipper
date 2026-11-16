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

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	auditingv1 "github.com/kubeclipper/kubeclipper/pkg/apis/auditing/v1"
	"github.com/kubeclipper/kubeclipper/pkg/apis/proxy"

	"github.com/kubeclipper/kubeclipper/pkg/apis/oauth"

	iamv1 "github.com/kubeclipper/kubeclipper/pkg/apis/iam/v1"

	corev1 "github.com/kubeclipper/kubeclipper/pkg/apis/core/v1"
	operationsv1alpha1 "github.com/kubeclipper/kubeclipper/pkg/apis/operations/v1alpha1"

	"github.com/emicklei/go-restful"
	restfulspec "github.com/emicklei/go-restful-openapi"
	"github.com/go-openapi/loads"
	"github.com/go-openapi/spec"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/validate"
	urlruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/version"

	configv1 "github.com/kubeclipper/kubeclipper/pkg/apis/config/v1"
)

var output string

func init() {
	flag.StringVar(&output, "output", "./api/openapi-spec/swagger.json", "--output=./api.json")
}

func main() {
	flag.Parse()
	if _, err := generateSwaggerJSON(); err != nil {
		log.Fatal(err)
	}
}

func validateSpec(apiSpec []byte) error {
	// Recursive response schemas need a real base document for reference expansion.
	directory, err := os.MkdirTemp("", "kc-swagger-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	specPath := filepath.Join(directory, "swagger.json")
	if err := os.WriteFile(specPath, apiSpec, 0644); err != nil {
		return err
	}
	swaggerDoc, err := loads.Spec(specPath)
	if err != nil {
		return err
	}

	// Attempts to report about all errors
	validate.SetContinueOnErrors(true)

	v := validate.NewSpecValidator(swaggerDoc.Schema(), strfmt.Default)
	result, _ := v.Validate(swaggerDoc)

	if result.HasWarnings() {
		log.Printf("See warnings below:\n")
		for _, desc := range result.Warnings {
			log.Printf("- WARNING: %s\n", desc.Error())
		}

	}
	if result.HasErrors() {
		str := fmt.Sprintf("The swagger spec is invalid against swagger specification %s.\nSee errors below:\n", swaggerDoc.Version())
		for _, desc := range result.Errors {
			str += fmt.Sprintf("- %s\n", desc.Error())
		}
		log.Println(str)
		return errors.New(str)
	}

	return nil
}

func generateSwaggerJSON() ([]byte, error) {

	container := restful.NewContainer()
	urlruntime.Must(corev1.AddToContainer(container, nil, nil, nil, nil, nil, nil, nil, nil))
	container.Add(operationsv1alpha1.SetupWebService(nil))
	urlruntime.Must(iamv1.AddToContainer(container, nil, nil, nil))
	urlruntime.Must(configv1.AddToContainer(container, nil, nil, nil, nil))
	urlruntime.Must(oauth.AddToContainer(container, nil, nil, nil, nil, nil, nil, nil))
	urlruntime.Must(proxy.AddToContainer(container, nil))
	urlruntime.Must(auditingv1.AddToContainer(container, nil))

	config := restfulspec.Config{
		WebServices:                   container.RegisteredWebServices(),
		PostBuildSwaggerObjectHandler: enrichSwaggerObject}

	swagger := restfulspec.BuildSwagger(config)
	swagger.Info.Extensions = make(spec.Extensions)
	swagger.Info.Extensions.Add("x-tagGroups", []struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}{
		// {
		//	Name: "Cluster",
		//	Tags: []string{
		//		constants.CoreClusterTag,
		//	},
		// },
	})

	data, err := json.MarshalIndent(swagger, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode Swagger document: %w", err)
	}
	if err := validateSpec(data); err != nil {
		return nil, err
	}
	if err := os.WriteFile(output, data, 0644); err != nil {
		return nil, fmt.Errorf("write Swagger document: %w", err)
	}
	log.Printf("successfully written to %s", output)

	return data, nil
}

func enrichSwaggerObject(swo *spec.Swagger) {
	swo.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:       "KubeClipper",
			Description: "KubeClipper OpenAPI",
			Version:     version.Get().GitVersion,
			Contact: &spec.ContactInfo{
				// TODO: add url and email
				ContactInfoProps: spec.ContactInfoProps{
					Name:  "KubeClipper",
					URL:   "https://github.com/kubeclipper-labs/kubeclipper",
					Email: "",
				},
			},
			License: &spec.License{
				LicenseProps: spec.LicenseProps{
					Name: "Apache 2.0",
					URL:  "https://www.apache.org/licenses/LICENSE-2.0.html",
				},
			},
		},
	}

	// setup security definitions
	swo.SecurityDefinitions = map[string]*spec.SecurityScheme{
		"jwt": spec.APIKeyAuth("Authorization", "header"),
	}
	swo.Security = []map[string][]string{{"jwt": []string{}}}
}

// func apiTree(container *restful.Container) {
//	buf := bytes.NewBufferString("\n")
//	for _, ws := range container.RegisteredWebServices() {
//		for _, route := range ws.Routes() {
//			buf.WriteString(fmt.Sprintf("%s %s\n", route.Method, route.Path))
//		}
//	}
//	log.Println(buf.String())
// }
