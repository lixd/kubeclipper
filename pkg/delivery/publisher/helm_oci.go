/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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

package publisher

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"sigs.k8s.io/yaml"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

type HelmChartPublishRequest struct {
	ChartPath        string
	Registry         string
	RepositoryPrefix string
	Name             string
}

type HelmChartPublishResult struct {
	Name      string
	Version   string
	Ref       string
	Digest    string
	ChartHash string
}

func PublishHelmChart(req HelmChartPublishRequest) (*HelmChartPublishResult, error) {
	if req.ChartPath == "" {
		return nil, fmt.Errorf("chart path is required")
	}
	if req.Registry == "" {
		return nil, fmt.Errorf("registry is required")
	}
	chartData, err := os.ReadFile(req.ChartPath)
	if err != nil {
		return nil, err
	}
	configData, metadata, err := helmChartConfig(chartData)
	if err != nil {
		return nil, err
	}
	if req.Name != "" && req.Name != metadata.Name {
		return nil, fmt.Errorf("chart name %q does not match requested name %q", metadata.Name, req.Name)
	}
	prefix := strings.Trim(req.RepositoryPrefix, "/")
	if prefix == "" {
		return nil, fmt.Errorf("repository prefix is required")
	}
	ref := fmt.Sprintf("%s/%s/%s:%s", strings.TrimRight(req.Registry, "/"), prefix, metadata.Name, metadata.Version)
	img := newHelmChartImage(configData, chartData)
	if err = crane.Push(img, ref, crane.Insecure); err != nil {
		return nil, err
	}
	digest, err := img.Digest()
	if err != nil {
		return nil, err
	}
	chartHash, _, err := v1.SHA256(bytes.NewReader(chartData))
	if err != nil {
		return nil, err
	}
	return &HelmChartPublishResult{
		Name:      metadata.Name,
		Version:   metadata.Version,
		Ref:       strings.TrimSuffix(ref, ":"+metadata.Version),
		Digest:    digest.String(),
		ChartHash: chartHash.String(),
	}, nil
}

type helmChartMetadata struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func helmChartConfig(chartData []byte) ([]byte, helmChartMetadata, error) {
	chartYAML, err := readChartYAML(chartData)
	if err != nil {
		return nil, helmChartMetadata{}, err
	}
	configData, err := yaml.YAMLToJSON(chartYAML)
	if err != nil {
		return nil, helmChartMetadata{}, err
	}
	var metadata helmChartMetadata
	if err = json.Unmarshal(configData, &metadata); err != nil {
		return nil, helmChartMetadata{}, err
	}
	if metadata.Name == "" || metadata.Version == "" {
		return nil, helmChartMetadata{}, fmt.Errorf("Chart.yaml must contain name and version")
	}
	return configData, metadata, nil
}

func readChartYAML(chartData []byte) ([]byte, error) {
	gzr, err := gzip.NewReader(bytes.NewReader(chartData))
	if err != nil {
		return nil, fmt.Errorf("read chart archive: %w", err)
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("Chart.yaml not found in chart archive")
		}
		if err != nil {
			return nil, err
		}
		if header.Typeflag != tar.TypeReg || filepath.Base(header.Name) != "Chart.yaml" {
			continue
		}
		return io.ReadAll(tr)
	}
}

type helmChartImage struct {
	config []byte
	layer  v1.Layer
}

func newHelmChartImage(configData, chartData []byte) v1.Image {
	return &helmChartImage{
		config: configData,
		layer:  static.NewLayer(chartData, types.MediaType(deliveryapis.MediaTypeHelmChartLayer)),
	}
}

func (i *helmChartImage) Layers() ([]v1.Layer, error) {
	return []v1.Layer{i.layer}, nil
}

func (i *helmChartImage) MediaType() (types.MediaType, error) {
	return types.OCIManifestSchema1, nil
}

func (i *helmChartImage) Size() (int64, error) {
	return partial.Size(i)
}

func (i *helmChartImage) ConfigName() (v1.Hash, error) {
	return partial.ConfigName(i)
}

func (i *helmChartImage) ConfigFile() (*v1.ConfigFile, error) {
	return &v1.ConfigFile{}, nil
}

func (i *helmChartImage) RawConfigFile() ([]byte, error) {
	return i.config, nil
}

func (i *helmChartImage) ConfigLayer() (v1.Layer, error) {
	return static.NewLayer(i.config, types.MediaType(deliveryapis.MediaTypeHelmConfig)), nil
}

func (i *helmChartImage) Digest() (v1.Hash, error) {
	return partial.Digest(i)
}

func (i *helmChartImage) Manifest() (*v1.Manifest, error) {
	configName, err := i.ConfigName()
	if err != nil {
		return nil, err
	}
	layerDesc, err := partial.Descriptor(i.layer)
	if err != nil {
		return nil, err
	}
	return &v1.Manifest{
		SchemaVersion: 2,
		MediaType:     types.OCIManifestSchema1,
		Config: v1.Descriptor{
			MediaType: types.MediaType(deliveryapis.MediaTypeHelmConfig),
			Size:      int64(len(i.config)),
			Digest:    configName,
		},
		Layers: []v1.Descriptor{*layerDesc},
	}, nil
}

func (i *helmChartImage) RawManifest() ([]byte, error) {
	return partial.RawManifest(i)
}

func (i *helmChartImage) LayerByDigest(hash v1.Hash) (v1.Layer, error) {
	digest, err := i.layer.Digest()
	if err != nil {
		return nil, err
	}
	if digest == hash {
		return i.layer, nil
	}
	return nil, fmt.Errorf("layer %s not found", hash)
}

func (i *helmChartImage) LayerByDiffID(hash v1.Hash) (v1.Layer, error) {
	diffID, err := i.layer.DiffID()
	if err != nil {
		return nil, err
	}
	if diffID == hash {
		return i.layer, nil
	}
	return nil, fmt.Errorf("layer %s not found", hash)
}
