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

package fetcher

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	apis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

// Limits bound individual allocations and archive expansion, not supported versions.
const (
	maxPackageFile    int64 = 8 << 30
	maxPayload        int64 = 16 << 30
	maxRootFS         int64 = 32 << 30
	extractionVersion       = 1
)

func snapshotResult(osName, arch string, c apis.ResolvedComponent) (ComponentFetchResult, []apis.ArtifactContent, error) {
	if _, err := ociReference(c.Transport); err != nil {
		return ComponentFetchResult{}, nil, err
	}
	d := digest.Digest(c.Transport.Digest)
	if err := d.Validate(); err != nil {
		return ComponentFetchResult{}, nil, err
	}
	contents := append([]apis.ArtifactContent(nil), packageLayerContents(c.Contents)...)
	if len(contents) == 0 {
		return ComponentFetchResult{}, nil, fmt.Errorf("package has no layer contents")
	}
	for i := range contents {
		contents[i].File = contentFile(contents[i])
		contents[i].Transport = apis.TransportRef{}
	}
	sort.Slice(contents, func(i, j int) bool { return contents[i].File < contents[j].File })
	seen := map[string]bool{}
	for _, x := range contents {
		if seen[contentFile(x)] {
			return ComponentFetchResult{}, nil, fmt.Errorf("duplicate extraction target")
		}
		seen[contentFile(x)] = true
	}
	b, err := json.Marshal(struct {
		Version  int
		Contents []apis.ArtifactContent
	}{extractionVersion, contents})
	if err != nil {
		return ComponentFetchResult{}, nil, err
	}
	sum := sha256.Sum256(b)
	base := filepath.Join(downloader.PackageDir(c.Kind, c.Name, c.Version, platformDir(osName, arch)), d.Encoded(), hex.EncodeToString(sum[:]))
	r := ComponentFetchResult{Slot: c.Slot, Kind: c.Kind, Name: c.Name, Version: c.Version, OS: osName, Arch: arch, BaseDir: base, ManifestPath: filepath.Join(base, "receipt.json"), Transport: c.Transport, Files: map[string]string{}}
	for _, x := range contents {
		r.Files[x.Name] = filepath.Join(base, "contents", contentFile(x))
	}
	return r, contents, nil
}

func (f *OCIArtifactFetcher) fetchComponent(ctx context.Context, osName, arch string, c apis.ResolvedComponent) (ComponentFetchResult, error) {
	r, contents, err := snapshotResult(osName, arch, c)
	if err != nil {
		return r, err
	}
	if f.DryRun {
		return r, nil
	}
	lock, err := downloader.LockCache(ctx, "snapshot:"+r.BaseDir, true, false)
	if err != nil {
		return r, err
	}
	defer lock.Release()
	if _, err = os.Stat(r.ManifestPath); err == nil {
		if err = validateSnapshot(ctx, r, contents); err != nil {
			if ctx.Err() != nil {
				return r, ctx.Err()
			}
			return r, &corruptCache{r.BaseDir}
		}
		return r, nil
	} else if !os.IsNotExist(err) {
		return r, err
	}
	if _, e := os.Stat(r.BaseDir); e == nil {
		return r, &corruptCache{r.BaseDir}
	}
	store, err := openContent()
	if err != nil {
		return r, err
	}
	ref, err := ociReference(c.Transport)
	if err != nil {
		return r, err
	}
	var manifest v1.Manifest
	manifestDigest := digest.Digest(c.Transport.Digest)
	manifestPath := store.path(manifestDigest)
	var image v1.Image
	info, statErr := os.Stat(manifestPath)
	if statErr == nil {
		if info.Size() > maxManifestSize {
			return r, &corruptCache{manifestPath}
		}
		if _, err = store.verified(ctx, ocispec.Descriptor{Digest: manifestDigest, Size: info.Size()}); err != nil {
			return r, err
		}
		data, e := os.ReadFile(manifestPath)
		if e != nil {
			return r, e
		}
		if e = json.Unmarshal(data, &manifest); e != nil {
			return r, &corruptCache{manifestPath}
		}
	} else if !os.IsNotExist(statErr) {
		return r, statErr
	} else {
		image, err = f.pullImage(ctx, ref)
		if err != nil {
			return r, safeNetworkError(err)
		}
		data, e := image.RawManifest()
		if e != nil {
			return r, safeNetworkError(e)
		}
		if int64(len(data)) > maxManifestSize || digest.FromBytes(data) != manifestDigest {
			return r, fmt.Errorf("platform manifest size/digest mismatch")
		}
		if e = json.Unmarshal(data, &manifest); e != nil {
			return r, e
		}
		if e = store.put(ctx, ocispec.Descriptor{Digest: manifestDigest, Size: int64(len(data))}, data); e != nil {
			return r, e
		}
	}
	if len(manifest.Layers) != 1 {
		return r, fmt.Errorf("unsupported package layer layout: want one rootfs layer")
	}
	layer := manifest.Layers[0]
	descriptor := ocispec.Descriptor{Digest: digest.Digest(layer.Digest.String()), Size: layer.Size, MediaType: string(layer.MediaType)}
	if descriptor.Size > maxRootFS {
		return r, fmt.Errorf("compressed package layer exceeds limit")
	}
	var client *http.Client
	var endpoint string
	open := func(offset int64) (*http.Response, error) {
		if f.PullImage != nil {
			if image == nil {
				image, err = f.pullImage(ctx, ref)
				if err != nil {
					return nil, err
				}
			}
			layers, e := image.Layers()
			if e != nil {
				return nil, e
			}
			body, e := layers[0].Compressed()
			if e != nil {
				return nil, e
			}
			if offset > 0 {
				if _, e = io.CopyN(io.Discard, body, offset); e != nil {
					body.Close()
					return nil, e
				}
			}
			h := http.Header{}
			code := 200
			if offset > 0 {
				code = 206
				h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, descriptor.Size-1, descriptor.Size))
			}
			return &http.Response{StatusCode: code, Body: body, ContentLength: descriptor.Size - offset, Header: h}, nil
		}
		if client == nil {
			client, endpoint, err = f.blobClient(ctx, ref)
			if err != nil {
				return nil, err
			}
		}
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+descriptor.Digest.String(), nil)
		if e != nil {
			return nil, e
		}
		req.Header.Set("Accept-Encoding", "identity")
		if offset > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
		return client.Do(req)
	}
	source, err := name.ParseReference(ref)
	if err != nil {
		return r, err
	}
	if err = store.fetch(ctx, descriptor, source.Context().Name(), open); err != nil {
		return r, err
	}
	if err = downloader.CheckCacheSpace(0); err != nil {
		return r, err
	}
	if err = os.MkdirAll(filepath.Dir(r.BaseDir), 0700); err != nil {
		return r, err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(r.BaseDir), ".extract-")
	if err != nil {
		return r, err
	}
	defer os.RemoveAll(tmp)
	if err = os.MkdirAll(filepath.Join(tmp, "contents"), 0700); err != nil {
		return r, err
	}
	pkg, err := extractSnapshot(ctx, store.path(descriptor.Digest), tmp, c, osName, arch, contents)
	if err != nil {
		return r, err
	}
	tempResult := r
	tempResult.ManifestPath = filepath.Join(tmp, "receipt.json")
	if err = writeFetchedManifest(tempResult, contents, pkg, false); err != nil {
		return r, err
	}
	if err = downloader.SyncDirectory(filepath.Join(tmp, "contents")); err != nil {
		return r, err
	}
	if err = downloader.SyncDirectory(tmp); err != nil {
		return r, err
	}
	if err = os.Rename(tmp, r.BaseDir); err != nil {
		return r, err
	}
	if err = downloader.SyncDirectory(filepath.Dir(r.BaseDir)); err != nil {
		return r, err
	}
	return r, nil
}

func extractSnapshot(ctx context.Context, blob, tmp string, c apis.ResolvedComponent, osName, arch string, contents []apis.ArtifactContent) (apis.PackageManifest, error) {
	var pkg apis.PackageManifest
	layer, err := tarball.LayerFromFile(blob)
	if err != nil {
		return pkg, err
	}
	reader, err := layer.Uncompressed()
	if err != nil {
		return pkg, err
	}
	defer reader.Close()
	limited := &io.LimitedReader{R: contextReader{ctx, reader}, N: maxRootFS + 1}
	tr := tar.NewReader(limited)
	selected := map[string]apis.ArtifactContent{}
	for _, x := range contents {
		selected[path.Join("opt/kubeclipper/resource", contentFile(x))] = x
	}
	seen := map[string]bool{}
	foundManifest := false
	for {
		if err = ctx.Err(); err != nil {
			return pkg, err
		}
		h, e := tr.Next()
		if limited.N <= 0 {
			return pkg, fmt.Errorf("rootfs exceeds expansion limit")
		}
		if e == io.EOF {
			if _, err = copyContext(ctx, io.Discard, limited, limited.N-1); err != nil {
				return pkg, err
			}
			break
		}

		if e != nil {
			return pkg, e
		}
		if limited.N <= 0 {
			return pkg, fmt.Errorf("rootfs exceeds expansion limit")
		}
		if path.IsAbs(h.Name) || strings.Contains(h.Name, "\\") {
			return pkg, fmt.Errorf("unsafe rootfs path")
		}
		for _, part := range strings.Split(h.Name, "/") {
			if part == ".." {
				return pkg, fmt.Errorf("unsafe rootfs path")
			}
		}
		name := path.Clean(h.Name)
		if h.Typeflag == tar.TypeSymlink || h.Typeflag == tar.TypeLink {
			return pkg, fmt.Errorf("package rootfs contains link")
		}
		if name == packageManifestPath() {
			if foundManifest || h.Typeflag != tar.TypeReg || h.Size > maxManifestSize {
				return pkg, fmt.Errorf("invalid or duplicate package manifest")
			}
			foundManifest = true
			data, e := io.ReadAll(io.LimitReader(tr, maxManifestSize+1))
			if e != nil {
				return pkg, e
			}
			if e = json.Unmarshal(data, &pkg); e != nil {
				return pkg, e
			}
			continue
		}
		x, ok := selected[name]
		if !ok {
			continue
		}
		if seen[name] || h.Typeflag != tar.TypeReg || h.Size < 0 || h.Size > maxPackageFile {
			return pkg, fmt.Errorf("invalid or duplicate package target")
		}
		seen[name] = true
		target := filepath.Join(tmp, "contents", contentFile(x))
		if err = downloader.CheckCacheSpace(h.Size); err != nil {
			return pkg, err
		}
		err = downloader.AtomicWrite(target, 0600, func(w io.Writer) error {
			n, e := copyContext(ctx, w, tr, h.Size)
			if e == nil && n != h.Size {
				return io.ErrUnexpectedEOF
			}
			return e
		})
		if err != nil {
			return pkg, err
		}
	}
	if !foundManifest {
		return pkg, fmt.Errorf("package manifest missing")
	}
	layerComponent := c
	layerComponent.Contents = contents
	if err = validateFetchedManifest(pkg, layerComponent, osName, arch); err != nil {
		return pkg, err
	}
	for _, x := range contents {
		p := filepath.Join(tmp, "contents", contentFile(x))
		if err = verifyPayload(ctx, p, x.Name, manifestFileDigest(pkg, x.Name)); err != nil {
			return pkg, err
		}
	}
	return pkg, nil
}

func manifestFileDigest(pkg apis.PackageManifest, name string) string {
	for _, x := range pkg.Contents {
		if x.Name == name {
			return x.Digest
		}
	}
	return ""
}
func verifyPayload(ctx context.Context, p, name, expected string) error {
	d := digest.Digest(expected)
	if err := d.Validate(); err != nil {
		return fmt.Errorf("invalid %s payload digest", name)
	}
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > maxPackageFile {
		return fmt.Errorf("invalid package file")
	}
	var r io.Reader = f
	if !isPlainFileContent(name) {
		gz, e := gzip.NewReader(f)
		if e != nil {
			return e
		}
		defer gz.Close()
		r = gz
	}
	h := d.Algorithm().Hash()
	if _, err = copyContext(ctx, h, r, maxPayload); err != nil {
		return err
	}
	if digest.NewDigest(d.Algorithm(), h) != d {
		return fmt.Errorf("%s payload digest mismatch", name)
	}
	return nil
}
func validateSnapshot(ctx context.Context, r ComponentFetchResult, contents []apis.ArtifactContent) error {
	f, err := os.Open(r.ManifestPath)
	if err != nil {
		return err
	}
	defer f.Close()
	var receipt fetchedComponentManifest
	if err = json.NewDecoder(io.LimitReader(f, maxManifestSize)).Decode(&receipt); err != nil {
		return err
	}
	if receipt.Kind != r.Kind || receipt.Name != r.Name || receipt.Version != r.Version || receipt.OS != r.OS || receipt.Arch != r.Arch || receipt.Transport.Digest != r.Transport.Digest || !reflect.DeepEqual(receipt.Contents, contents) {
		return fmt.Errorf("snapshot identity mismatch")
	}
	c := apis.ResolvedComponent{Kind: r.Kind, Name: r.Name, Version: r.Version, Contents: contents}
	if err = validateFetchedManifest(receipt.Package, c, r.OS, r.Arch); err != nil {
		return err
	}
	for _, x := range contents {
		if receipt.Files[x.Name] != r.Files[x.Name] {
			return fmt.Errorf("snapshot path mismatch")
		}
		if err = verifyPayload(ctx, r.Files[x.Name], x.Name, manifestFileDigest(receipt.Package, x.Name)); err != nil {
			return err
		}
	}
	return nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
