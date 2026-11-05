/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
	"fmt"

	"github.com/google/go-containerregistry/pkg/crane"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// artifactSource lets online and local content share selection and validation.
type artifactSource struct {
	v1.Descriptor
	image v1.Image
	index v1.ImageIndex
}

func remoteSource(ref string, options []crane.Option) (*artifactSource, error) {
	descriptor, err := crane.Get(ref, options...)
	if err != nil {
		return nil, err
	}
	source := &artifactSource{Descriptor: descriptor.Descriptor}
	if descriptor.MediaType.IsIndex() {
		source.index, err = descriptor.ImageIndex()
	} else {
		source.image, err = descriptor.Image()
	}
	return source, err
}

func (s *artifactSource) Image() (v1.Image, error) {
	if s.image == nil {
		return nil, fmt.Errorf("source %s is not an image", s.Digest)
	}
	return s.image, nil
}

func (s *artifactSource) ImageIndex() (v1.ImageIndex, error) {
	if s.index == nil {
		return nil, fmt.Errorf("source %s is not an image index", s.Digest)
	}
	return s.index, nil
}

func (s *artifactSource) content() any {
	if s.MediaType == types.OCIImageIndex || s.MediaType == types.DockerManifestList {
		return s.index
	}
	return s.image
}
