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

package utils

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	PrivateFileMode os.FileMode = 0600
	PrivateDirMode  os.FileMode = 0700
	defaultDirMode  os.FileMode = 0755
)

type sudoModeCopier interface {
	CopySudoMode(host, localFilePath, remoteFilePath string, mode os.FileMode) error
}

// CopyFileToHostsWithMode atomically installs one local file on every host.
func CopyFileToHostsWithMode(copier sudoModeCopier, localPath string, hosts []string, remoteDir string, mode os.FileMode) error {
	remotePath := filepath.Join(remoteDir, filepath.Base(localPath))
	for _, host := range hosts {
		if err := copier.CopySudoMode(host, localPath, remotePath, mode); err != nil {
			return fmt.Errorf("copy %s to %s: %w", filepath.Base(localPath), host, err)
		}
	}
	return nil
}

func FileExist(path string) bool {
	_, err := os.Lstat(path)
	return !os.IsNotExist(err)
}

func WriteToFile(path string, data []byte) error {
	err := os.MkdirAll(filepath.Dir(path), 0777)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

// WritePrivateFile atomically replaces a file that contains credentials.
func WritePrivateFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, defaultDirMode); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".kc-private-*")
	if err != nil {
		return err
	}
	tmpPath := f.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if err = f.Chmod(PrivateFileMode); err != nil {
		_ = f.Close()
		return err
	}
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if closeErr := f.Close(); closeErr != nil {
		return closeErr
	}
	return os.Rename(tmpPath, path)
}
