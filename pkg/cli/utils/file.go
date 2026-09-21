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

func FileExist(path string) bool {
	_, err := os.Lstat(path)
	return !os.IsNotExist(err)
}

// WriteToFile atomically writes data to path as a private (0600) file.
// The only current caller persists deploy-config.yaml, which embeds
// registry credentials, so pre-existing wider-permission files are
// tightened as well.
//
// The write goes through a 0600 temporary file in the target directory
// that replaces the target only after a successful write, sync and
// close; a failure anywhere keeps the original file intact and removes
// the temporary file. A symbolic link at path is refused instead of
// being followed, so a misplaced link cannot redirect the write onto
// another file. The mode is set explicitly on both files, so umask
// cannot widen it.
func WriteToFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to write config through symbolic link %s", path)
	}
	tmp, err := os.CreateTemp(dir, ".kc-write-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmpPath, path)
}
