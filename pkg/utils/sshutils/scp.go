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

package sshutils

import (
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"github.com/vbauerster/mpb/v8"

	"github.com/kubeclipper/kubeclipper/pkg/cli/logger"
)

const KB = 1024
const MB = 1024 * 1024
const defaultCopyTempDir = "/tmp"

// CopyForMD5V2 copy and check md5
func (ss *SSH) CopyForMD5V2(host, localFilePath, remoteFilePath, localMD5 string) (bool, error) {
	return ss.CopyForMD5V2WithTempDir(host, localFilePath, remoteFilePath, localMD5, defaultCopyTempDir)
}

// CopyForMD5V2WithTempDir copies a file through tempDir when sudo is required.
func (ss *SSH) CopyForMD5V2WithTempDir(host, localFilePath, remoteFilePath, localMD5, tempDir string) (bool, error) {
	var err error
	if localMD5 == "" {
		localMD5, err = MD5FromLocal(localFilePath)
		if err != nil {
			return false, err
		}
	}
	err = ss.CopySudoWithTempDir(host, localFilePath, remoteFilePath, tempDir)
	if err != nil {
		return false, err
	}
	remoteMD5, err := ss.MD5FromRemote(host, remoteFilePath)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(localMD5) == strings.TrimSpace(remoteMD5) {
		return true, nil
	}
	return false, nil
}

func (ss *SSH) CopySudo(host, localFilePath, remoteFilePath string) error {
	return ss.CopySudoWithTempDir(host, localFilePath, remoteFilePath, defaultCopyTempDir)
}

// CopySudoWithTempDir copies a file through tempDir when sudo is required.
func (ss *SSH) CopySudoWithTempDir(host, localFilePath, remoteFilePath, tempDir string) error {
	if ss.User == "root" { // root user,need not transit
		return ss.Copy(host, localFilePath, remoteFilePath)
	}
	// if not root, first scp to a flat path under the temp directory, then
	// sudo mv to target; see CopySudoWithBarWithTempDir for why the middle
	// path must stay flat under the temp dir.
	middle := filepath.Join(tempDir, middleFileName(remoteFilePath))
	err := ss.Copy(host, localFilePath, middle)
	if err != nil {
		return errors.Wrap(err, "copy")
	}
	// TODO maybe need chown
	ret, err := SSHCmdWithSudo(ss, host, fmt.Sprintf("mkdir -pv %s && mv -f %s %s", filepath.Dir(remoteFilePath), middle, remoteFilePath))
	if err != nil {
		return errors.Wrap(err, "mv")
	}
	return errors.Wrap(ret.Error(), "mv")
}

// Copy is
func (ss *SSH) Copy(host, localFilePath, remoteFilePath string) error {
	// do mkdir to ensure remote dir always exists
	ret, err := SSHCmd(ss, host, fmt.Sprintf("mkdir -pv %s", filepath.Dir(remoteFilePath)))
	if err != nil {
		return err
	}
	if err = ret.Error(); err != nil {
		return err
	}
	// if need run as exec,change to use scp cmd.
	if SSHToCmd(ss, host) {
		ret, err = CmdToString("scp", localFilePath, remoteFilePath)
		if err != nil {
			return err
		}
		return ret.Error()
	}

	sftpClient, err := ss.sftpConnect(host)
	if err != nil {
		return err
	}
	defer sftpClient.Close()
	srcFile, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := sftpClient.Create(remoteFilePath)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	buf := make([]byte, 100*MB) // 100mb
	total := 0
	unit := ""
	for {
		n, _ := srcFile.Read(buf)
		if n == 0 {
			break
		}
		length, _ := dstFile.Write(buf[0:n])
		isKb := length/MB < 1
		speed := 0
		if isKb {
			total += length
			unit = "KB"
			speed = length / KB
		} else {
			total += length
			unit = "MB"
			speed = length / MB
		}
		totalLength, totalUnit := toSizeFromInt(total)
		logger.Infof("[%s]transfer total size is: %.2f%s ;speed is %d%s", host, totalLength, totalUnit, speed, unit)
	}
	return nil
}

func (ss *SSH) DownloadSudo(host, localFilePath, remoteFilePath string) error {
	if ss.User == "root" { // root user,need not transit
		return ss.download(host, localFilePath, remoteFilePath)
	}
	// if not root, first scp to /tmp, then sudo mv to target
	middle := filepath.Join("/tmp", localFilePath)
	err := ss.download(host, middle, remoteFilePath)
	if err != nil {
		return errors.Wrap(err, "download")
	}

	ret, err := SSHCmdWithSudo(ss, host, fmt.Sprintf("mkdir -pv %s && mv -f %s %s", filepath.Dir(localFilePath), middle, localFilePath))
	if err != nil {
		return errors.Wrap(err, "mv")
	}
	return errors.Wrap(ret.Error(), "mv")
}

func (ss *SSH) download(host, localFilePath, remoteFilePath string) error {
	ret, err := CmdToString("mkdir", "-pv", filepath.Dir(localFilePath))
	if err != nil {
		return err
	}
	if err = ret.Error(); err != nil {
		return err
	}

	// if need run as exec,change to use scp cmd.
	if SSHToCmd(ss, host) {
		ret, err = CmdToString("scp", remoteFilePath, localFilePath)
		if err != nil {
			return err
		}
		return ret.Error()
	}

	sftpClient, err := ss.sftpConnect(host)
	if err != nil {
		return err
	}
	defer sftpClient.Close()
	srcFile, err := sftpClient.Open(remoteFilePath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(localFilePath)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	buf := make([]byte, 100*MB) // 100mb
	total := 0
	unit := ""
	for {
		n, _ := srcFile.Read(buf)
		if n == 0 {
			break
		}
		length, _ := dstFile.Write(buf[0:n])
		isKb := length/MB < 1
		speed := 0
		if isKb {
			total += length
			unit = "KB"
			speed = length / KB
		} else {
			total += length
			unit = "MB"
			speed = length / MB
		}
		totalLength, totalUnit := toSizeFromInt(total)
		logger.Infof("[%s]transfer total size is: %.2f%s ;speed is %d%s", host, totalLength, totalUnit, speed, unit)
	}
	return nil
}

// SftpConnect  is
func (ss *SSH) sftpConnect(host string) (*sftp.Client, error) {
	sshClient, err := ss.connect(host)
	if err != nil {
		return nil, err
	}
	return sftp.NewClient(sshClient)
}

func toSizeFromInt(length int) (float64, string) {
	isMb := length/MB > 1
	value, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(length)/MB), 64)
	if isMb {
		return value, "MB"
	}
	value, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(length)/KB), 64)
	return value, "KB"

}

func (ss *SSH) CopySudoWithBar(bar *mpb.Bar, host, localFilePath, remoteFilePath string) error {
	return ss.CopySudoWithBarWithTempDir(bar, host, localFilePath, remoteFilePath, defaultCopyTempDir)
}

// CopySudoWithBarWithTempDir copies a file with progress through tempDir when sudo is required.

// middleFileName builds a collision-free flat file name for the non-root
// transit copy. The name is unique per invocation: a deterministic name would
// collide with a root-owned leftover from an earlier privileged join, and the
// sftp PUT into that existing file fails with permission denied (R21).
func middleFileName(remoteFilePath string) string {
	sum := sha256.Sum256([]byte(remoteFilePath))
	var nonce [4]byte
	if _, err := crand.Read(nonce[:]); err != nil {
		return fmt.Sprintf("kc-transit-%s-%d-%s", hex.EncodeToString(sum[:8]), time.Now().UnixNano(), filepath.Base(remoteFilePath))
	}
	return fmt.Sprintf("kc-transit-%s-%s-%s", hex.EncodeToString(sum[:8]), hex.EncodeToString(nonce[:]), filepath.Base(remoteFilePath))
}

func (ss *SSH) CopySudoWithBarWithTempDir(bar *mpb.Bar, host, localFilePath, remoteFilePath, tempDir string) error {
	if ss.User == "root" { // root user,need not transit
		return ss.CopyWithBar(bar, host, localFilePath, remoteFilePath)
	}
	// if not root, first scp to a flat path under the temp directory, then
	// sudo mv to target. The middle path must not mirror the target path:
	// transit mkdir runs without sudo and a target parent like /tmp/etc owned
	// by root would make a non-root join fail with permission denied (R21).
	middle := filepath.Join(tempDir, middleFileName(remoteFilePath))
	err := ss.CopyWithBar(bar, host, localFilePath, middle)
	if err != nil {
		return errors.Wrap(err, "copy")
	}
	ret, err := SSHCmdWithSudo(ss, host, fmt.Sprintf("mkdir -pv %s && mv -f %s %s", filepath.Dir(remoteFilePath), middle, remoteFilePath))
	if err != nil {
		return errors.Wrap(err, "mv")
	}
	return errors.Wrap(ret.Error(), "mv")
}

func (ss *SSH) CopyWithBar(bar *mpb.Bar, host, localFilePath, remoteFilePath string) error {
	// do mkdir to ensure remote dir always exists
	ret, err := SSHCmd(ss, host, fmt.Sprintf("mkdir -pv %s", filepath.Dir(remoteFilePath)))
	if err != nil {
		return err
	}
	if err = ret.Error(); err != nil {
		return err
	}
	// if need run as exec,change to use scp cmd.
	if SSHToCmd(ss, host) {
		ret, err = CmdToString("scp", localFilePath, remoteFilePath)
		if err != nil {
			return err
		}
		return ret.Error()
	}

	sftpClient, err := ss.sftpConnect(host)
	if err != nil {
		return err
	}
	defer sftpClient.Close()
	srcFile, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := sftpClient.Create(remoteFilePath)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	stat, err := srcFile.Stat()
	if err != nil {
		return err
	}
	bar.SetTotal(stat.Size(), false)

	proxyReader := bar.ProxyReader(srcFile)

	_, err = io.Copy(dstFile, proxyReader)
	return err
}
