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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"github.com/vbauerster/mpb/v8"

	"github.com/kubeclipper/kubeclipper/pkg/cli/logger"
)

const KB = 1024
const MB = 1024 * 1024
const privateFileMode os.FileMode = 0600

// CopyForMD5V2 copy and check md5
func (ss *SSH) CopyForMD5V2(host, localFilePath, remoteFilePath, localMD5 string) (bool, error) {
	var err error
	if localMD5 == "" {
		localMD5, err = MD5FromLocal(localFilePath)
		if err != nil {
			return false, err
		}
	}
	err = ss.CopySudo(host, localFilePath, remoteFilePath)
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
	if ss.User == "root" { // root user,need not transit
		return ss.Copy(host, localFilePath, remoteFilePath)
	}
	// 	if not root,first scp to /tmp,then sudo mv to target
	middle := filepath.Join("/tmp", remoteFilePath)
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

// CopySudoMode installs a sensitive local file atomically with the requested
// mode. The transit directory is private to the SSH user.
func (ss *SSH) CopySudoMode(host, localFilePath, remoteFilePath string, mode os.FileMode) error {
	transitDir := "/tmp/kc-copy-" + uuid.New().String()
	transitFile := filepath.Join(transitDir, filepath.Base(remoteFilePath))
	ret, err := SSHCmd(ss, host, fmt.Sprintf("umask 077 && mkdir -p %s", transitDir))
	if err != nil {
		return errors.Wrap(err, "create private transit directory")
	}
	if err = ret.Error(); err != nil {
		return errors.Wrap(err, "create private transit directory")
	}
	defer func() {
		_, _ = SSHCmd(ss, host, fmt.Sprintf("rm -rf %s", transitDir)) //nolint:errcheck // Best-effort remote cleanup.
	}()
	if err = ss.Copy(host, localFilePath, transitFile); err != nil {
		return errors.Wrap(err, "copy through private transit directory")
	}
	command := fmt.Sprintf("install -D -m %04o %s %s", mode.Perm(), transitFile, remoteFilePath)
	ret, err = SSHCmdWithSudo(ss, host, command)
	if err != nil {
		return errors.Wrap(err, "install copied file")
	}
	return errors.Wrap(ret.Error(), "install copied file")
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
	// A non-root SSH user cannot read root-owned configuration directly. Copy it
	// to a short-lived remote file owned by that user, then download it.
	middle := "/tmp/kc-download-" + uuid.New().String()
	ret, err := SSHCmdWithSudo(ss, host, fmt.Sprintf("install -m 0600 -o \"$(id -u)\" %s %s", remoteFilePath, middle))
	if err != nil {
		return errors.Wrap(err, "prepare remote file for download")
	}
	if err = ret.Error(); err != nil {
		return errors.Wrap(err, "prepare remote file for download")
	}
	defer func() {
		_, _ = SSHCmdWithSudo(ss, host, fmt.Sprintf("rm -f %s", middle)) //nolint:errcheck // Best-effort remote cleanup.
	}()
	return errors.Wrap(ss.download(host, localFilePath, middle), "download")
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

	dstFile, err := os.OpenFile(localFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, privateFileMode)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	if err := dstFile.Chmod(privateFileMode); err != nil {
		return err
	}
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
	if ss.User == "root" { // root user,need not transit
		return ss.CopyWithBar(bar, host, localFilePath, remoteFilePath)
	}
	// 	if not root,first scp to /tmp,then sudo mv to target
	middle := filepath.Join("/tmp", remoteFilePath)
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
