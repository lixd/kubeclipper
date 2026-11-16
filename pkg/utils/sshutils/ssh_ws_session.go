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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/ssh"

	"github.com/kubeclipper/kubeclipper/pkg/logger"
)

const (
	wsMsgCmd    = "cmd"
	wsMsgResize = "resize"

	MagicExecShell = "(clear && bash) || (clear && ash) || (clear && sh) || bash || ash || sh"
)

type wsMsg struct {
	Type string `json:"type"`
	Cmd  string `json:"cmd"`
	Cols int    `json:"cols"`
	Rows int    `json:"rows"`
}

func NewSSHClient(username, password, ip string, port int) (*ssh.Client, error) {
	config := &ssh.ClientConfig{
		Timeout:         5 * time.Second,
		User:            username,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth:            []ssh.AuthMethod{ssh.Password(password)},
	}
	c, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", ip, port), config)
	if err != nil {
		return nil, err
	}
	return c, nil
}

type safeBuffer struct {
	buffer bytes.Buffer
	mu     sync.Mutex
}

func (w *safeBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(p)
}
func (w *safeBuffer) Drain() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	data := append([]byte(nil), w.buffer.Bytes()...)
	w.buffer.Reset()
	return data
}

type LogicSSHWsSession struct {
	stdinPipe   io.WriteCloser
	comboOutput *safeBuffer
	logBuff     *safeBuffer
	client      *ssh.Client
	session     *ssh.Session
	wsConn      *websocket.Conn
	done        chan struct{}
	shellDone   chan struct{}
	closeOnce   sync.Once
}

// NewLoginSSHWSSession owns sshClient and closes it with the terminal.
func NewLoginSSHWSSession(cols, rows int, sshClient *ssh.Client, wsConn *websocket.Conn) (*LogicSSHWsSession, error) {
	sshSession, err := sshClient.NewSession()
	if err != nil {
		return nil, err
	}
	stdinP, err := sshSession.StdinPipe()
	if err != nil {
		_ = sshSession.Close()
		return nil, err
	}
	comboWriter := new(safeBuffer)
	logBuf := new(safeBuffer)
	//ssh.stdout and stderr will write output into comboWriter
	sshSession.Stdout = comboWriter
	sshSession.Stderr = comboWriter
	modes := ssh.TerminalModes{
		ssh.ECHO:          1,     // disable echo
		ssh.TTY_OP_ISPEED: 14400, // input speed = 14.4kbaud
		ssh.TTY_OP_OSPEED: 14400, // output speed = 14.4kbaud
	}
	// Request pseudo terminal
	if err := sshSession.RequestPty("xterm", rows, cols, modes); err != nil {
		_ = sshSession.Close()
		return nil, err
	}
	// Start remote shell
	if err := sshSession.Shell(); err != nil {
		_ = sshSession.Close()
		return nil, err
	}
	return &LogicSSHWsSession{
		stdinPipe:   stdinP,
		comboOutput: comboWriter,
		logBuff:     logBuf,
		client:      sshClient,
		session:     sshSession,
		wsConn:      wsConn,
		done:        make(chan struct{}),
		shellDone:   make(chan struct{}),
	}, nil
}

// Close broadcasts shutdown and interrupts any blocked SSH or WebSocket I/O.
func (sws *LogicSSHWsSession) Close() {
	sws.closeOnce.Do(func() {
		close(sws.done)
		_ = sws.wsConn.Close()
		// Closing the transport first interrupts SSH writes, including a
		// channel close packet blocked behind an earlier write.
		_ = sws.client.Close()
		_ = sws.session.Close()
	})
}

func (sws *LogicSSHWsSession) Done() <-chan struct{} {
	return sws.done
}

func (sws *LogicSSHWsSession) Start() {
	go sws.receiveWsMsg()
	go sws.sendComboOutput()
	go func() {
		if err := sws.session.Wait(); err != nil {
			logger.Errorf("ssh session wait failed due to error: %s", err.Error())
		}
		close(sws.shellDone)
	}()
}

// receiveWsMsg  receive websocket msg do some handling then write into ssh.session.stdin
func (sws *LogicSSHWsSession) receiveWsMsg() {
	wsConn := sws.wsConn
	defer sws.Close()
	for {
		select {
		case <-sws.done:
			return
		default:
			//read websocket msg
			_, wsData, err := wsConn.ReadMessage()
			if err != nil {
				logger.Errorf("reading webSocket message failed due to error: %s", err.Error())
				return
			}
			//unmashal bytes into struct
			msgObj := wsMsg{}
			if err := json.Unmarshal(wsData, &msgObj); err != nil {
				logger.Errorf("unmarshal websocket message failed due to error: %s, wsData is %s", err.Error(), string(wsData))
			}
			switch msgObj.Type {
			case wsMsgResize:
				//handle xterm.js size change
				if msgObj.Cols > 0 && msgObj.Rows > 0 {
					if err := sws.session.WindowChange(msgObj.Rows, msgObj.Cols); err != nil {
						logger.Errorf("ssh pty change windows size failed due to error: %s", err.Error())
					}
				}
			case wsMsgCmd:
				//handle xterm.js stdin
				decodeBytes, err := base64.StdEncoding.DecodeString(msgObj.Cmd)
				if err != nil {
					logger.Errorf("websock cmd string base64 decoding failed due to error: %s", err.Error())
				}
				if _, err := sws.stdinPipe.Write(decodeBytes); err != nil {
					logger.Errorf("ws cmd bytes write to ssh.stdin pipe failed due to error: %s", err.Error())
					return
				}
			}
		}
	}
}

func (sws *LogicSSHWsSession) sendComboOutput() {
	defer sws.Close()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := sws.sendOutput(); err != nil {
				logger.Errorf("ssh sending combo output to webSocket failed due to error: %s", err.Error())
				return
			}
		case <-sws.shellDone:
			// Wait has joined the SSH output copy loops, so this drains all
			// remaining output before the terminal's close frame.
			if err := sws.sendOutput(); err != nil {
				logger.Errorf("ssh sending final output to webSocket failed due to error: %s", err.Error())
				return
			}
			message := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
			_ = sws.wsConn.WriteControl(websocket.CloseMessage, message, time.Now().Add(writeWait))
			return
		case <-sws.done:
			return
		}
	}
}

func (sws *LogicSSHWsSession) sendOutput() error {
	data := sws.comboOutput.Drain()
	if len(data) == 0 {
		return nil
	}
	logger.Debugf("output data", zap.ByteString("data", data))
	if err := sws.wsConn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	if err := sws.wsConn.WriteMessage(websocket.TextMessage, data); err != nil {
		return err
	}
	_, err := sws.logBuff.Write(data)
	return err
}
