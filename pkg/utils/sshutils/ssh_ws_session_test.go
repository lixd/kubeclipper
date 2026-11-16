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
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/ssh"
)

type terminalTestConnection struct {
	net.Conn
	blockWrites  atomic.Bool
	writeStarted chan struct{}
	closed       chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
}

func (connection *terminalTestConnection) Write(data []byte) (int, error) {
	if connection.blockWrites.Load() {
		connection.writeOnce.Do(func() { close(connection.writeStarted) })
		<-connection.closed
		return 0, net.ErrClosed
	}
	return connection.Conn.Write(data)
}

func (connection *terminalTestConnection) Close() error {
	err := connection.Conn.Close()
	connection.closeOnce.Do(func() { close(connection.closed) })
	return err
}

func newTerminalTestSession(t *testing.T) (*LogicSSHWsSession, *websocket.Conn, ssh.Channel, <-chan struct{}, *terminalTestConnection) {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{NoClientAuth: true}
	config.AddHostKey(signer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	channels := make(chan ssh.Channel, 1)
	serverErrors := make(chan error, 1)
	remoteClosed := make(chan struct{})
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErrors <- acceptErr
			return
		}
		server, incoming, requests, handshakeErr := ssh.NewServerConn(connection, config)
		if handshakeErr != nil {
			_ = connection.Close()
			serverErrors <- handshakeErr
			return
		}
		defer server.Close()
		go ssh.DiscardRequests(requests)
		channelRequest := <-incoming
		if channelRequest == nil {
			serverErrors <- fmt.Errorf("SSH session channel was not opened")
			return
		}
		channel, channelRequests, channelErr := channelRequest.Accept()
		if channelErr != nil {
			serverErrors <- channelErr
			return
		}
		go func() {
			_, _ = io.Copy(io.Discard, channel)
			close(remoteClosed)
		}()
		for request := range channelRequests {
			_ = request.Reply(true, nil)
			if request.Type == "shell" {
				channels <- channel
			}
		}
	}()
	tcpConnection, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	transport := &terminalTestConnection{
		Conn:         tcpConnection,
		writeStarted: make(chan struct{}),
		closed:       make(chan struct{}),
	}
	t.Cleanup(func() { _ = transport.Close() })
	clientConnection, incomingChannels, incomingRequests, err := ssh.NewClientConn(transport, listener.Addr().String(), &ssh.ClientConfig{
		User:            "terminal-test",
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	})
	if err != nil {
		t.Fatal(err)
	}
	sshClient := ssh.NewClient(clientConnection, incomingChannels, incomingRequests)
	t.Cleanup(func() { _ = sshClient.Close() })

	connections := make(chan *websocket.Conn, 1)
	webServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upgrader := websocket.Upgrader{}
		connection, upgradeErr := upgrader.Upgrade(writer, request, nil)
		if upgradeErr != nil {
			serverErrors <- upgradeErr
			return
		}
		connections <- connection
	}))
	t.Cleanup(webServer.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(webServer.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	serverConnection := <-connections
	t.Cleanup(func() { _ = serverConnection.Close() })
	session, err := NewLoginSSHWSSession(80, 24, sshClient, serverConnection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Close)
	select {
	case channel := <-channels:
		return session, client, channel, remoteClosed, transport
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("SSH shell did not start")
	}
	return nil, nil, nil, nil, nil
}

func TestTerminalClientDisconnectBroadcastsShutdown(t *testing.T) {
	for _, graceful := range []bool{false, true} {
		t.Run(fmt.Sprintf("close-frame=%t", graceful), func(t *testing.T) {
			session, client, _, remoteClosed, _ := newTerminalTestSession(t)
			session.Start()
			var waiters sync.WaitGroup
			for i := 0; i < 8; i++ {
				waiters.Add(1)
				go func() {
					defer waiters.Done()
					select {
					case <-session.Done():
					case <-time.After(5 * time.Second):
						t.Error("terminal shutdown was not broadcast to every waiter")
					}
				}()
			}
			if graceful {
				err := client.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				_ = client.Close()
			}
			waiters.Wait()
			select {
			case <-remoteClosed:
			case <-time.After(5 * time.Second):
				t.Fatal("SSH shell remained open after the WebSocket disconnected")
			}
		})
	}
}

func TestTerminalRemoteExitFlushesOutput(t *testing.T) {
	session, client, remote, _, _ := newTerminalTestSession(t)
	session.Start()
	expected := bytes.Repeat([]byte("terminal output\n"), 1024)
	if _, err := remote.Write(expected); err != nil {
		t.Fatal(err)
	}
	_, err := remote.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
	if err != nil {
		t.Fatal(err)
	}
	_ = remote.Close()
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	var output []byte
	for {
		_, data, err := client.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				t.Fatalf("terminal closed with %v", err)
			}
			break
		}
		output = append(output, data...)
	}
	if !bytes.Equal(output, expected) {
		t.Fatalf("received %d output bytes, want %d", len(output), len(expected))
	}
	select {
	case <-session.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("remote shell exit did not close the terminal session")
	}
}

func TestTerminalCloseIsConcurrentAndIdempotent(t *testing.T) {
	session, _, _, remoteClosed, _ := newTerminalTestSession(t)
	session.Start()
	var closers sync.WaitGroup
	for i := 0; i < 8; i++ {
		closers.Add(1)
		go func() {
			defer closers.Done()
			session.Close()
		}()
	}
	closers.Wait()
	select {
	case <-remoteClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("closing the terminal did not close the SSH shell")
	}
}

func TestTerminalBufferDrainCopiesOutput(t *testing.T) {
	var output safeBuffer
	_, _ = output.Write([]byte("before"))
	first := output.Drain()
	_, _ = output.Write([]byte("after"))
	if string(first) != "before" {
		t.Fatalf("drained output changed to %q after a later write", first)
	}
	if got := string(output.Drain()); got != "after" {
		t.Fatalf("remaining output = %q, want after", got)
	}
}

func TestTerminalCloseInterruptsBlockedSSHWrite(t *testing.T) {
	session, _, _, remoteClosed, transport := newTerminalTestSession(t)
	// Ensure a failing Close test can still tear down its blocked transport.
	t.Cleanup(func() { _ = transport.Close() })
	session.Start()
	transport.blockWrites.Store(true)
	writeDone := make(chan error, 1)
	go func() {
		_, err := session.session.SendRequest("terminal-test-blocked-write", true, nil)
		writeDone <- err
	}()
	select {
	case <-transport.writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("SSH write did not reach the blocked TCP transport")
	}
	closeDone := make(chan struct{})
	go func() {
		session.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal Close blocked behind an SSH write")
	}
	select {
	case err := <-writeDone:
		if err == nil {
			t.Fatal("blocked SSH write succeeded after transport shutdown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing the terminal did not interrupt the SSH write")
	}
	select {
	case <-remoteClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("SSH shell remained open after transport shutdown")
	}
}
