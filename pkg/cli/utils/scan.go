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
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/term"
)

func AskForConfirmation() bool {
	var response string

	if _, err := fmt.Scanln(&response); err != nil {
		// Scanning fails when stdin is closed (no TTY): report it as a normal
		// refusal instead of dumping a fatal goroutine stack. Non-interactive
		// callers are expected to pass --assumeyes (R25).
		fmt.Fprintln(os.Stderr, "no interactive input available; pass --assumeyes (-y) to confirm non-interactively")
		return false
	}

	switch strings.ToLower(response) {
	case "y", "yes":
		return true
	case "n", "no":
		return false
	default:
		fmt.Println("I'm sorry but I didn't get what you meant, please type (y)es or (n)o and then press enter:")
		return AskForConfirmation()
	}
}

func WaitInputPasswd() (string, error) {
	terminal := term.IsTerminal(int(os.Stdin.Fd()))
	if !terminal {
		return "", errors.New("operation not supported by device")
	}
	pBytes, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	return string(pBytes), nil
}
