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

package printer

import (
	"encoding/json"
	"fmt"
	"io"

	"sigs.k8s.io/yaml"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

type PrintFlags struct {
	format string
}

func (p *PrintFlags) AllowedFormats() []string {
	if p == nil {
		return []string{}
	}
	return []string{"json", "yaml", "table"}
}

// Print renders the response in the format selected by the CLI flags.
func (p *PrintFlags) Print(pr ResourcePrinter, w io.Writer) error {
	if err := p.Validate(); err != nil {
		return err
	}
	switch p.format {
	case "json":
		data, err := pr.JSONPrint()
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		if err != nil {
			return err
		}
		_, err = w.Write([]byte("\n"))
		return err
	case "yaml":
		data, err := pr.YAMLPrint()
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	case "table":
		fallthrough
	default:
		table := tablewriter.NewWriter(w)
		headers, data := pr.TablePrint()
		table.SetHeader(headers)
		for _, v := range data {
			table.Append(v)
		}
		table.Render()
		return nil
	}
}

// Format returns the requested output format.
func (p *PrintFlags) Format() string { return p.format }

// Validate rejects unsupported formats before a command contacts the API.
func (p *PrintFlags) Validate() error {
	switch p.format {
	case "json", "yaml", "table":
		return nil
	}
	return fmt.Errorf("unsupported output format %q; choose json, yaml or table", p.format)
}

// PrintList preserves the list envelope even when it contains a single object.
// Detail and create commands retain their single-object printers.
func (p *PrintFlags) PrintList(pr ResourcePrinter, w io.Writer) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.format == "table" {
		return p.Print(pr, w)
	}
	data, err := json.Marshal(pr)
	if err != nil {
		return err
	}
	var list map[string]json.RawMessage
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	if len(list["items"]) == 0 || string(list["items"]) == "null" {
		list["items"] = json.RawMessage("[]")
	}
	return p.PrintData(list, w)
}

// PrintData writes a structured value without adding human-readable messages.
func (p *PrintFlags) PrintData(value interface{}, w io.Writer) error {
	var data []byte
	var err error
	switch p.format {
	case "json":
		data, err = JSONPrinter(value)
		data = append(data, '\n')
	case "yaml":
		data, err = YAMLPrinter(value)
	default:
		return fmt.Errorf("structured output requires json or yaml")
	}
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (p *PrintFlags) AddFlags(c *cobra.Command) {
	if p == nil {
		return
	}
	c.Flags().StringVarP(&p.format, "output", "o", p.format, "Output format either: json,yaml,table")
}

func NewPrintFlags() *PrintFlags {
	return &PrintFlags{format: "table"}
}

func JSONPrinter(data interface{}) ([]byte, error) {
	return json.MarshalIndent(data, "", "\t")
}

func YAMLPrinter(data interface{}) ([]byte, error) {
	b, err := JSONPrinter(data)
	if err != nil {
		return nil, err
	}
	return yaml.JSONToYAML(b)
}
