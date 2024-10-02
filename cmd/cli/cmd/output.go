/*
 * From https://github.com/superfly/flyctl/blob/19481dfa110c8a6274500321b3214c5d2abbfc71/internal/render/render.go
 * Copyright 2023 https://github.com/superfly
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
 *
 * Modifications by https://github.com/stephen.
 */
package cmd

import (
	"fmt"
	"io"

	"github.com/olekukonko/tablewriter"
)

func Table(w io.Writer, rows [][]string, cols ...string) error {
	table := tablewriter.NewWriter(w)

	if len(cols) > 0 {
		table.SetHeader(cols)
	}

	table.SetBorder(false)
	table.SetHeaderLine(false)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetColumnSeparator(" ")
	table.SetNoWhiteSpace(true)
	table.SetTablePadding("\t")

	table.AppendBulk(rows)

	table.Render()

	fmt.Fprintln(w)

	return nil
}
