// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package column

import (
	"fmt"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/memory"

	"otel-arrow-adapter/pkg/air/rfield"
	"otel-arrow-adapter/pkg/air/stats"
)

// StructColumn is a column of struct data.
type StructColumn struct {
	fieldStringPath string
	name            string
	structType      arrow.DataType
	columns         *Columns
	metadata        arrow.Metadata
}

// NewStructColumn creates a new Struct column.
func NewStructColumn(fieldStringPath string, name string, metadata arrow.Metadata, structType arrow.DataType, columns *Columns) *StructColumn {
	return &StructColumn{
		fieldStringPath: fieldStringPath,
		name:            name,
		structType:      structType,
		columns:         columns,
		metadata:        metadata,
	}
}

// Push pushes the value to the column.
func (c *StructColumn) Push(fieldPath *rfield.FieldPath, field *rfield.Field) {
	c.columns.UpdateColumn(fieldPath, c.fieldStringPath, field)
}

// Name returns the name of the column.
func (c *StructColumn) Name() string {
	return c.name
}

// Len returns the number of elements in the column.
func (c *StructColumn) Len() int {
	return c.columns.Len()
}

// Clear resets the column to its initial state.
func (c *StructColumn) Clear() {
	c.columns.Clear()
}

// PushFromValues adds the given values to the column.
func (c *StructColumn) PushFromValues(fieldPath *rfield.FieldPath, data []rfield.Value) {
	for _, value := range data {
		for i, field := range value.(*rfield.Struct).Fields {
			c.Push(fieldPath.ChildPath(i), field)
		}
	}
}

// NewArrowField returns an Arrow field for the column.
func (c *StructColumn) NewArrowField() *arrow.Field {
	// Create struct field
	fieldRefs := c.columns.NewArrowFields()

	// Create a struct field.
	fields := make([]arrow.Field, len(fieldRefs))
	names := map[string]bool{}
	for i, field := range fieldRefs {
		fields[i] = *field
		if _, ok := names[field.Name]; ok {
			panic(fmt.Sprintf("duplicate field name %q (in struct %q)", field.Name, c.name))
		} else {
			names[field.Name] = true
		}
	}
	return &arrow.Field{Name: c.name, Type: arrow.StructOf(fields...), Metadata: c.metadata}
}

// NewArray returns a new array for the column.
func (c *StructColumn) NewArray(allocator *memory.GoAllocator) arrow.Array {
	fieldArrays, err := c.columns.NewArrays(allocator)
	if err != nil {
		panic(err)
	}
	fieldRefs := c.columns.NewArrowFields()

	// Create a struct field.
	fields := make([]arrow.Field, len(fieldRefs))
	for i, field := range fieldRefs {
		fields[i] = *field
	}

	children := make([]arrow.ArrayData, len(fieldArrays))
	for i, fieldArray := range fieldArrays {
		defer fieldArray.Release()
		children[i] = fieldArray.Data()
	}
	c.structType = arrow.StructOf(fields...)
	data := array.NewData(c.structType, children[0].Len(), []*memory.Buffer{nil, nil}, children, 0, 0)
	defer data.Release()
	structArray := array.NewStructData(data)

	c.Clear()

	return structArray
}

// Build builds the column.
func (c *StructColumn) Build(allocator *memory.GoAllocator) (*arrow.Field, arrow.Array, error) {
	// Create struct field
	fieldRefs := c.columns.NewArrowFields()
	fieldArrays, err := c.columns.NewArrays(allocator)
	if err != nil {
		return nil, nil, err
	}

	// Create a struct field.
	fields := make([]arrow.Field, len(fieldRefs))
	for i, field := range fieldRefs {
		fields[i] = *field
	}
	structField := arrow.Field{Name: c.name, Type: arrow.StructOf(fields...)}

	// Create struct array.
	children := make([]arrow.ArrayData, len(fieldArrays))
	for i, fieldArray := range fieldArrays {
		defer fieldArray.Release()
		children[i] = fieldArray.Data()
	}
	c.structType = arrow.StructOf(fields...)
	data := array.NewData(c.structType, children[0].Len(), []*memory.Buffer{nil, nil}, children, 0, 0)
	defer data.Release()
	structArray := array.NewStructData(data)

	c.Clear()

	return &structField, structArray, nil
}

// DictionaryStats returns the dictionary statistics of the column.
func (c *StructColumn) DictionaryStats(parentPath string) []*stats.DictionaryStats {
	if len(parentPath) > 0 {
		parentPath += "." + c.name
	} else {
		parentPath = c.name
	}
	return c.columns.DictionaryStats(parentPath)
}

// Type returns the type of the column.
func (c *StructColumn) Type() arrow.DataType {
	return c.structType
}

// Metadata returns the metadata of the column.
func (c *StructColumn) Metadata() *ColumnMetadata {
	return &ColumnMetadata{
		Field:    c.NewArrowField(),
		Len:      0,
		Children: c.columns.Metadata(),
	}
}