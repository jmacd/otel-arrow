# OTAP Data Frame Specification

**Version:** 0.1.0  
**Last Updated:** May 9, 2025

## Table of Contents
1. [Introduction](#1-introduction)
2. [Core Concepts](#2-core-concepts)
3. [Attribute Encoding](#3-attribute-encoding)
4. [RelatedData Structure](#4-relateddata-structure)
5. [Handling Collections and Map Types](#5-handling-collections-and-map-types)
6. [Optimization Techniques](#6-optimization-techniques)
7. [Relationship to OTLP](#7-relationship-to-otlp)
8. [Examples](#8-examples)

## 1. Introduction

The OpenTelemetry Arrow Protocol (OTAP) is a binary interchange format for OpenTelemetry data using Apache Arrow. This document specifies how OpenTelemetry data is encoded in OTAP, with specific focus on how attributes are represented.

OTAP aims to provide:
- Efficient storage and transmission of telemetry data
- Columnar access patterns for improved query performance
- Compatibility with the OpenTelemetry Protocol (OTLP) data model

## 2. Core Concepts

### 2.1 Record Batch Organization

OTAP organizes telemetry data into a collection of Arrow RecordBatches, each representing a specific aspect of the telemetry data:

- Resource data (including resource attributes)
- Scope data (including scope attributes) 
- Spans (including span attributes)
- Events
- Links
- Log records
- Metrics data points

Each record batch contains rows representing individual telemetry objects or their components.

### 2.2 Data Relationships

Data relationships in OTAP are established through ID references:

- Resources are assigned unique identifiers (resource_id)
- Scopes reference their parent resource using resource_id
- Spans reference their parent scope using scope_id
- Events and links reference their parent span using span_id
- Log records reference their parent scope using scope_id
- Metric data points reference their parent metric using metric_id

IDs are assigned sequentially within a batch, starting from 0, and are scoped to the current payload.

### 2.3 Schema Identification

Each Record Batch in OTAP is identified by a schema_id in the schema metadata:

```
schema_id -> "resource_attributes" | "scope_attributes" | "span_attributes" | "span_event_attributes" | "span_link_attributes" | ...
```

## 3. Attribute Encoding

### 3.1 Attribute Schema

Attributes in OTAP are encoded as a record batch with the following schema:

| Field Name        | Data Type         | Nullable | Description                                       |
|-------------------|-------------------|----------|---------------------------------------------------|
| parent_id         | UInt16 or UInt32  | No       | ID of parent object (e.g., resource, scope, span) |
| key               | String            | No       | Attribute key name                                |
| type              | UInt8             | No       | Value type code (see below)                       |
| string_value      | String            | Yes      | String value if type=1                            |
| int_value         | Int64             | Yes      | Integer value if type=2                           |
| double_value      | Float64           | Yes      | Double value if type=3                            |
| bool_value        | Boolean           | Yes      | Boolean value if type=4                           |
| bytes_value       | Binary            | Yes      | Binary value if type=5                            |
| serialized_value  | Binary            | Yes      | For array/map/other complex types                 |

### 3.2 Value Type Codes

| Type Code | Type                | Value Column       |
|-----------|---------------------|-------------------|
| 1         | String              | string_value      |
| 2         | Int                 | int_value         |
| 3         | Double              | double_value      |
| 4         | Bool                | bool_value        |
| 5         | Bytes               | bytes_value       |
| 6         | Array               | serialized_value  |
| 7         | KeyValue (Map)      | serialized_value  |

### 3.3 Parent ID Type by Context

The `parent_id` field type varies depending on the attribute context:

- **UInt16**: Used for resource attributes, scope attributes, span attributes, log attributes
- **UInt32**: Used for span events, span links, and metric data points where cardinality may be higher

### 3.4 Optional Sorting Strategies

Attributes can be stored in different sort orders:

1. **Unsorted**: No specific sort order is applied
2. **By ParentID**: Attributes are sorted by parent_id
3. **By ParentID + Key**: Attributes are sorted first by parent_id, then by key 
4. **By ParentID + Key + Value**: Attributes are sorted by parent_id, key, then by value

The sort order affects physical storage and can improve compression ratios.

## 4. RelatedData Structure

RelatedData is a logical grouping of multiple attribute RecordBatches that organizes attributes by context for a specific telemetry signal (traces, metrics, logs).

### 4.1 TracesRelatedData

Contains the following attribute sets:

- `res_attr_map_store`: Resource attributes (UInt16 parent_id)
- `scope_attr_map_store`: Scope attributes (UInt16 parent_id) 
- `span_attr_map_store`: Span attributes (UInt16 parent_id)
- `span_event_attr_map_store`: Span event attributes (UInt32 parent_id)
- `span_link_attr_map_store`: Span link attributes (UInt32 parent_id)

### 4.2 LogsRelatedData

Contains the following attribute sets:

- `res_attr_map_store`: Resource attributes (UInt16 parent_id)
- `scope_attr_map_store`: Scope attributes (UInt16 parent_id)
- `log_record_attr_map_store`: Log record attributes (UInt16 parent_id)

### 4.3 MetricsRelatedData

Contains the following attribute sets:

- `res_attr_map_store`: Resource attributes (UInt16 parent_id)
- `scope_attr_map_store`: Scope attributes (UInt16 parent_id)
- `number_dp_attrs_store`: Number data point attributes (UInt32 parent_id)
- `summary_attrs_store`: Summary data point attributes (UInt32 parent_id)
- `histogram_attrs_store`: Histogram data point attributes (UInt32 parent_id)
- `exp_histogram_attrs_store`: Exponential histogram data point attributes (UInt32 parent_id)

## 5. Handling Collections and Map Types

Complex attributes (arrays, maps) are serialized to a binary format in the `serialized_value` column.

### 5.1 Array Value Encoding

Array values are serialized with:
- Type code: 6
- `serialized_value`: Contains the binary representation of the array elements

### 5.2 Map/KeyValue Encoding

KeyValue (map) attributes are serialized with:
- Type code: 7
- `serialized_value`: Contains the binary representation of the map entries

## 6. Optimization Techniques

### 6.1 Dictionary Encoding

String columns (key, string_value) may use Arrow dictionary encoding to reduce memory usage and improve query performance.

### 6.2 Delta Encoding

OTAP may use delta encoding for sequences of related values, especially for timestamps or IDs that change incrementally.

### 6.3 Run-Length Encoding

For columns with repeated values (like parent_id), run-length encoding can significantly reduce storage size.

## 7. Relationship to OTLP

OTAP is a physical representation of the same data model defined by OpenTelemetry Protocol (OTLP). The key differences are:

- OTLP uses Protocol Buffers with a hierarchical message structure
- OTAP uses columnar Apache Arrow format with normalized tables and ID references
- OTLP nests attributes within their parent objects
- OTAP separates attributes into dedicated tables referenced by parent_id

This separation in OTAP provides performance advantages for columnar processing while maintaining the logical relationships present in the original OTLP data.

## 8. Examples

### 8.1 Example 1: Encoding Resource Attributes

#### 8.1.1 OTLP Resource Attributes

```protobuf
resource {
  attributes {
    key: "host.name"
    value {
      string_value: "host1"
    }
  }
  attributes {
    key: "service.name"
    value {
      string_value: "service1"
    }
  }
}
```

#### 8.1.2 OTAP Encoded Attributes

| parent_id | key            | type | string_value | int_value | double_value | bool_value | bytes_value | serialized_value |
|-----------|----------------|------|--------------|-----------|--------------|------------|-------------|------------------|
| 0         | "host.name"    | 1    | "host1"      |           |              |            |             |                  |
| 0         | "service.name" | 1    | "service1"   |           |              |            |             |                  |

### 8.2 Example 2: Encoding Span Attributes

#### 8.2.1 OTLP Span Attributes

```protobuf
span {
  trace_id: "trace1"
  span_id: "span1"
  parent_span_id: "span0"
  attributes {
    key: "http.method"
    value {
      string_value: "GET"
    }
  }
}
```

#### 8.2.2 OTAP Encoded Attributes

| parent_id | key            | type | string_value | int_value | double_value | bool_value | bytes_value | serialized_value |
|-----------|----------------|------|--------------|-----------|--------------|------------|-------------|------------------|
| 0         | "http.method"  | 1    | "GET"        |           |              |            |             |                  |

### 8.3 Example 3: Encoding Metrics Data Point Attributes

#### 8.3.1 OTLP Metrics Data Point

```protobuf
metric {
  name: "cpu.usage"
  description: "CPU Usage"
  unit: "percentage"
  data_points {
    attributes {
      key: "host.name"
      value {
        string_value: "host1"
      }
    }
    value: 75.5
  }
}
```

#### 8.3.2 OTAP Encoded Attributes

| parent_id | key            | type | string_value | int_value | double_value | bool_value | bytes_value | serialized_value |
|-----------|----------------|------|--------------|-----------|--------------|------------|-------------|------------------|
| 0         | "host.name"    | 1    | "host1"      |           |              |            |             |                  |
