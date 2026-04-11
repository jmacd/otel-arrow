#!/usr/bin/env python3
"""Generate SVG diagrams showing OTAP metrics tables in columnar format.

Produces four diagrams:
  0. Overview of all OTAP metrics table schemas (columns + types only)
  1. Scope attributes, flat metrics (worked example)
  2. Scope attributes as dimensional metrics (worked example)
  3. Data point attributes (worked example)

The worked examples use the "consumer.items" counter metric from the
internal-metrics-sdk design, with 1 dimension (outcome: success /
failed / refused) producing 3 timeseries.

Usage:
    python3 tools/otap-diagram/generate_otap_svg.py

Output goes to tools/otap-diagram/output/*.svg
"""

import os
from dataclasses import dataclass, field

# ── Styling constants ──────────────────────────────────────────────

FONT = "Consolas, 'Courier New', monospace"
FONT_SIZE = 12
HEADER_FONT_SIZE = 11
TITLE_FONT_SIZE = 16
SUBTITLE_FONT_SIZE = 13

CELL_H = 22
CELL_PAD = 8
HEADER_H = 26
TABLE_GAP = 40
TABLE_HGAP = 30
TABLE_TITLE_H = 28
MARGIN = 30
ARROW_MARKER_SIZE = 8

COL_BG = "#f8f9fa"
COL_TABLE_TITLE = "#2c3e50"
COL_TABLE_TITLE_TEXT = "#ffffff"
COL_HEADER_BG = "#34495e"
COL_HEADER_TEXT = "#ecf0f1"
COL_ROW_EVEN = "#ffffff"
COL_ROW_ODD = "#f0f4f8"
COL_BORDER = "#bdc3c7"
COL_TEXT = "#2c3e50"
COL_TYPE_TEXT = "#7f8c8d"
COL_ID_HIGHLIGHT = "#e8f4fd"
COL_FK_LINE = "#3498db"


@dataclass
class Column:
    name: str
    arrow_type: str
    values: list = field(default_factory=list)
    is_id: bool = False
    is_fk: bool = False
    width: int = 0


@dataclass
class Table:
    name: str
    payload_type: str
    columns: list
    x: int = 0
    y: int = 0
    width: int = 0
    height: int = 0


def measure_text(text: str) -> int:
    return len(str(text)) * 7 + CELL_PAD * 2


def compute_col_widths(table: Table):
    for col in table.columns:
        header_w = measure_text(col.name)
        type_w = measure_text(col.arrow_type) - CELL_PAD
        val_w = max((measure_text(str(v)) for v in col.values), default=0)
        col.width = max(header_w, type_w, val_w, 60)


def compute_table_size(table: Table):
    compute_col_widths(table)
    table.width = sum(c.width for c in table.columns)
    n_rows = max(len(c.values) for c in table.columns) if table.columns else 0
    table.height = TABLE_TITLE_H + HEADER_H + CELL_H + n_rows * CELL_H


def xml_escape(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── SVG rendering ──────────────────────────────────────────────────

def render_table(table: Table) -> str:
    parts = []
    x0, y0 = table.x, table.y
    n_rows = max(len(c.values) for c in table.columns) if table.columns else 0

    # Title banner
    parts.append(
        f'<rect x="{x0}" y="{y0}" width="{table.width}" height="{TABLE_TITLE_H}" '
        f'rx="6" ry="6" fill="{COL_TABLE_TITLE}"/>'
    )
    parts.append(
        f'<rect x="{x0}" y="{y0 + TABLE_TITLE_H - 6}" width="{table.width}" '
        f'height="6" fill="{COL_TABLE_TITLE}"/>'
    )
    parts.append(
        f'<text x="{x0 + table.width // 2}" y="{y0 + TABLE_TITLE_H - 8}" '
        f'text-anchor="middle" fill="{COL_TABLE_TITLE_TEXT}" '
        f'font-family="{FONT}" font-size="{HEADER_FONT_SIZE}" font-weight="bold">'
        f'{xml_escape(table.name)}</text>'
    )
    parts.append(
        f'<text x="{x0 + table.width // 2}" y="{y0 + TABLE_TITLE_H - 19}" '
        f'text-anchor="middle" fill="{COL_TYPE_TEXT}" '
        f'font-family="{FONT}" font-size="9" opacity="0.7">'
        f'{xml_escape(table.payload_type)}</text>'
    )

    cy = y0 + TABLE_TITLE_H

    # Column headers
    cx = x0
    for col in table.columns:
        bg = COL_ID_HIGHLIGHT if col.is_id or col.is_fk else COL_HEADER_BG
        tc = COL_TEXT if col.is_id or col.is_fk else COL_HEADER_TEXT
        parts.append(
            f'<rect x="{cx}" y="{cy}" width="{col.width}" height="{HEADER_H}" '
            f'fill="{bg}" stroke="{COL_BORDER}" stroke-width="0.5"/>'
        )
        parts.append(
            f'<text x="{cx + col.width // 2}" y="{cy + 11}" '
            f'text-anchor="middle" fill="{tc}" '
            f'font-family="{FONT}" font-size="{HEADER_FONT_SIZE}" font-weight="bold">'
            f'{xml_escape(col.name)}</text>'
        )
        cx += col.width
    cy += HEADER_H

    # Type row
    cx = x0
    for col in table.columns:
        parts.append(
            f'<rect x="{cx}" y="{cy}" width="{col.width}" height="{CELL_H}" '
            f'fill="{COL_ROW_ODD}" stroke="{COL_BORDER}" stroke-width="0.5"/>'
        )
        parts.append(
            f'<text x="{cx + col.width // 2}" y="{cy + 15}" '
            f'text-anchor="middle" fill="{COL_TYPE_TEXT}" '
            f'font-family="{FONT}" font-size="10" font-style="italic">'
            f'{xml_escape(col.arrow_type)}</text>'
        )
        cx += col.width
    cy += CELL_H

    # Data rows
    for row_idx in range(n_rows):
        cx = x0
        bg = COL_ROW_EVEN if row_idx % 2 == 0 else COL_ROW_ODD
        for col in table.columns:
            cell_bg = COL_ID_HIGHLIGHT if (col.is_id or col.is_fk) else bg
            val = col.values[row_idx] if row_idx < len(col.values) else ""
            parts.append(
                f'<rect x="{cx}" y="{cy}" width="{col.width}" height="{CELL_H}" '
                f'fill="{cell_bg}" stroke="{COL_BORDER}" stroke-width="0.5"/>'
            )
            parts.append(
                f'<text x="{cx + col.width // 2}" y="{cy + 15}" '
                f'text-anchor="middle" fill="{COL_TEXT}" '
                f'font-family="{FONT}" font-size="{FONT_SIZE}">'
                f'{xml_escape(val)}</text>'
            )
            cx += col.width
        cy += CELL_H

    # Outline
    parts.append(
        f'<rect x="{x0}" y="{y0}" width="{table.width}" height="{table.height}" '
        f'rx="6" ry="6" fill="none" stroke="{COL_BORDER}" stroke-width="1.5"/>'
    )
    return "\n".join(parts)


def col_center_x(table, col_name):
    """X center of a named column in a table."""
    cx = table.x
    for c in table.columns:
        if c.name == col_name:
            return cx + c.width // 2
        cx += c.width
    return table.x + table.width // 2


def col_header_y(table):
    """Y center of the column header row."""
    return table.y + TABLE_TITLE_H + HEADER_H // 2


DOT_R = 5          # radius of the connection dot
STUB_LEN = 18      # vertical stub before the curve begins


def render_fk_arrow(src_table, src_col_name, dst_table, dst_col_name,
                    label="", color=COL_FK_LINE, offset=0, **kw) -> str:
    """Draw an FK arrow with colored dots on the column headers and a
    short vertical stub so the origin/destination columns are obvious."""
    sx = col_center_x(src_table, src_col_name)
    dx = col_center_x(dst_table, dst_col_name)

    # Dots sit on the column header row
    src_dot_y = col_header_y(src_table)
    dst_dot_y = col_header_y(dst_table)

    # The path runs from below the source table to above the dest table
    sy = src_table.y + src_table.height
    dy = dst_table.y

    # Stub: drop straight down from the table edge, then curve
    stub_sy = sy + STUB_LEN
    stub_dy = dy - STUB_LEN
    mid_y = (stub_sy + stub_dy) / 2 + offset

    cid = color.replace("#", "")
    path = (
        f'M {sx},{sy} '          # start at bottom of src table
        f'L {sx},{stub_sy} '     # vertical stub down
        f'C {sx},{mid_y} {dx},{mid_y} {dx},{stub_dy} '  # curve
        f'L {dx},{dy}'           # vertical stub up into dst
    )

    parts = [
        # Colored dot on source column header
        f'<circle cx="{sx}" cy="{src_dot_y}" r="{DOT_R}" '
        f'fill="{color}" opacity="0.85"/>',
        # Colored dot on destination column header
        f'<circle cx="{dx}" cy="{dst_dot_y}" r="{DOT_R}" '
        f'fill="{color}" opacity="0.85"/>',
        # The arrow path
        f'<path d="{path}" fill="none" stroke="{color}" '
        f'stroke-width="1.5" stroke-dasharray="6,3" '
        f'marker-end="url(#ah-{cid})"/>',
    ]
    if label:
        lx = (sx + dx) / 2
        ly = mid_y - 8
        # White background for readability
        parts.append(
            f'<rect x="{lx - len(label) * 3.5 - 4}" y="{ly - 10}" '
            f'width="{len(label) * 7 + 8}" height="14" rx="3" '
            f'fill="{COL_BG}" opacity="0.9"/>'
        )
        parts.append(
            f'<text x="{lx}" y="{ly}" text-anchor="middle" fill="{color}" '
            f'font-family="{FONT}" font-size="10" font-weight="bold">'
            f'{xml_escape(label)}</text>'
        )
    return "\n".join(parts)


def arrow_marker(color):
    cid = color.replace("#", "")
    s = ARROW_MARKER_SIZE
    return (
        f'<marker id="ah-{cid}" markerWidth="{s}" markerHeight="{s}" '
        f'refX="{s}" refY="{s // 2}" orient="auto">'
        f'<polygon points="0 0, {s} {s // 2}, 0 {s}" fill="{color}"/></marker>'
    )


def make_svg(tables, fk_arrows, title, subtitle="", note=""):
    for t in tables:
        compute_table_size(t)
    max_x = max(t.x + t.width for t in tables)
    max_y = max(t.y + t.height for t in tables)
    svg_w = max_x + MARGIN * 2
    svg_h = max_y + MARGIN * 2 + 60

    colors = {fk.get("color", COL_FK_LINE) for fk in fk_arrows}
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {svg_w} {svg_h}" width="{svg_w}" height="{svg_h}">',
        '<defs>',
    ]
    for c in colors:
        parts.append(arrow_marker(c))
    parts.append('</defs>')
    parts.append(f'<rect width="{svg_w}" height="{svg_h}" fill="{COL_BG}" rx="10"/>')
    parts.append(
        f'<text x="{svg_w // 2}" y="{MARGIN + 4}" text-anchor="middle" '
        f'fill="{COL_TABLE_TITLE}" font-family="{FONT}" '
        f'font-size="{TITLE_FONT_SIZE}" font-weight="bold">'
        f'{xml_escape(title)}</text>'
    )
    if subtitle:
        parts.append(
            f'<text x="{svg_w // 2}" y="{MARGIN + 22}" text-anchor="middle" '
            f'fill="{COL_TYPE_TEXT}" font-family="{FONT}" '
            f'font-size="{SUBTITLE_FONT_SIZE}">{xml_escape(subtitle)}</text>'
        )
    if note:
        parts.append(
            f'<text x="{svg_w // 2}" y="{svg_h - 12}" text-anchor="middle" '
            f'fill="{COL_TYPE_TEXT}" font-family="{FONT}" font-size="11" '
            f'font-style="italic">{xml_escape(note)}</text>'
        )
    for t in tables:
        parts.append(render_table(t))
    for fk in fk_arrows:
        parts.append(render_fk_arrow(**fk))
    parts.append('</svg>')
    return "\n".join(parts)


# ── Diagram builders ──────────────────────────────────────────────

def make_overview():
    """Schema overview of all OTAP metrics tables (no data rows)."""
    resource_attrs = Table("ResourceAttrs", "RESOURCE_ATTRS", [
        Column("parent_id", "uint16", [], is_fk=True),
        Column("key", "string/dict", []),
        Column("type", "uint8", []),
        Column("str", "string/dict", []),
        Column("int", "int64", []),
        Column("double", "float64", []),
        Column("bool", "bool", []),
        Column("bytes", "binary", []),
        Column("ser", "binary", []),
    ])
    scope_attrs = Table("ScopeAttrs", "SCOPE_ATTRS", [
        Column("parent_id", "uint16", [], is_fk=True),
        Column("key", "string/dict", []),
        Column("type", "uint8", []),
        Column("str", "string/dict", []),
        Column("int", "int64", []),
        Column("double", "float64", []),
        Column("bool", "bool", []),
        Column("bytes", "binary", []),
        Column("ser", "binary", []),
    ])
    metrics = Table("UnivariateMetrics", "UNIVARIATE_METRICS", [
        Column("id", "uint16", [], is_id=True),
        Column("resource{}", "struct", []),
        Column("scope{}", "struct", []),
        Column("schema_url", "string/dict", []),
        Column("metric_type", "uint8", []),
        Column("name", "string/dict", []),
        Column("description", "string/dict", []),
        Column("unit", "string/dict", []),
        Column("agg_temporality", "int32", []),
        Column("is_monotonic", "bool", []),
    ])
    number_dp = Table("NumberDataPoint", "NUMBER_DATA_POINTS", [
        Column("id", "uint32", [], is_id=True),
        Column("parent_id", "uint16", [], is_fk=True),
        Column("start_time_unix_nano", "timestamp_ns", []),
        Column("time_unix_nano", "timestamp_ns", []),
        Column("int_value", "int64", []),
        Column("double_value", "float64", []),
        Column("flags", "uint32", []),
    ])
    histogram_dp = Table("HistogramDataPoint", "HISTOGRAM_DATA_POINTS", [
        Column("id", "uint32", [], is_id=True),
        Column("parent_id", "uint16", [], is_fk=True),
        Column("start_time_unix_nano", "timestamp_ns", []),
        Column("time_unix_nano", "timestamp_ns", []),
        Column("count", "uint64", []),
        Column("sum", "float64", []),
        Column("bucket_counts", "list<uint64>", []),
        Column("explicit_bounds", "list<f64>", []),
        Column("flags", "uint32", []),
        Column("min", "float64", []),
        Column("max", "float64", []),
    ])
    ndp_attrs = Table("NumberDPAttrs", "NUMBER_DP_ATTRS", [
        Column("parent_id", "uint32", [], is_fk=True),
        Column("key", "string/dict", []),
        Column("type", "uint8", []),
        Column("str", "string/dict", []),
        Column("int", "int64", []),
        Column("double", "float64", []),
        Column("bool", "bool", []),
        Column("bytes", "binary", []),
        Column("ser", "binary", []),
    ])
    hdp_attrs = Table("HistogramDPAttrs", "HISTOGRAM_DP_ATTRS", [
        Column("parent_id", "uint32", [], is_fk=True),
        Column("key", "string/dict", []),
        Column("type", "uint8", []),
        Column("str", "string/dict", []),
        Column("int", "int64", []),
        Column("double", "float64", []),
        Column("bool", "bool", []),
        Column("bytes", "binary", []),
        Column("ser", "binary", []),
    ])

    for t in [resource_attrs, scope_attrs, metrics, number_dp,
              histogram_dp, ndp_attrs, hdp_attrs]:
        compute_table_size(t)

    metrics.x = MARGIN
    metrics.y = MARGIN + 40

    resource_attrs.x = MARGIN
    resource_attrs.y = metrics.y + metrics.height + TABLE_GAP
    scope_attrs.x = resource_attrs.x + resource_attrs.width + TABLE_HGAP
    scope_attrs.y = resource_attrs.y

    number_dp.x = MARGIN
    number_dp.y = resource_attrs.y + resource_attrs.height + TABLE_GAP
    histogram_dp.x = number_dp.x + number_dp.width + TABLE_HGAP
    histogram_dp.y = number_dp.y

    ndp_attrs.x = MARGIN
    ndp_attrs.y = number_dp.y + number_dp.height + TABLE_GAP
    hdp_attrs.x = ndp_attrs.x + ndp_attrs.width + TABLE_HGAP
    hdp_attrs.y = ndp_attrs.y

    return make_svg(
        [metrics, resource_attrs, scope_attrs, number_dp,
         histogram_dp, ndp_attrs, hdp_attrs],
        [
            dict(src_table=number_dp, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="id",
                 label="parent_id \u2192 id", color="#e74c3c"),
            dict(src_table=histogram_dp, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="id",
                 label="parent_id \u2192 id", color="#e74c3c", offset=10),
            dict(src_table=ndp_attrs, src_col_name="parent_id",
                 dst_table=number_dp, dst_col_name="id",
                 label="parent_id \u2192 id", color="#27ae60"),
            dict(src_table=hdp_attrs, src_col_name="parent_id",
                 dst_table=histogram_dp, dst_col_name="id",
                 label="parent_id \u2192 id", color="#27ae60"),
            dict(src_table=resource_attrs, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="resource{}",
                 label="parent_id \u2192 resource.id", color=COL_FK_LINE),
            dict(src_table=scope_attrs, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="scope{}",
                 label="parent_id \u2192 scope.id", color=COL_FK_LINE,
                 offset=-15),
        ],
        "OTAP Metrics Table Schemas \u2014 Overview",
        "Arrow table schemas showing column names, types, and foreign-key relationships",
    )


def _scope_attrs_cols(values_per_row):
    """Build the standard 9-column attr schema with given row data."""
    pids, keys, types, strs = [], [], [], []
    ints, doubles, bools, bytess, sers = [], [], [], [], []
    for pid, key, typ, strv in values_per_row:
        pids.append(pid)
        keys.append(key)
        types.append(typ)
        strs.append(strv)
        ints.append("")
        doubles.append("")
        bools.append("")
        bytess.append("")
        sers.append("")
    return [
        Column("parent_id", "uint16", pids, is_fk=True),
        Column("key", "string/dict", keys),
        Column("type", "uint8", types),
        Column("str", "string/dict", strs),
        Column("int", "int64", ints),
        Column("double", "float64", doubles),
        Column("bool", "bool", bools),
        Column("bytes", "binary", bytess),
        Column("ser", "binary", sers),
    ]


def make_encoding1():
    """Encoding 1: Scope attributes, flat metrics."""
    scope_attrs = Table("ScopeAttrs", "SCOPE_ATTRS", _scope_attrs_cols([
        (0, "node_id", "Str", "node-7"),
        (0, "pipeline", "Str", "ingest"),
    ]))
    metrics = Table("UnivariateMetrics", "UNIVARIATE_METRICS", [
        Column("id", "uint16", [0, 1, 2], is_id=True),
        Column("resource.id", "uint16", [0, 0, 0]),
        Column("scope.id", "uint16", [0, 0, 0]),
        Column("scope.name", "str/dict", ["otap", "otap", "otap"]),
        Column("metric_type", "uint8", ["Sum", "Sum", "Sum"]),
        Column("name", "str/dict", [
            "consumed_success", "consumed_failed", "consumed_refused"]),
        Column("unit", "str/dict", ["{item}", "{item}", "{item}"]),
        Column("agg_temp", "int32", ["Cum", "Cum", "Cum"]),
        Column("is_monotonic", "bool", [True, True, True]),
    ])
    ndp = Table("NumberDataPoint", "NUMBER_DATA_POINTS", [
        Column("id", "uint32", [0, 1, 2], is_id=True),
        Column("parent_id", "uint16", [0, 1, 2], is_fk=True),
        Column("start_time", "ts_ns", ["10:00:00", "10:00:00", "10:00:00"]),
        Column("time", "ts_ns", ["10:00:10", "10:00:10", "10:00:10"]),
        Column("int_value", "int64", [142, 3, 0]),
        Column("double_value", "float64", ["", "", ""]),
        Column("flags", "uint32", [0, 0, 0]),
    ])

    scope_attrs.x = MARGIN; scope_attrs.y = MARGIN + 40
    compute_table_size(scope_attrs)
    metrics.x = MARGIN; metrics.y = scope_attrs.y + scope_attrs.height + TABLE_GAP + 10
    compute_table_size(metrics)
    ndp.x = MARGIN; ndp.y = metrics.y + metrics.height + TABLE_GAP + 10
    compute_table_size(ndp)

    return make_svg(
        [scope_attrs, metrics, ndp],
        [
            dict(src_table=scope_attrs, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="scope.id",
                 label="parent_id \u2192 scope.id", color=COL_FK_LINE),
            dict(src_table=ndp, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="id",
                 label="parent_id \u2192 id", color="#e74c3c"),
        ],
        "OTAP Metrics: Scope Attributes, Flat Metrics",
        "Encoding 1 \u2014 1 dimension (outcome), 3 timeseries, M=1 metric",
        "Total rows: K + 6M = 2 + 6 = 8  (K=2 scope attrs, M=1 metric \u00d7 3 flat variants)",
    )


def make_encoding2():
    """Encoding 2: Scope attributes as dimensional metrics."""
    scope_attrs = Table("ScopeAttrs", "SCOPE_ATTRS", _scope_attrs_cols([
        (0, "node_id",  "Str", "node-7"),
        (0, "pipeline", "Str", "ingest"),
        (0, "outcome",  "Str", "success"),
        (1, "node_id",  "Str", "node-7"),
        (1, "pipeline", "Str", "ingest"),
        (1, "outcome",  "Str", "failed"),
        (2, "node_id",  "Str", "node-7"),
        (2, "pipeline", "Str", "ingest"),
        (2, "outcome",  "Str", "refused"),
    ]))
    metrics = Table("UnivariateMetrics", "UNIVARIATE_METRICS", [
        Column("id", "uint16", [0, 1, 2], is_id=True),
        Column("resource.id", "uint16", [0, 0, 0]),
        Column("scope.id", "uint16", [0, 1, 2]),
        Column("scope.name", "str/dict", ["otap", "otap", "otap"]),
        Column("metric_type", "uint8", ["Sum", "Sum", "Sum"]),
        Column("name", "str/dict", [
            "consumer.items", "consumer.items", "consumer.items"]),
        Column("unit", "str/dict", ["{item}", "{item}", "{item}"]),
        Column("agg_temp", "int32", ["Cum", "Cum", "Cum"]),
        Column("is_monotonic", "bool", [True, True, True]),
    ])
    ndp = Table("NumberDataPoint", "NUMBER_DATA_POINTS", [
        Column("id", "uint32", [0, 1, 2], is_id=True),
        Column("parent_id", "uint16", [0, 1, 2], is_fk=True),
        Column("start_time", "ts_ns", ["10:00:00", "10:00:00", "10:00:00"]),
        Column("time", "ts_ns", ["10:00:10", "10:00:10", "10:00:10"]),
        Column("int_value", "int64", [142, 3, 0]),
        Column("double_value", "float64", ["", "", ""]),
        Column("flags", "uint32", [0, 0, 0]),
    ])

    scope_attrs.x = MARGIN; scope_attrs.y = MARGIN + 40
    compute_table_size(scope_attrs)
    metrics.x = MARGIN; metrics.y = scope_attrs.y + scope_attrs.height + TABLE_GAP + 10
    compute_table_size(metrics)
    ndp.x = MARGIN; ndp.y = metrics.y + metrics.height + TABLE_GAP + 10
    compute_table_size(ndp)

    return make_svg(
        [scope_attrs, metrics, ndp],
        [
            dict(src_table=scope_attrs, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="scope.id",
                 label="parent_id \u2192 scope.id", color=COL_FK_LINE),
            dict(src_table=ndp, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="id",
                 label="parent_id \u2192 id", color="#e74c3c"),
        ],
        "OTAP Metrics: Scope Attributes as Dimensions",
        "Encoding 2 \u2014 1 dimension (outcome), 3 timeseries, M=1 metric",
        "Total rows: 3(K+1) + 6M = 3(2+1) + 6 = 15  (outcome promoted into scope attrs)",
    )


def make_encoding3():
    """Encoding 3: Data point attributes."""
    scope_attrs = Table("ScopeAttrs", "SCOPE_ATTRS", _scope_attrs_cols([
        (0, "node_id",  "Str", "node-7"),
        (0, "pipeline", "Str", "ingest"),
    ]))
    metrics = Table("UnivariateMetrics", "UNIVARIATE_METRICS", [
        Column("id", "uint16", [0], is_id=True),
        Column("resource.id", "uint16", [0]),
        Column("scope.id", "uint16", [0]),
        Column("scope.name", "str/dict", ["otap"]),
        Column("metric_type", "uint8", ["Sum"]),
        Column("name", "str/dict", ["consumer.items"]),
        Column("unit", "str/dict", ["{item}"]),
        Column("agg_temp", "int32", ["Cum"]),
        Column("is_monotonic", "bool", [True]),
    ])
    ndp = Table("NumberDataPoint", "NUMBER_DATA_POINTS", [
        Column("id", "uint32", [0, 1, 2], is_id=True),
        Column("parent_id", "uint16", [0, 0, 0], is_fk=True),
        Column("start_time", "ts_ns", ["10:00:00", "10:00:00", "10:00:00"]),
        Column("time", "ts_ns", ["10:00:10", "10:00:10", "10:00:10"]),
        Column("int_value", "int64", [142, 3, 0]),
        Column("double_value", "float64", ["", "", ""]),
        Column("flags", "uint32", [0, 0, 0]),
    ])
    dp_attrs = Table("NumberDPAttrs", "NUMBER_DP_ATTRS", _scope_attrs_cols([
        (0, "outcome", "Str", "success"),
        (1, "outcome", "Str", "failed"),
        (2, "outcome", "Str", "refused"),
    ]))
    # Override parent_id type to uint32 for dp attrs
    dp_attrs.columns[0] = Column("parent_id", "uint32",
                                  [0, 1, 2], is_fk=True)

    scope_attrs.x = MARGIN; scope_attrs.y = MARGIN + 40
    compute_table_size(scope_attrs)
    metrics.x = MARGIN; metrics.y = scope_attrs.y + scope_attrs.height + TABLE_GAP + 10
    compute_table_size(metrics)
    ndp.x = MARGIN; ndp.y = metrics.y + metrics.height + TABLE_GAP + 10
    compute_table_size(ndp)
    dp_attrs.x = MARGIN; dp_attrs.y = ndp.y + ndp.height + TABLE_GAP + 10
    compute_table_size(dp_attrs)

    return make_svg(
        [scope_attrs, metrics, ndp, dp_attrs],
        [
            dict(src_table=scope_attrs, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="scope.id",
                 label="parent_id \u2192 scope.id", color=COL_FK_LINE),
            dict(src_table=ndp, src_col_name="parent_id",
                 dst_table=metrics, dst_col_name="id",
                 label="parent_id \u2192 id", color="#e74c3c"),
            dict(src_table=dp_attrs, src_col_name="parent_id",
                 dst_table=ndp, dst_col_name="id",
                 label="parent_id \u2192 id", color="#27ae60"),
        ],
        "OTAP Metrics: Data Point Attributes",
        "Encoding 3 \u2014 1 dimension (outcome), 3 timeseries, M=1 metric",
        "Total rows: K + 7M = 2 + 7 = 9  (outcome as data-point-level attribute)",
    )


def main():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
    os.makedirs(out_dir, exist_ok=True)

    diagrams = [
        ("otap-metrics-overview.svg", make_overview),
        ("otap-metrics-encoding1-flat.svg", make_encoding1),
        ("otap-metrics-encoding2-scope-dims.svg", make_encoding2),
        ("otap-metrics-encoding3-dp-attrs.svg", make_encoding3),
    ]
    for name, fn in diagrams:
        path = os.path.join(out_dir, name)
        with open(path, "w") as f:
            f.write(fn())
        print(f"  \u2713 {path}")
    print(f"\nGenerated {len(diagrams)} SVG diagrams in {out_dir}/")


if __name__ == "__main__":
    main()
