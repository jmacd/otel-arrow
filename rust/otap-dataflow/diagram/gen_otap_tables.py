#!/usr/bin/env python3
"""
Generate an SVG slide that renders the OTAP-Logs columnar tables
corresponding to the two OTLP records defined in `gen_otlp_bytes.py`,
with a small copy of the OTLP byte rows in the bottom-left so the
row -> column transformation is easy to read.

Colors are reused verbatim from the OTLP slide:

  green  = resource attributes
  purple = scope attributes
  blue   = log record scalar fields
  orange = log attributes
  grey   = structural ids (parent_id / id / resource / scope)

So a green attribute byte run in the OTLP picture "lands" in the green
resource_attrs table, and so on. The whole slide is data-driven from
the same `Record` model the OTLP slide uses; nothing is duplicated.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
import os, sys

import gen_otlp_bytes as otlp  # encoder, palette, RECORDS

# ---------------------------------------------------------------- geometry
PAGE_W            = 1600
PAGE_MARGIN_X     = 60
TOP_MARGIN        = 110
BOTTOM_MARGIN     = 60

TABLE_GAP         = 40
CELL_W_DEFAULT    = 60
CELL_H            = 28
ARRAY_GAP         = 10            # gap between arrays within a RecordBatch
FRAME_PAD_X       = 14
FRAME_PAD_Y_TOP   = 36            # room for "<name> · RecordBatch" label
FRAME_PAD_Y_BOT   = 14
HEADER_TEXT_H     = 16            # column-name text (the "map key")
HEADER_RULE_GAP   = 4             # space between header text and array cells
GHOST_GAP         = 6             # space between last cell and ghost cell
GHOST_CELL_H      = 16            # dashed "more rows could go here" placeholder
GROW_ARROW_H      = 12            # ▼ under the ghost cell

COLOR_FRAME_FILL  = "#fafbfc"
COLOR_FRAME_LINE  = "#cbd2d9"
COLOR_TITLE_SUB   = "#7b8794"

MINI_BYTE_W       = 13
MINI_BYTE_H       = 16
MINI_BYTE_GAP     = 1
MINI_BYTE_FONT    = 8
MINI_LABEL_W      = 90
MINI_RECORD_GAP   = 12
MINI_TOP_GAP      = 70

FONT              = "Inter, 'Helvetica Neue', Arial, sans-serif"
FONT_MONO         = "'JetBrains Mono', Menlo, Consolas, monospace"

COLOR_BG          = "#ffffff"
COLOR_TEXT        = "#1f2933"
COLOR_SUB         = "#52606d"

# Section palette is the SAME map as the OTLP slide -- do not redefine.
SECTION_COLORS    = otlp.SECTION_COLORS

# ---------------------------------------------------------------- model
@dataclass
class Cell:
    text: str
    cat: str           # key in SECTION_COLORS

@dataclass
class Table:
    title: str
    columns: List[Cell]
    rows: List[List[Cell]]
    col_widths: Optional[List[int]] = None

    def widths(self) -> List[int]:
        return self.col_widths or [CELL_W_DEFAULT] * len(self.columns)

    def inner_w(self) -> int:
        n = len(self.columns)
        return sum(self.widths()) + (n - 1) * ARRAY_GAP

    def width(self) -> int:
        return self.inner_w() + 2 * FRAME_PAD_X

    def height(self) -> int:
        return (FRAME_PAD_Y_TOP + HEADER_TEXT_H + HEADER_RULE_GAP
                + len(self.rows) * CELL_H
                + GHOST_GAP + GHOST_CELL_H
                + FRAME_PAD_Y_BOT)

# ---------------------------------------------------------------- data
def build_tables() -> List[Table]:
    res_attrs = Table(
        title="resource_attrs",
        columns=[Cell("parent_id", "struct"),
                 Cell("key",       "res_attrs"),
                 Cell("str",       "res_attrs")],
        rows=[
            [Cell("0", "struct"), Cell("svc", "res_attrs"), Cell("A", "res_attrs")],
            [Cell("1", "struct"), Cell("svc", "res_attrs"), Cell("B", "res_attrs")],
        ],
        col_widths=[60, 55, 50],
    )
    scope_attrs = Table(
        title="scope_attrs",
        columns=[Cell("parent_id", "struct"),
                 Cell("key",       "scope_attrs"),
                 Cell("str",       "scope_attrs")],
        rows=[
            [Cell("1", "struct"), Cell("v", "scope_attrs"), Cell("1", "scope_attrs")],
        ],
        col_widths=[60, 55, 50],
    )
    log_attrs = Table(
        title="log_attrs",
        columns=[Cell("parent_id", "struct"),
                 Cell("key",       "log_attrs"),
                 Cell("str",       "log_attrs")],
        rows=[
            [Cell("0", "struct"), Cell("k",    "log_attrs"), Cell("v",   "log_attrs")],
            [Cell("1", "struct"), Cell("n",    "log_attrs"), Cell("2",   "log_attrs")],
            [Cell("2", "struct"), Cell("user", "log_attrs"), Cell("bob", "log_attrs")],
            [Cell("2", "struct"), Cell("err",  "log_attrs"), Cell("io",  "log_attrs")],
        ],
        col_widths=[60, 55, 55],
    )
    logs = Table(
        title="logs",
        columns=[Cell("id",         "struct"),
                 Cell("resource",   "struct"),
                 Cell("scope",      "struct"),
                 Cell("scope.name", "scope_name"),
                 Cell("severity",   "log_field"),
                 Cell("body",       "log_field")],
        rows=[
            [Cell("0", "struct"), Cell("0", "struct"), Cell("0", "struct"),
             Cell("s", "scope_name"),
             Cell("9",  "log_field"), Cell("hi",    "log_field")],
            [Cell("1", "struct"), Cell("0", "struct"), Cell("0", "struct"),
             Cell("s", "scope_name"),
             Cell("13", "log_field"), Cell("oops",  "log_field")],
            [Cell("2", "struct"), Cell("1", "struct"), Cell("1", "struct"),
             Cell("s", "scope_name"),
             Cell("17", "log_field"), Cell("oh no", "log_field")],
        ],
        col_widths=[35, 65, 50, 75, 60, 65],
    )
    return [res_attrs, scope_attrs, log_attrs, logs]

# ---------------------------------------------------------------- render
def render_table(out: List[str], x0: float, y0: float, t: Table) -> None:
    widths = t.widths()

    # ---- frame around the whole RecordBatch ----
    frame_w = t.width()
    frame_h = t.height()
    out.append(f'<rect x="{x0:.1f}" y="{y0:.1f}" '
               f'width="{frame_w}" height="{frame_h}" rx="6" ry="6" '
               f'fill="{COLOR_FRAME_FILL}" stroke="{COLOR_FRAME_LINE}" '
               f'stroke-width="1"/>')

    # Title in the upper-left of the frame: "<name>" + small subtitle.
    out.append(f'<text x="{x0 + FRAME_PAD_X:.1f}" y="{y0 + 20:.1f}" '
               f'font-family="{FONT}" font-size="14" font-weight="700" '
               f'fill="{COLOR_TEXT}">{t.title}</text>')

    # ---- columns: name (map key) -> array (values) ----
    cells_y0 = y0 + FRAME_PAD_Y_TOP + HEADER_TEXT_H + HEADER_RULE_GAP
    header_baseline = y0 + FRAME_PAD_Y_TOP + HEADER_TEXT_H - 2

    cx = x0 + FRAME_PAD_X
    for col_idx, (col, w) in enumerate(zip(t.columns, widths)):
        fill, stroke = SECTION_COLORS[col.cat]
        cx_center = cx + w / 2

        # Column name (the key in the name->array map). Plain text in the
        # column's stroke color so the eye binds name and array together.
        out.append(f'<text x="{cx_center:.1f}" y="{header_baseline:.1f}" '
                   f'text-anchor="middle" font-family="{FONT_MONO}" '
                   f'font-size="11" font-weight="700" fill="{stroke}">'
                   f'{col.text}</text>')

        # Array cells (one Arrow array, contiguous downward).
        for row_idx, row in enumerate(t.rows):
            cell = row[col_idx]
            cy = cells_y0 + row_idx * CELL_H
            cf, cs = SECTION_COLORS[cell.cat]
            out.append(f'<rect x="{cx:.1f}" y="{cy:.1f}" '
                       f'width="{w}" height="{CELL_H}" '
                       f'fill="{cf}" stroke="{cs}" stroke-width="0.7"/>')
            out.append(f'<text x="{cx_center:.1f}" '
                       f'y="{cy + CELL_H/2 + 4:.1f}" '
                       f'text-anchor="middle" font-family="{FONT_MONO}" '
                       f'font-size="11" fill="{COLOR_TEXT}">{cell.text}</text>')

        # Continuation: dashed ghost cell + ⋮ + downward chevron, in the
        # column's stroke color, to convey "this array grows downward."
        gy = cells_y0 + len(t.rows) * CELL_H + GHOST_GAP
        out.append(f'<rect x="{cx:.1f}" y="{gy:.1f}" '
                   f'width="{w}" height="{GHOST_CELL_H}" '
                   f'fill="none" stroke="{stroke}" stroke-width="0.7" '
                   f'stroke-dasharray="3,3"/>')
        out.append(f'<text x="{cx_center:.1f}" '
                   f'y="{gy + GHOST_CELL_H/2 + 4:.1f}" '
                   f'text-anchor="middle" font-family="{FONT_MONO}" '
                   f'font-size="11" fill="{stroke}">⋮</text>')

        cx += w + ARRAY_GAP

def render_mini_record(out: List[str], x0: float, y0: float,
                       label: str, blk) -> None:
    out.append(f'<text x="{x0}" y="{y0 + MINI_BYTE_H - 2:.1f}" '
               f'font-family="{FONT}" font-size="12" font-weight="700" '
               f'fill="{COLOR_TEXT}">{label}</text>')
    bx0 = x0 + MINI_LABEL_W
    for i, (val, cat) in enumerate(blk.bytes):
        x = bx0 + i * (MINI_BYTE_W + MINI_BYTE_GAP)
        fill, stroke = SECTION_COLORS[cat]
        out.append(f'<rect x="{x:.1f}" y="{y0:.1f}" '
                   f'width="{MINI_BYTE_W}" height="{MINI_BYTE_H}" '
                   f'fill="{fill}" stroke="{stroke}" stroke-width="0.5" '
                   f'rx="1.5" ry="1.5"/>')
        ch = otlp.printable(val)
        out.append(f'<text x="{x + MINI_BYTE_W/2:.1f}" '
                   f'y="{y0 + MINI_BYTE_H/2 + 3:.1f}" '
                   f'text-anchor="middle" font-family="{FONT_MONO}" '
                   f'font-size="{MINI_BYTE_FONT}" fill="{COLOR_TEXT}">'
                   f'{ch}</text>')

def render_legend(out: List[str], x: float, y: float) -> None:
    items = [
        ("struct",      "Identifier"),
        ("res_attrs",   "Resource attrs"),
        ("scope_name",  "Scope name"),
        ("scope_attrs", "Scope attrs"),
        ("log_field",   "LogRecord fields"),
        ("log_attrs",   "Log attrs"),
    ]
    out.append(f'<g font-family="{FONT}" font-size="12" fill="{COLOR_TEXT}">')
    cx = x
    for cat, label in items:
        fill, stroke = SECTION_COLORS[cat]
        out.append(f'<rect x="{cx:.1f}" y="{y - 12:.1f}" '
                   f'width="16" height="16" fill="{fill}" '
                   f'stroke="{stroke}" stroke-width="0.8" rx="2" ry="2"/>')
        out.append(f'<text x="{cx + 22:.1f}" y="{y + 1:.1f}">{label}</text>')
        cx += 22 + 7 * len(label) + 22
    out.append('</g>')

# ---------------------------------------------------------------- page
def render_svg() -> str:
    tables = build_tables()
    blocks = [otlp.build_record(r) for r in otlp.RECORDS]

    total_tables_w = sum(t.width() for t in tables) + TABLE_GAP * (len(tables) - 1)
    tables_x0 = max(PAGE_MARGIN_X, (PAGE_W - total_tables_w) // 2)

    table_h_max = max(t.height() for t in tables)
    table_y     = TOP_MARGIN

    mini_y0 = table_y + table_h_max + MINI_TOP_GAP

    record_h = MINI_BYTE_H + MINI_RECORD_GAP
    legend_y = mini_y0 + len(blocks) * record_h + 50
    page_h   = legend_y + BOTTOM_MARGIN

    out: List[str] = []
    out.append(f'<svg xmlns="http://www.w3.org/2000/svg" '
               f'width="{PAGE_W}" height="{page_h}" '
               f'viewBox="0 0 {PAGE_W} {page_h}">')
    out.append(f'<rect width="100%" height="100%" fill="{COLOR_BG}"/>')

    out.append(f'<g font-family="{FONT}" fill="{COLOR_TEXT}">')
    out.append(f'<text x="{PAGE_MARGIN_X}" y="50" font-size="26" '
               f'font-weight="700">OTAP Logs — four Arrow RecordBatches'
               f'</text>')
    out.append('</g>')

    cx = tables_x0
    for t in tables:
        render_table(out, cx, table_y, t)
        cx += t.width() + TABLE_GAP

    out.append(f'<text x="{PAGE_MARGIN_X}" y="{mini_y0 - 18:.1f}" '
               f'font-family="{FONT}" font-size="13" font-weight="700" '
               f'fill="{COLOR_SUB}">'
               f'Source OTLP records'
               f'</text>')

    ry = mini_y0
    for r, blk in zip(otlp.RECORDS, blocks):
        render_mini_record(out, PAGE_MARGIN_X, ry, r.title, blk)
        ry += record_h

    render_legend(out, PAGE_MARGIN_X, legend_y)

    out.append('</svg>')
    return "\n".join(out)

# ---------------------------------------------------------------- main
def main(argv: List[str]) -> int:
    out_path = argv[1] if len(argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "otap_tables.svg")
    svg = render_svg()
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(svg)
    print(f"wrote {out_path}  ({len(svg)} bytes)")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
