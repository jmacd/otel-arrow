#!/usr/bin/env python3
"""
Generate an SVG slide that renders OTLP Logs protobuf wire bytes as a
horizontal row of color-coded byte boxes, with brackets above naming the
logical sections (resource attributes, scope, scope attributes, log record
fields, log attributes).

This is the row-oriented "before" picture for the OTLP -> OTAP transformation
story. Each byte in the wire stream is encoded exactly as `protoc` would emit
it (varint tags, length-delimited submessages, etc.). Two small LogsData
messages are drawn so the reader can see structure and per-section coloring
side by side.

Conventions
-----------
- Color = section. Each logical section of the OTLP message has one color.
  Wrapper bytes (tag + length) for the *outer* message frames (LogsData,
  ResourceLogs, ScopeLogs, LogRecord) are drawn in the structural grey;
  wrapper bytes inside a section (e.g. KeyValue.tag, AnyValue.tag) inherit
  that section's color so the bracket above covers a contiguous run.
- Brackets above the byte row come in two tiers:
    tier 0 -- innermost section labels  (resource attrs, scope, log fields...)
    tier 1 -- outer message labels      (ResourceLogs, ScopeLogs, LogRecord)
- Printable ASCII bytes show the character; everything else shows two hex
  digits.

The model is intentionally tiny so the slide stays readable; expanding to
more attributes / records is just data.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Tuple, Optional
import os, sys

# ---------------------------------------------------------------- geometry
PAGE_W           = 1600
PAGE_MARGIN_X    = 60
TOP_MARGIN       = 110
BOTTOM_MARGIN    = 110

LEFT_RAIL_W      = 200          # per-record description column
ROW_GAP          = 70

BYTE_W           = 16
BYTE_H           = 22
BYTE_GAP         = 1
BYTE_FONT_SIZE   = 9
BYTE_LABEL_FONT  = "'JetBrains Mono', Menlo, Consolas, monospace"

# Vertical layout above each byte row (relative to the top of the box row).
TIER0_BAR_DY     = -14
TIER0_LABEL_DY   = -20
TIER1_BAR_DY     = -52
TIER1_LABEL_DY   = -58
TIER_STEP_DY     = -32          # additional offset per outer tier (2, 3, ...)
ROW_TOP_PAD      = 78 + 32 * 2  # space for tier0 + 3 outer tiers (1,2,3)

FONT             = "Inter, 'Helvetica Neue', Arial, sans-serif"

# ---------------------------------------------------------------- palette
# Distinct hues, similar saturation/lightness so no one section dominates.
COLOR_BG         = "#ffffff"
COLOR_TEXT       = "#1f2933"
COLOR_SUB        = "#52606d"
COLOR_BRACKET    = "#3e4c59"

# section -> (fill, stroke)
SECTION_COLORS = {
    "struct":      ("#e4e7eb", "#9aa5b1"),  # outer wrapper tag/length bytes
    "res_attrs":   ("#cfe8d4", "#5fa572"),  # resource attributes (green)
    "scope_name":  ("#dfd3ea", "#9a6fb0"),  # scope name (purple, lighter)
    "scope_attrs": ("#c9b6dd", "#7d4fa0"),  # scope attributes (purple, deeper)
    "log_field":   ("#cfe0f7", "#3f7ad6"),  # log record scalar fields (blue)
    "log_attrs":   ("#fbd9b3", "#d97706"),  # log attributes (orange)
}

SECTION_LABELS = {
    "res_attrs":   "Resource attrs",
    "scope_name":  "Scope name",
    "scope_attrs": "Scope attrs",
    "log_field":   "LogRecord fields",
    "log_attrs":   "Log attrs",
}

# ---------------------------------------------------------- proto encoders
def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)

def tag_bytes(field_num: int, wire_type: int) -> bytes:
    return varint((field_num << 3) | wire_type)

# ---------------------------------------------------------- byte/spans IR
TByte = Tuple[int, str]   # (byte value, category key)

@dataclass
class Span:
    start: int            # inclusive byte index
    end: int              # exclusive
    label: str
    tier: int             # 0 = innermost, 1 = outer message frame

@dataclass
class Block:
    bytes: List[TByte] = field(default_factory=list)
    spans: List[Span] = field(default_factory=list)

def b_concat(blocks: List[Block]) -> Block:
    out = Block()
    for blk in blocks:
        off = len(out.bytes)
        out.bytes.extend(blk.bytes)
        for s in blk.spans:
            out.spans.append(Span(s.start + off, s.end + off, s.label, s.tier))
    return out

def b_wrap(field_num: int, inner: Block, frame_cat: str,
           label: Optional[str] = None, tier: int = 1) -> Block:
    """Prepend a length-delimited wrapper (tag + varint length) using
    `frame_cat` for the wrapper bytes. Inner spans are shifted accordingly.
    If `label` is given, add a span covering the whole wrapped block."""
    prefix = tag_bytes(field_num, 2) + varint(len(inner.bytes))
    out = Block()
    out.bytes = [(b, frame_cat) for b in prefix] + list(inner.bytes)
    pre = len(prefix)
    for s in inner.spans:
        out.spans.append(Span(s.start + pre, s.end + pre, s.label, s.tier))
    if label is not None:
        out.spans.append(Span(0, len(out.bytes), label, tier))
    return out

def b_string_field(field_num: int, value: str, cat: str) -> Block:
    enc = value.encode("utf-8")
    raw = tag_bytes(field_num, 2) + varint(len(enc)) + enc
    return Block([(b, cat) for b in raw])

def b_varint_field(field_num: int, value: int, cat: str) -> Block:
    raw = tag_bytes(field_num, 0) + varint(value)
    return Block([(b, cat) for b in raw])

def b_kv_string(key: str, value: str, cat: str) -> Block:
    """KeyValue payload (no outer wrapper). All bytes carry `cat`."""
    av = b_string_field(1, value, cat)               # AnyValue.string_value
    return b_concat([
        b_string_field(1, key, cat),                 # KeyValue.key
        b_wrap(2, av, cat),                          # KeyValue.value (AnyValue)
    ])

# ---------------------------------------------------------------- record
@dataclass
class LogRec:
    severity: Optional[int] = None
    body: Optional[str] = None
    attrs: List[Tuple[str, str]] = field(default_factory=list)

@dataclass
class Record:
    title: str
    subtitle: str
    resource_attrs: List[Tuple[str, str]]
    scope_name: str
    scope_attrs: List[Tuple[str, str]]
    logs: List[LogRec]

def build_record(rec: Record) -> Block:
    # Build LogRecord payloads.
    lr_blocks: List[Block] = []
    for lr in rec.logs:
        parts: List[Block] = []
        if lr.severity is not None:
            parts.append(b_varint_field(2, lr.severity, "log_field"))
        if lr.body is not None:
            av = b_string_field(1, lr.body, "log_field")
            parts.append(b_wrap(5, av, "log_field"))   # LogRecord.body
        for k, v in lr.attrs:
            kv = b_kv_string(k, v, "log_attrs")
            parts.append(b_wrap(6, kv, "log_attrs"))   # LogRecord.attributes
        lr_blocks.append(b_concat(parts))

    # InstrumentationScope payload.
    isp_parts: List[Block] = [b_string_field(1, rec.scope_name, "scope_name")]
    for k, v in rec.scope_attrs:
        kv = b_kv_string(k, v, "scope_attrs")
        isp_parts.append(b_wrap(3, kv, "scope_attrs"))  # Scope.attributes
    isp = b_concat(isp_parts)

    # Mark inner section spans (tier 0).
    if rec.scope_attrs:
        # Scope name span = first emit, scope attrs span covers rest.
        name_len = len(isp_parts[0].bytes)
        isp.spans.append(Span(0, name_len, "scope name", 0))
        isp.spans.append(Span(name_len, len(isp.bytes),
                              "scope attrs", 0))
    else:
        isp.spans.append(Span(0, len(isp.bytes), "scope name", 0))

    # Wrap each LogRecord with section spans for its inner regions before
    # putting it into the ScopeLogs container.
    wrapped_lrs: List[Block] = []
    for i, (lr, blk) in enumerate(zip(rec.logs, lr_blocks)):
        # Compute split between scalar fields and attribute bytes.
        scalar_len = 0
        if lr.severity is not None:
            scalar_len += len(b_varint_field(2, lr.severity, "log_field").bytes)
        if lr.body is not None:
            av = b_string_field(1, lr.body, "log_field")
            scalar_len += len(b_wrap(5, av, "log_field").bytes)
        if scalar_len > 0:
            blk.spans.append(Span(0, scalar_len, "log fields", 0))
        if lr.attrs:
            blk.spans.append(Span(scalar_len, len(blk.bytes),
                                  "log attrs", 0))
        wrapped = b_wrap(2, blk, "struct",
                         label="LogRecord", tier=1)  # ScopeLogs.log_records
        wrapped_lrs.append(wrapped)

    # ScopeLogs payload = scope (wrapped) + log_records (wrapped).
    scope_msg = b_wrap(1, isp, "struct", label="Scope", tier=1)
    sl = b_concat([scope_msg] + wrapped_lrs)

    # Resource payload: list of attribute KVs each wrapped as field 1.
    resp_parts: List[Block] = []
    for k, v in rec.resource_attrs:
        kv = b_kv_string(k, v, "res_attrs")
        resp_parts.append(b_wrap(1, kv, "res_attrs"))   # Resource.attributes
    resp = b_concat(resp_parts)
    if rec.resource_attrs:
        resp.spans.append(Span(0, len(resp.bytes),
                               "resource attrs", 0))

    res_msg = b_wrap(1, resp, "struct", label="Resource", tier=1)

    rl = b_concat([res_msg, b_wrap(2, sl, "struct",
                                   label="ScopeLogs", tier=2)])
    rl_msg = b_wrap(1, rl, "struct", label="ResourceLogs", tier=3)

    # Outer LogsData -- single field, no extra wrapper for the whole thing
    # beyond what b_wrap already added. We DO want a final ResourceLogs span;
    # the LogsData itself contributes no bytes of its own (it is a virtual
    # container of repeated ResourceLogs entries on the wire).
    return rl_msg

# ---------------------------------------------------------------- records
RECORDS: List[Record] = [
    Record(
        title="Batch A",
        subtitle="2 log records",
        resource_attrs=[("svc", "A")],
        scope_name="s",
        scope_attrs=[],
        logs=[
            LogRec(severity=9,  body="hi",   attrs=[("k", "v")]),
            LogRec(severity=13, body="oops", attrs=[("n", "2")]),
        ],
    ),
    Record(
        title="Batch B",
        subtitle="1 log record",
        resource_attrs=[("svc", "B")],
        scope_name="s",
        scope_attrs=[("v", "1")],
        logs=[LogRec(severity=17, body="oh no",
                     attrs=[("user", "bob"), ("err", "io")])],
    ),
]

# ---------------------------------------------------------------- rendering
def printable(b: int) -> str:
    """Always two hex digits, so every box has consistent, distinct content."""
    return f"{b:02x}"

def render_record(out: List[str], y0: float, x_bytes: float,
                  rec: Record, blk: Block) -> float:
    # Left rail: title + subtitle.
    out.append(f'<g font-family="{FONT}" fill="{COLOR_TEXT}">')
    out.append(f'<text x="{PAGE_MARGIN_X}" y="{y0 + ROW_TOP_PAD + 4}" '
               f'font-size="20" font-weight="700">{rec.title}</text>')
    out.append(f'<text x="{PAGE_MARGIN_X}" y="{y0 + ROW_TOP_PAD + 24}" '
               f'font-size="11" fill="{COLOR_SUB}">'
               f'{len(blk.bytes)} bytes on the wire</text>')
    out.append('</g>')

    byte_top = y0 + ROW_TOP_PAD
    byte_bot = byte_top + BYTE_H

    # Bytes.
    for i, (val, cat) in enumerate(blk.bytes):
        x = x_bytes + i * (BYTE_W + BYTE_GAP)
        fill, stroke = SECTION_COLORS[cat]
        out.append(f'<rect x="{x:.1f}" y="{byte_top:.1f}" '
                   f'width="{BYTE_W}" height="{BYTE_H}" '
                   f'fill="{fill}" stroke="{stroke}" stroke-width="0.8" '
                   f'rx="2" ry="2"/>')
        ch = printable(val)
        out.append(f'<text x="{x + BYTE_W/2:.1f}" '
                   f'y="{byte_top + BYTE_H/2 + 3:.1f}" '
                   f'text-anchor="middle" '
                   f'font-family="{BYTE_LABEL_FONT}" '
                   f'font-size="{BYTE_FONT_SIZE}" fill="{COLOR_TEXT}">'
                   f'{ch}</text>')

    # Brackets above bytes.
    def bracket_x(idx: int) -> float:
        return x_bytes + idx * (BYTE_W + BYTE_GAP)

    for sp in blk.spans:
        x1 = bracket_x(sp.start) + 1.0
        x2 = bracket_x(sp.end) - BYTE_GAP - 1.0
        if sp.tier == 0:
            bar_y = byte_top + TIER0_BAR_DY
            lbl_y = byte_top + TIER0_LABEL_DY
            tick = 4
            stroke_w = 1.2
            font_size = 10
            color = COLOR_BRACKET
        else:
            # Outer tiers stack upward by TIER_STEP_DY per tier above 1.
            extra = (sp.tier - 1) * TIER_STEP_DY
            bar_y = byte_top + TIER1_BAR_DY + extra
            lbl_y = byte_top + TIER1_LABEL_DY + extra
            tick = 6 + (sp.tier - 1)
            stroke_w = 1.4 + 0.1 * (sp.tier - 1)
            font_size = 11 + (sp.tier - 1)
            color = COLOR_BRACKET
        # bar with downward end ticks (brackets pointing down to bytes)
        out.append(f'<path d="M{x1:.1f} {bar_y + tick:.1f} '
                   f'L{x1:.1f} {bar_y:.1f} '
                   f'L{x2:.1f} {bar_y:.1f} '
                   f'L{x2:.1f} {bar_y + tick:.1f}" '
                   f'fill="none" stroke="{color}" '
                   f'stroke-width="{stroke_w}" stroke-linecap="round"/>')
        cx = (x1 + x2) / 2
        out.append(f'<text x="{cx:.1f}" y="{lbl_y:.1f}" '
                   f'text-anchor="middle" font-family="{FONT}" '
                   f'font-size="{font_size}" fill="{color}">'
                   f'{sp.label}</text>')

    # Vertical y-extent used by this record.
    return byte_bot - y0

def render_legend(out: List[str], y: float) -> None:
    items = [
        ("struct",      "Message frame"),
        ("res_attrs",   SECTION_LABELS["res_attrs"]),
        ("scope_name",  SECTION_LABELS["scope_name"]),
        ("scope_attrs", SECTION_LABELS["scope_attrs"]),
        ("log_field",   SECTION_LABELS["log_field"]),
        ("log_attrs",   SECTION_LABELS["log_attrs"]),
    ]
    x = PAGE_MARGIN_X
    out.append(f'<g font-family="{FONT}" font-size="12" fill="{COLOR_TEXT}">')
    for cat, label in items:
        fill, stroke = SECTION_COLORS[cat]
        out.append(f'<rect x="{x}" y="{y - 12}" width="16" height="16" '
                   f'fill="{fill}" stroke="{stroke}" stroke-width="0.8" '
                   f'rx="2" ry="2"/>')
        out.append(f'<text x="{x + 22}" y="{y + 1}">{label}</text>')
        x += 22 + 8 * len(label) + 24  # rough width step
    out.append('</g>')

def render_svg(records: List[Record]) -> str:
    blocks = [build_record(r) for r in records]
    max_bytes = max(len(b.bytes) for b in blocks)
    track_w = max_bytes * (BYTE_W + BYTE_GAP) - BYTE_GAP
    x_bytes = PAGE_MARGIN_X + LEFT_RAIL_W
    needed_w = x_bytes + track_w + PAGE_MARGIN_X
    page_w = max(PAGE_W, int(needed_w))

    row_height = ROW_TOP_PAD + BYTE_H + 30  # bytes + small bottom pad
    page_h = (TOP_MARGIN
              + len(records) * row_height
              + (len(records) - 1) * ROW_GAP
              + BOTTOM_MARGIN)

    out: List[str] = []
    out.append(f'<svg xmlns="http://www.w3.org/2000/svg" '
               f'width="{page_w}" height="{page_h}" '
               f'viewBox="0 0 {page_w} {page_h}">')
    out.append(f'<rect width="100%" height="100%" fill="{COLOR_BG}"/>')

    # Title.
    out.append(f'<g font-family="{FONT}" fill="{COLOR_TEXT}">')
    out.append(f'<text x="{PAGE_MARGIN_X}" y="50" '
               f'font-size="26" font-weight="700">'
               f'OTLP Logs in protocol buffer bytes'
               f'</text>')
    out.append(f'<text x="{PAGE_MARGIN_X}" y="78" font-size="14" '
               f'fill="{COLOR_SUB}">'
               f'LogsData → ResourceLogs → ScopeLogs → LogRecord (with '
               f'attributes nested at each level). Each box is one wire byte.'
               f'</text>')
    out.append('</g>')

    y = TOP_MARGIN
    for rec, blk in zip(records, blocks):
        render_record(out, y, x_bytes, rec, blk)
        y += row_height + ROW_GAP

    render_legend(out, page_h - BOTTOM_MARGIN / 2)

    out.append('</svg>')
    return "\n".join(out)

# ---------------------------------------------------------------- main
def main(argv: List[str]) -> int:
    out_path = argv[1] if len(argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "otlp_bytes.svg")
    svg = render_svg(RECORDS)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(svg)
    print(f"wrote {out_path}  ({len(svg)} bytes)")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
