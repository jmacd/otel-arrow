#!/usr/bin/env python3
"""Anatomy slide for ``OtapPdata`` -- the in-flight value flowing through
every node in the pipeline.

Single OtapPdata box contains both halves: a Context (stack of Frames
plus two scalar fields) on the left, and the payload tagged enum on
the right. Both payload variants are reference-counted, so handing a
PData from one node to the next is zero-copy.

A small Frame anatomy panel sits below the main box, listing the
fields of one Frame at their source-code identifiers.

Source of truth:
- ``OtapPdata``   -- crates/otap/src/pdata.rs:402
- ``Context``     -- crates/otap/src/pdata.rs:41
- ``Frame``       -- crates/engine/src/control.rs:138
- ``RouteData``   -- crates/engine/src/control.rs:102
- ``CallData``    -- crates/engine/src/control.rs:77
- ``Interests``   -- crates/engine/src/lib.rs:282
- ``OtapPayload`` -- crates/pdata/src/payload.rs:104
- ``OtlpProtoBytes`` wraps ``bytes::Bytes`` -- crates/pdata/src/otlp/mod.rs:40
- ``OtapArrowRecords`` holds Arc-shared Arrow RecordBatches.
"""

from __future__ import annotations
import sys
from typing import List, Tuple

from node_lib import (
    SLIDE_PAGE_W, SLIDE_PAGE_H, SLIDE_MARGIN_X, SLIDE_MARGIN_Y,
    COLOR_OTAP, COLOR_OTLP, COLOR_CTRL, COLOR_CTRL_SOFT, COLOR_CTX,
    FS_TITLE, FS_SUBTITLE, FS_NODE, FS_NODE_SUB, FS_LABEL, FS_TINY,
    W_PDATA, W_CTRL, W_FRAME,
    page_open, page_close, title_bar, arrow_marker_defs,
    _esc,
)
from gen_diagram import COLOR_BG, COLOR_LABEL, COLOR_SUBLABEL, FONT, FONT_MONO


# --------------------------------------------------------------- layout

PAGE_W = SLIDE_PAGE_W
PAGE_H = SLIDE_PAGE_H

TITLE_X = SLIDE_MARGIN_X
TITLE_Y = 60
SUBTITLE_Y = 90

# One big OtapPdata box that holds both Context and Payload.
OUTER_X = SLIDE_MARGIN_X
OUTER_Y = 130
OUTER_W = PAGE_W - 2 * SLIDE_MARGIN_X
OUTER_H = 510

INNER_PAD = 24
HEADER_H  = 56                      # outer box title bar + subtitle space
INNER_TOP = OUTER_Y + HEADER_H

COL_GAP   = 32
COL_W     = (OUTER_W - 2 * INNER_PAD - COL_GAP) / 2
CTX_X     = OUTER_X + INNER_PAD
PAY_X     = CTX_X + COL_W + COL_GAP
COL_TOP   = INNER_TOP + 12
COL_H     = OUTER_Y + OUTER_H - COL_TOP - INNER_PAD

# Frame stack inside the context column.
FRAME_W   = COL_W - 32
FRAME_H   = 78
STACK_X   = CTX_X + 16
STACK_TOP_Y = COL_TOP + 36

# Two scalar context chips along the bottom of the context column.
CHIP_H = 32
CHIPS_Y = COL_TOP + COL_H - CHIP_H - 4

# Payload variants, two large boxes stacked vertically with an XOR badge.
VAR_X = PAY_X + 16
VAR_W = COL_W - 32
XOR_H = 28
VAR_H = (COL_H - 36 - XOR_H - 16) / 2     # 36 hdr + XOR badge + paddings

# Frame anatomy strip below the OtapPdata box.
ANATOMY_Y = OUTER_Y + OUTER_H + 26
ANATOMY_X = SLIDE_MARGIN_X
ANATOMY_W = OUTER_W
ANATOMY_H = PAGE_H - ANATOMY_Y - SLIDE_MARGIN_Y


# ---------------------------------------------------------- primitives

def _outer(out: List[str]) -> None:
    out.append(
        f'<rect x="{OUTER_X}" y="{OUTER_Y}" width="{OUTER_W}" '
        f'height="{OUTER_H}" rx="14" ry="14" fill="white" '
        f'stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>'
    )
    out.append(
        f'<rect x="{OUTER_X}" y="{OUTER_Y}" width="{OUTER_W}" '
        f'height="6" rx="3" ry="3" fill="{COLOR_OTAP}"/>'
    )
    out.append(
        f'<text x="{OUTER_X + 22}" y="{OUTER_Y + 36}" '
        f'font-size="{FS_NODE}" font-weight="700" '
        f'fill="{COLOR_LABEL}">OtapPdata</text>'
    )
    # Thin vertical divider down the middle separates context (left)
    # from payload (right) without needing column headings.
    div_x = (CTX_X + COL_W + PAY_X) / 2
    out.append(
        f'<line x1="{div_x}" y1="{INNER_TOP - 4}" '
        f'x2="{div_x}" y2="{OUTER_Y + OUTER_H - INNER_PAD/2}" '
        f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>'
    )


def _frame_tile(x: float, y: float, w: float, h: float,
                position: str,
                fields: List[Tuple[str, str]],
                accent: str = COLOR_CTRL) -> str:
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" ry="6" '
        f'fill="white" stroke="{accent}" stroke-width="{W_FRAME}"/>',
        f'<rect x="{x}" y="{y}" width="6" height="{h}" rx="3" ry="3" '
        f'fill="{accent}"/>',
        f'<text x="{x + 18}" y="{y + 22}" font-size="{FS_LABEL}" '
        f'font-family="{FONT_MONO}" font-weight="700" '
        f'fill="{COLOR_LABEL}">Frame</text>',
        f'<text x="{x + w - 14}" y="{y + 22}" text-anchor="end" '
        f'font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">{_esc(position)}</text>',
    ]
    # Per-frame example field rows (mono).
    fy = y + 44
    for name, val in fields:
        parts.append(
            f'<text x="{x + 18}" y="{fy}" font-size="{FS_TINY}" '
            f'font-family="{FONT_MONO}" font-weight="700" '
            f'fill="{COLOR_LABEL}">{_esc(name)}</text>'
        )
        parts.append(
            f'<text x="{x + 110}" y="{fy}" font-size="{FS_TINY}" '
            f'font-family="{FONT_MONO}" '
            f'fill="{COLOR_SUBLABEL}">{_esc(val)}</text>'
        )
        fy += 18
    return "".join(parts)


def _vertical_dots(cx: float, cy: float, color: str, n: int = 3,
                   r: float = 2.4, gap: float = 8.0) -> str:
    """Three small circles stacked vertically, geometrically centered
    on (cx, cy) -- replaces the unicode ``\u22ee`` glyph whose
    baseline anchor is unreliable across renderers.
    """
    total = (n - 1) * gap
    y0 = cy - total / 2
    parts = []
    for i in range(n):
        parts.append(
            f'<circle cx="{cx}" cy="{y0 + i * gap}" r="{r}" '
            f'fill="{color}"/>'
        )
    return "".join(parts)


def _stack(out: List[str]) -> None:
    # Top frame -- the next-popped frame on Ack/Nack, painted in the
    # OTAP accent color so the eye lands on it first.
    top_y = STACK_TOP_Y
    out.append(_frame_tile(
        STACK_X, top_y, FRAME_W, FRAME_H,
        position="top",
        accent=COLOR_OTAP,
        fields=[
            ("node_id",   "= 7"),
            ("interests", "Ack | Nack | Producer"),
        ],
    ))
    # Three small circles, geometrically centered in the 60-px gap
    # between the two frames -- replaces the unicode \u22ee glyph.
    gap_y_center = top_y + FRAME_H + 30
    out.append(_vertical_dots(
        STACK_X + FRAME_W / 2, gap_y_center, COLOR_SUBLABEL,
    ))
    # Bottom frame.
    bot_y = top_y + FRAME_H + 60
    out.append(_frame_tile(
        STACK_X, bot_y, FRAME_W, FRAME_H,
        position="bottom",
        fields=[
            ("node_id",   "= 1"),
            ("interests", "Ack | Nack | Return"),
        ],
    ))


def _scalar_chip(x: float, y: float, w: float, h: float,
                 name: str, type_: str) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" ry="6" '
        f'fill="#f4f6f8" stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>'
        f'<text x="{x + 12}" y="{y + h/2 + 5}" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'font-weight="700" fill="{COLOR_LABEL}">{_esc(name)}</text>'
        f'<text x="{x + w - 12}" y="{y + h/2 + 5}" text-anchor="end" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_SUBLABEL}">{_esc(type_)}</text>'
    )


def _scalars(out: List[str]) -> None:
    # Two slim chips along the bottom of the context column, stacked
    # vertically (no parentheticals, no descriptions).
    chip_w = FRAME_W
    out.append(_scalar_chip(STACK_X, CHIPS_Y - CHIP_H - 8,
                            chip_w, CHIP_H,
                            "transport_headers",
                            "Option<TransportHeaders>"))
    out.append(_scalar_chip(STACK_X, CHIPS_Y, chip_w, CHIP_H,
                            "flow_compute_ns",
                            "Option<NonZeroU64>"))


def _variant(x: float, y: float, w: float, h: float,
             name: str, format_label: str, accent: str,
             body_label: str) -> str:
    """A large variant chip with the inverse-white format chip in the
    upper-right and the format-name in big bold sans below.
    """
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" ry="10" '
        f'fill="white" stroke="{accent}" stroke-width="{W_FRAME}"/>',
        f'<rect x="{x}" y="{y}" width="{w}" height="6" rx="3" ry="3" '
        f'fill="{accent}"/>',
        # Variant identifier (mono).
        f'<text x="{x + 22}" y="{y + 32}" font-size="{FS_NODE_SUB}" '
        f'font-family="{FONT_MONO}" font-weight="700" '
        f'fill="{COLOR_LABEL}">{_esc(name)}</text>',
        # Big body label (the thing the slide wants the eye to land on).
        f'<text x="{x + 22}" y="{y + h/2 + 18}" '
        f'font-size="{FS_NODE}" font-weight="700" fill="{accent}">'
        f'{_esc(body_label)}</text>',
        # Tag-line: reference-counted, zero-copy.
        f'<text x="{x + 22}" y="{y + h - 18}" '
        f'font-size="{FS_LABEL}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'reference-counted \u00b7 immutable'
        f'</text>',
    ]
    # Inverse-white format chip in upper-right.
    cw, ch = 64, 24
    cx = x + w - cw - 16
    cy = y + 18
    parts.append(
        f'<rect x="{cx}" y="{cy}" width="{cw}" height="{ch}" '
        f'rx="5" ry="5" fill="{accent}" stroke="{accent}"/>'
    )
    parts.append(
        f'<text x="{cx + cw/2}" y="{cy + ch/2 + 5}" text-anchor="middle" '
        f'font-size="{FS_TINY}" font-weight="700" '
        f'font-family="{FONT_MONO}" fill="white">{format_label}</text>'
    )
    return "".join(parts)


def _xor_badge(out: List[str], cx: float, cy: float) -> None:
    """Visual emphasis: 'one of' badge between the two payload variant
    tiles, making it explicit that the payload field is exactly one of
    the two -- never both, never neither.
    """
    w, h = 96, 28
    x = cx - w / 2
    y = cy - h / 2
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" ry="14" '
        f'fill="white" stroke="{COLOR_LABEL}" stroke-width="1.4"/>'
    )
    out.append(
        f'<text x="{cx}" y="{cy + 5}" text-anchor="middle" '
        f'font-size="{FS_LABEL}" font-weight="700" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">one of</text>'
    )
    # Short vertical leader lines into the badge from both tiles, so
    # the eye reads "this OR that".
    out.append(
        f'<line x1="{cx}" y1="{cy - h/2 - 8}" x2="{cx}" y2="{cy - h/2}" '
        f'stroke="{COLOR_LABEL}" stroke-width="1.4"/>'
    )
    out.append(
        f'<line x1="{cx}" y1="{cy + h/2}" x2="{cx}" y2="{cy + h/2 + 8}" '
        f'stroke="{COLOR_LABEL}" stroke-width="1.4"/>'
    )


def _payload(out: List[str]) -> None:
    top_y = STACK_TOP_Y
    out.append(_variant(VAR_X, top_y, VAR_W, VAR_H,
                        name="OtapArrowRecords",
                        format_label="OTAP",
                        accent=COLOR_OTAP,
                        body_label="OTAP records"))
    # XOR badge, vertically centered in the gap between variants.
    badge_cy = top_y + VAR_H + 8 + XOR_H / 2
    _xor_badge(out, VAR_X + VAR_W / 2, badge_cy)
    out.append(_variant(VAR_X, top_y + VAR_H + 8 + XOR_H + 8, VAR_W, VAR_H,
                        name="OtlpBytes",
                        format_label="OTLP",
                        accent=COLOR_OTLP,
                        body_label="OTLP bytes"))


# All eight Interests bitflag identifiers in declaration order.
INTERESTS_BITS: List[str] = [
    "ACKS",
    "NACKS",
    "RETURN_DATA",
    "ENTRY_TIMESTAMP",
    "CONSUMER_METRICS",
    "PRODUCER_METRICS",
    "SOURCE_TAGGING",
    "PROCESS_DURATION",
]


def _anatomy(out: List[str]) -> None:
    # Compact panel listing the full Frame layout: every field with
    # its source-code identifier and Rust type, plus the eight
    # Interests bits as small chips.
    out.append(
        f'<rect x="{ANATOMY_X}" y="{ANATOMY_Y}" '
        f'width="{ANATOMY_W}" height="{ANATOMY_H}" '
        f'rx="10" ry="10" fill="white" stroke="{COLOR_CTRL_SOFT}" '
        f'stroke-width="1"/>'
    )
    # Title.
    out.append(
        f'<text x="{ANATOMY_X + 22}" y="{ANATOMY_Y + 26}" '
        f'font-size="{FS_NODE_SUB}" font-weight="700" '
        f'font-family="{FONT_MONO}" fill="{COLOR_LABEL}">'
        f'Frame</text>'
    )
    out.append(
        f'<text x="{ANATOMY_X + 88}" y="{ANATOMY_Y + 26}" '
        f'font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}"></text>'
    )

    # Two columns of "name : type" rows.
    rows: List[Tuple[str, str]] = [
        ("node_id",                  "usize"),
        ("interests",                "Interests"),
        ("route.calldata",           "CallData = SmallVec<[Context8u8; 3]>"),
        ("route.entry_time_ns",      "u64"),
        ("route.output_port_index",  "u16"),
    ]
    col_x   = ANATOMY_X + 22
    type_x  = col_x + 240
    row_y0  = ANATOMY_Y + 52
    row_h   = 20
    for i, (name, type_) in enumerate(rows):
        ry = row_y0 + i * row_h
        out.append(
            f'<text x="{col_x}" y="{ry}" font-size="{FS_TINY}" '
            f'font-family="{FONT_MONO}" font-weight="700" '
            f'fill="{COLOR_LABEL}">{_esc(name)}</text>'
        )
        out.append(
            f'<text x="{type_x}" y="{ry}" font-size="{FS_TINY}" '
            f'font-family="{FONT_MONO}" '
            f'fill="{COLOR_SUBLABEL}">{_esc(type_)}</text>'
        )

    # Right side: the eight Interests bits as labelled chips, in two
    # rows of four so the long identifiers fit without overlapping.
    chip_w, chip_h = 134, 22
    chip_gap_x = 6
    chip_gap_y = 14         # between rows: room for the bit-number caption
    cols = 4
    bits_x = ANATOMY_X + ANATOMY_W - 22 - cols * chip_w \
             - (cols - 1) * chip_gap_x
    bits_y = ANATOMY_Y + 52
    out.append(
        f'<text x="{bits_x}" y="{bits_y - 16}" font-size="{FS_TINY}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'Interests</text>'
    )
    for i, name in enumerate(INTERESTS_BITS):
        row = i // cols
        col = i % cols
        cx = bits_x + col * (chip_w + chip_gap_x)
        cy = bits_y + row * (chip_h + chip_gap_y + 12)
        out.append(
            f'<rect x="{cx}" y="{cy}" width="{chip_w}" height="{chip_h}" '
            f'rx="4" ry="4" fill="white" stroke="{COLOR_CTRL}" '
            f'stroke-width="1"/>'
        )
        out.append(
            f'<text x="{cx + chip_w/2}" y="{cy + chip_h/2 + 4}" '
            f'text-anchor="middle" font-size="{FS_TINY - 2}" '
            f'font-family="{FONT_MONO}" fill="{COLOR_LABEL}">'
            f'{_esc(name)}</text>'
        )
        # Bit number under the chip.
        out.append(
            f'<text x="{cx + chip_w/2}" y="{cy + chip_h + 12}" '
            f'text-anchor="middle" font-size="{FS_TINY - 3}" '
            f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
            f'1&lt;&lt;{i}</text>'
        )


def _subtitle(out: List[str]) -> None:
    out.append(
        f'<text x="{TITLE_X}" y="{SUBTITLE_Y}" font-size="{FS_SUBTITLE}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'Pipeline data is payload and context, each carries its own callstack'
        f'</text>'
    )


def render() -> str:
    out: List[str] = []
    out.append(page_open(PAGE_W, PAGE_H))
    out.append(arrow_marker_defs())
    out.append(title_bar(
        TITLE_X, TITLE_Y, PAGE_W - 2 * SLIDE_MARGIN_X,
        title="Pipeline Data",
        urn="Context + payload",
        accent=COLOR_OTAP,
    ))
    _subtitle(out)
    _outer(out)
    _stack(out)
    _scalars(out)
    _payload(out)
    _anatomy(out)
    out.append(page_close())
    return "".join(out)


def main(argv: List[str]) -> int:
    out = argv[1] if len(argv) > 1 else "otap_pdata.svg"
    svg = render()
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
