#!/usr/bin/env python3
"""Anatomy slide for ``OtapPdata`` -- the in-flight value flowing through
every node in the pipeline.

One slide that spells out, in source-faithful detail, what travels on
the pdata edges drawn by every per-node slide:

    OtapPdata
      |--- context: Context
      |       |--- stack: Vec<Frame>      <-- the per-subscriber frames
      |       |--- transport_headers: Option<TransportHeaders>
      |       '--- flow_compute_ns: Option<NonZeroU64>
      '--- payload: OtapPayload
              |--- OtapArrowRecords(...)  <-- OTAP variant (blue)
              '--- OtlpBytes(...)          <-- OTLP variant (red)

A single ``Frame`` from the middle of the stack is "exploded" out to
the right with a dotted leader, showing each field at its source-code
identifier and Rust type:

    Frame {
        node_id:  usize           // return address / source node
        interests: Interests      // 8 bitflag bits
        route: RouteData {
            calldata:          CallData (= SmallVec<[Context8u8; 3]>)
            entry_time_ns:     u64
            output_port_index: u16
        }
    }

The 8 ``Interests`` bits are drawn as a row of mini boxes so the
viewer can see exactly which bits are defined and which are set in
this particular frame.

Source of truth (cited file:line so the picture stays honest):
- ``OtapPdata`` -- crates/otap/src/pdata.rs:402
- ``Context``   -- crates/otap/src/pdata.rs:41
- ``Frame``     -- crates/engine/src/control.rs:138
- ``RouteData`` -- crates/engine/src/control.rs:102
- ``CallData = SmallVec<[Context8u8; 3]>`` -- crates/engine/src/control.rs:77
- ``Interests`` (8 bitflags) -- crates/engine/src/lib.rs:282
- ``OtapPayload`` -- crates/pdata/src/payload.rs:104

Visual grammar follows ``node_lib`` -- same palette, title bar, notes
box, arrow markers as the per-node slides.
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
    notes_box,
    _esc,
)
from gen_diagram import COLOR_BG, COLOR_LABEL, COLOR_SUBLABEL, FONT, FONT_MONO


# --------------------------------------------------------------- layout

PAGE_W = SLIDE_PAGE_W
PAGE_H = SLIDE_PAGE_H

TITLE_X = SLIDE_MARGIN_X
TITLE_Y = 60
SUBTITLE_Y = 90

# Outer OtapPdata box (left half).
OUTER_X = SLIDE_MARGIN_X
OUTER_Y = 130
OUTER_W = 720
OUTER_H = 510

# Context panel (top half of outer box).
CTX_PAD = 22
CTX_X   = OUTER_X + CTX_PAD
CTX_Y   = OUTER_Y + 56
CTX_W   = OUTER_W - 2 * CTX_PAD
CTX_H   = 360

# Stack: bottom-anchored, frames stacked upward.
STACK_X      = CTX_X + 22
STACK_W      = 380
FRAME_H      = 50
STACK_GAP    = 8
STACK_BOTTOM = CTX_Y + CTX_H - 110

# Payload panel (right half; outside the OtapPdata outer box).
PAYLOAD_X = OUTER_X + OUTER_W + 28
PAYLOAD_Y = OUTER_Y
PAYLOAD_W = PAGE_W - SLIDE_MARGIN_X - PAYLOAD_X
PAYLOAD_H = 220

# Exploded Frame panel (lower right, under payload).
FRAME_PANEL_X = PAYLOAD_X
FRAME_PANEL_Y = PAYLOAD_Y + PAYLOAD_H + 18
FRAME_PANEL_W = PAYLOAD_W
FRAME_PANEL_H = OUTER_Y + OUTER_H - FRAME_PANEL_Y

# Notes box: full width, below everything.
NOTES_X = SLIDE_MARGIN_X
NOTES_Y = OUTER_Y + OUTER_H + 18
NOTES_W = PAGE_W - 2 * SLIDE_MARGIN_X
NOTES_H = PAGE_H - NOTES_Y - 20


# Interests bitflag definitions, ordered by bit index (0..7).
# The third tuple entry is whether the bit is *set* in the example
# frame we are exploding -- caller-visible, so the picture is
# concrete instead of abstract.
INTERESTS_BITS: List[Tuple[str, str, bool]] = [
    ("ACKS",             "1<<0", True),
    ("NACKS",            "1<<1", True),
    ("RETURN_DATA",      "1<<2", False),
    ("ENTRY_TIMESTAMP",  "1<<3", True),
    ("CONSUMER_METRICS", "1<<4", False),
    ("PRODUCER_METRICS", "1<<5", True),
    ("SOURCE_TAGGING",   "1<<6", False),
    ("PROCESS_DURATION", "1<<7", True),
]


# ---------------------------------------------------------- primitives

def _outer_box(out: List[str]) -> None:
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
        f'<text x="{OUTER_X + 22}" y="{OUTER_Y + 34}" '
        f'font-size="{FS_NODE}" font-weight="700" '
        f'fill="{COLOR_LABEL}">OtapPdata</text>'
    )
    out.append(
        f'<text x="{OUTER_X + OUTER_W - 16}" y="{OUTER_Y + 30}" '
        f'text-anchor="end" font-size="{FS_LABEL}" '
        f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
        f'crates/otap/src/pdata.rs:402</text>'
    )
    out.append(
        f'<text x="{OUTER_X + 22}" y="{OUTER_Y + 52}" '
        f'font-size="{FS_NODE_SUB}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'two fields: a Context (left) and a payload (right)'
        f'</text>'
    )


def _context_panel(out: List[str]) -> None:
    out.append(
        f'<rect x="{CTX_X}" y="{CTX_Y}" width="{CTX_W}" height="{CTX_H}" '
        f'rx="10" ry="10" fill="#f4f6f8" '
        f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>'
    )
    out.append(
        f'<text x="{CTX_X + 14}" y="{CTX_Y + 22}" '
        f'font-size="{FS_NODE_SUB + 2}" font-weight="700" '
        f'fill="{COLOR_LABEL}">context: Context</text>'
    )
    out.append(
        f'<text x="{CTX_X + CTX_W - 14}" y="{CTX_Y + 22}" '
        f'text-anchor="end" font-size="{FS_TINY - 2}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_SUBLABEL}">crates/otap/src/pdata.rs:41</text>'
    )

    # transport_headers and flow_compute_ns chips along the bottom.
    chip_y = CTX_Y + CTX_H - 96
    chip_h = 40
    chip_w = (CTX_W - 22 - 22 - 12) / 2
    chips = [
        ("transport_headers", "Option<TransportHeaders>"),
        ("flow_compute_ns",   "Option<NonZeroU64>"),
    ]
    cx0 = CTX_X + 22
    for i, (label, sub) in enumerate(chips):
        x = cx0 + i * (chip_w + 12)
        out.append(
            f'<rect x="{x}" y="{chip_y}" width="{chip_w}" height="{chip_h}" '
            f'rx="5" ry="5" fill="white" stroke="{COLOR_CTRL_SOFT}" '
            f'stroke-width="1"/>'
        )
        out.append(
            f'<text x="{x + 8}" y="{chip_y + 18}" '
            f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
            f'font-weight="700" fill="{COLOR_LABEL}">{_esc(label)}</text>'
        )
        out.append(
            f'<text x="{x + 8}" y="{chip_y + 34}" '
            f'font-size="{FS_TINY - 2}" '
            f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
            f'{_esc(sub)}</text>'
        )

    # Stack heading right above the frames; subtitle on its own line
    # below to avoid colliding with long heading widths.
    stack_top_label_y = CTX_Y + 50
    out.append(
        f'<text x="{STACK_X}" y="{stack_top_label_y}" '
        f'font-size="{FS_LABEL}" font-weight="700" '
        f'fill="{COLOR_LABEL}">stack: Vec&lt;Frame&gt;</text>'
    )
    out.append(
        f'<text x="{STACK_X}" y="{stack_top_label_y + 18}" '
        f'font-size="{FS_TINY - 2}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'pushed by subscribe_to() \u00b7 popped on Ack/Nack'
        f'</text>'
    )


def _frame_tile(x: float, y: float, w: float, h: float,
                node_label: str, interest_summary: str,
                exploded: bool = False,
                top_marker: bool = False) -> str:
    """One stacked frame in the context stack.

    Two-line content: title 'Frame' on top, node_label + interest
    summary on their own lines so they never collide regardless of
    how long the labels get.
    """
    accent = COLOR_OTAP if exploded else COLOR_CTRL
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" ry="6" '
        f'fill="white" stroke="{accent}" stroke-width="{W_FRAME}"/>',
        f'<rect x="{x}" y="{y}" width="6" height="{h}" rx="3" ry="3" '
        f'fill="{accent}"/>',
        f'<text x="{x + 16}" y="{y + 20}" font-size="{FS_TINY}" '
        f'font-family="{FONT_MONO}" font-weight="700" '
        f'fill="{COLOR_LABEL}">Frame</text>',
        f'<text x="{x + 70}" y="{y + 20}" font-size="{FS_TINY}" '
        f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
        f'{_esc(node_label)}</text>',
        f'<text x="{x + 16}" y="{y + h - 10}" font-size="{FS_TINY - 2}" '
        f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
        f'{_esc(interest_summary)}</text>',
    ]
    if top_marker:
        parts.append(
            f'<text x="{x + w + 18}" y="{y + h/2 + 5}" '
            f'font-size="{FS_TINY - 2}" font-style="italic" '
            f'fill="{COLOR_OTAP}">'
            f'\u2190 top of stack'
            f'</text>'
        )
    return "".join(parts)


def _stack(out: List[str]) -> Tuple[float, float, float, float]:
    """Draw the context-stack frames bottom-up. Returns the bbox of the
    exploded frame so the leader line knows where to start."""
    # Stack contents: bottom (oldest pusher) to top (most recent).
    # The middle frame is the one we explode.
    frames = [
        ("node_id=7  otlp_grpc_exporter",
         "Ack|Nack|Producer"),
        ("node_id=4  batch_processor",
         "Ack|Nack|EntryTs|Producer|ProcDur"),
        ("node_id=1  otlp_receiver",
         "Ack|Nack|Return"),
    ]
    # Bottom-up: index 0 is bottom, index -1 is top.
    n = len(frames)
    exploded_idx = 1   # middle frame
    exploded_box = (0.0, 0.0, 0.0, 0.0)
    # First frame y (bottom one).
    for i, (node_label, summary) in enumerate(frames):
        # i=0 -> bottom. y grows upward.
        y = STACK_BOTTOM - (i + 1) * FRAME_H - i * STACK_GAP
        is_top = (i == n - 1)
        out.append(_frame_tile(
            STACK_X, y, STACK_W, FRAME_H,
            node_label=node_label,
            interest_summary=summary,
            exploded=(i == exploded_idx),
            top_marker=is_top,
        ))
        if i == exploded_idx:
            exploded_box = (STACK_X, y, STACK_W, FRAME_H)

    # Stack-base spacer: small italic note placed *just under* the
    # bottom-most frame, visible because we left ~30 px of room before
    # the bottom chips.
    base_y = STACK_BOTTOM + 14
    out.append(
        f'<text x="{STACK_X}" y="{base_y}" font-size="{FS_TINY}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'\u2191 stack grows upward as nodes call subscribe_to()'
        f'</text>'
    )

    return exploded_box


def _payload_panel(out: List[str]) -> None:
    out.append(
        f'<rect x="{PAYLOAD_X}" y="{PAYLOAD_Y}" width="{PAYLOAD_W}" '
        f'height="{PAYLOAD_H}" rx="14" ry="14" fill="white" '
        f'stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>'
    )
    # Two-tone top stripe: half OTAP-blue, half OTLP-red, to make the
    # tagged-enum nature of OtapPayload visible at a glance.
    half = PAYLOAD_W / 2
    out.append(
        f'<rect x="{PAYLOAD_X}" y="{PAYLOAD_Y}" width="{half}" '
        f'height="6" fill="{COLOR_OTAP}"/>'
    )
    out.append(
        f'<rect x="{PAYLOAD_X + half}" y="{PAYLOAD_Y}" width="{half}" '
        f'height="6" fill="{COLOR_OTLP}"/>'
    )
    out.append(
        f'<text x="{PAYLOAD_X + 22}" y="{PAYLOAD_Y + 34}" '
        f'font-size="{FS_NODE}" font-weight="700" '
        f'fill="{COLOR_LABEL}">payload: OtapPayload</text>'
    )
    out.append(
        f'<text x="{PAYLOAD_X + PAYLOAD_W - 14}" y="{PAYLOAD_Y + 30}" '
        f'text-anchor="end" font-size="{FS_TINY}" '
        f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
        f'crates/pdata/src/payload.rs:104</text>'
    )
    out.append(
        f'<text x="{PAYLOAD_X + 22}" y="{PAYLOAD_Y + 56}" '
        f'font-size="{FS_NODE_SUB}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'tagged enum: exactly one variant present at a time'
        f'</text>'
    )

    # Two variant chips, side by side.
    chip_y = PAYLOAD_Y + 82
    chip_h = PAYLOAD_H - (chip_y - PAYLOAD_Y) - 20
    gap = 16
    chip_w = (PAYLOAD_W - 2 * 22 - gap) / 2
    variants = [
        ("OtapArrowRecords", "Arrow RecordBatches", COLOR_OTAP, "OTAP"),
        ("OtlpBytes",        "OtlpProtoBytes",      COLOR_OTLP, "OTLP"),
    ]
    for i, (variant, sub, color, chip_label) in enumerate(variants):
        x = PAYLOAD_X + 22 + i * (chip_w + gap)
        out.append(
            f'<rect x="{x}" y="{chip_y}" width="{chip_w}" height="{chip_h}" '
            f'rx="8" ry="8" fill="white" stroke="{color}" '
            f'stroke-width="{W_FRAME}"/>'
        )
        out.append(
            f'<rect x="{x}" y="{chip_y}" width="{chip_w}" height="6" '
            f'rx="3" ry="3" fill="{color}"/>'
        )
        # Inverse-white format chip in the corner (matches per-node port
        # chips so the viewer sees the link).
        cw, ch = 56, 22
        out.append(
            f'<rect x="{x + chip_w - cw - 8}" y="{chip_y + 14}" '
            f'width="{cw}" height="{ch}" rx="5" ry="5" '
            f'fill="{color}" stroke="{color}"/>'
        )
        out.append(
            f'<text x="{x + chip_w - cw/2 - 8}" y="{chip_y + 14 + ch/2 + 5}" '
            f'text-anchor="middle" font-size="{FS_TINY}" font-weight="700" '
            f'font-family="{FONT_MONO}" fill="white">{chip_label}</text>'
        )
        out.append(
            f'<text x="{x + 16}" y="{chip_y + 36}" '
            f'font-size="{FS_NODE_SUB}" font-weight="700" '
            f'fill="{COLOR_LABEL}">{_esc(variant)}</text>'
        )
        out.append(
            f'<text x="{x + 16}" y="{chip_y + 56}" '
            f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
            f'fill="{COLOR_SUBLABEL}">{_esc(sub)}</text>'
        )
        # Bottom caption inside the chip.
        cap = (
            "columnar; Arrow IPC ready"
            if variant == "OtapArrowRecords"
            else "row-oriented; protobuf wire bytes"
        )
        out.append(
            f'<text x="{x + chip_w/2}" y="{chip_y + chip_h - 12}" '
            f'text-anchor="middle" font-size="{FS_TINY}" '
            f'font-style="italic" fill="{COLOR_SUBLABEL}">'
            f'{_esc(cap)}</text>'
        )


def _interests_row(out: List[str], x: float, y: float,
                   bits: List[Tuple[str, str, bool]]) -> float:
    """Draw the 8 Interests bits as a row of mini boxes.

    Returns the y-coordinate just below the row (for the caller to
    place the next line of fields under it).
    """
    box = 18
    gap = 4
    for i, (name, _bit, set_) in enumerate(bits):
        bx = x + i * (box + gap)
        fill = COLOR_OTAP if set_ else "white"
        out.append(
            f'<rect x="{bx}" y="{y}" width="{box}" height="{box}" '
            f'rx="3" ry="3" fill="{fill}" stroke="{COLOR_CTRL}" '
            f'stroke-width="1"/>'
        )
        if set_:
            out.append(
                f'<text x="{bx + box/2}" y="{y + box/2 + 4}" '
                f'text-anchor="middle" font-size="{FS_TINY - 2}" '
                f'font-weight="700" fill="white">1</text>'
            )
    # Labels under each box, rotated for readability.
    for i, (name, _bit, _set) in enumerate(bits):
        bx = x + i * (box + gap) + box / 2
        ly = y + box + 14
        out.append(
            f'<text x="{bx}" y="{ly}" font-size="{FS_TINY - 3}" '
            f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}" '
            f'transform="rotate(-35,{bx},{ly})">{_esc(name)}</text>'
        )
    return y + box + 80


def _frame_panel(out: List[str], exploded_box: Tuple[float, float, float, float]) -> None:
    """The exploded view of one Frame, lower-right."""
    out.append(
        f'<rect x="{FRAME_PANEL_X}" y="{FRAME_PANEL_Y}" '
        f'width="{FRAME_PANEL_W}" height="{FRAME_PANEL_H}" '
        f'rx="14" ry="14" fill="white" stroke="{COLOR_OTAP}" '
        f'stroke-width="{W_FRAME}"/>'
    )
    out.append(
        f'<rect x="{FRAME_PANEL_X}" y="{FRAME_PANEL_Y}" '
        f'width="{FRAME_PANEL_W}" height="6" rx="3" ry="3" '
        f'fill="{COLOR_OTAP}"/>'
    )
    out.append(
        f'<text x="{FRAME_PANEL_X + 22}" y="{FRAME_PANEL_Y + 32}" '
        f'font-size="{FS_NODE_SUB + 2}" font-weight="700" '
        f'fill="{COLOR_LABEL}">Frame (exploded)</text>'
    )
    out.append(
        f'<text x="{FRAME_PANEL_X + FRAME_PANEL_W - 14}" '
        f'y="{FRAME_PANEL_Y + 28}" text-anchor="end" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_SUBLABEL}">crates/engine/src/control.rs:138</text>'
    )

    # Field rows.
    px = FRAME_PANEL_X + 22
    py = FRAME_PANEL_Y + 56

    # node_id row.
    out.append(
        f'<text x="{px}" y="{py}" font-size="{FS_LABEL}" '
        f'font-family="{FONT_MONO}" font-weight="700" '
        f'fill="{COLOR_LABEL}">node_id: usize</text>'
    )
    out.append(
        f'<text x="{px + 240}" y="{py}" font-size="{FS_TINY}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'= 4 (return address \u00b7 source node)</text>'
    )
    py += 26

    # interests row + bitflag boxes.
    out.append(
        f'<text x="{px}" y="{py}" font-size="{FS_LABEL}" '
        f'font-family="{FONT_MONO}" font-weight="700" '
        f'fill="{COLOR_LABEL}">interests: Interests</text>'
    )
    out.append(
        f'<text x="{px + 240}" y="{py}" font-size="{FS_TINY}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'8-bit flags (filled = set in this frame)</text>'
    )
    py = _interests_row(out, px, py + 8, INTERESTS_BITS)

    # route nested struct.
    out.append(
        f'<text x="{px}" y="{py}" font-size="{FS_LABEL}" '
        f'font-family="{FONT_MONO}" font-weight="700" '
        f'fill="{COLOR_LABEL}">route: RouteData</text>'
    )
    out.append(
        f'<text x="{px + 240}" y="{py}" font-size="{FS_TINY}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'crates/engine/src/control.rs:102</text>'
    )
    py += 24
    nested = [
        ("    calldata: CallData",
         "= SmallVec<[Context8u8; 3]>  (per-frame opaque data)"),
        ("    entry_time_ns: u64",
         "monotonic ns at subscribe_to() if ENTRY_TIMESTAMP set"),
        ("    output_port_index: u16",
         "producer's output port for this frame"),
    ]
    for name, sub in nested:
        out.append(
            f'<text x="{px}" y="{py}" font-size="{FS_TINY + 1}" '
            f'font-family="{FONT_MONO}" fill="{COLOR_LABEL}">'
            f'{_esc(name)}</text>'
        )
        out.append(
            f'<text x="{px + 260}" y="{py}" font-size="{FS_TINY}" '
            f'font-style="italic" fill="{COLOR_SUBLABEL}">'
            f'{_esc(sub)}</text>'
        )
        py += 20


def _leader(out: List[str], from_box: Tuple[float, float, float, float]) -> None:
    """Dotted leader from the exploded frame in the stack to the
    frame-explosion panel on the lower right."""
    fx, fy, fw, fh = from_box
    x1 = fx + fw
    y1 = fy + fh / 2
    x2 = FRAME_PANEL_X
    y2 = FRAME_PANEL_Y + 16
    out.append(
        f'<path d="M{x1},{y1} L{x1 + 30},{y1} L{x2 - 30},{y2} L{x2},{y2}" '
        f'fill="none" stroke="{COLOR_OTAP}" stroke-width="1.4" '
        f'stroke-dasharray="3,3"/>'
    )
    out.append(
        f'<circle cx="{x1}" cy="{y1}" r="3" fill="{COLOR_OTAP}"/>'
    )


def _subtitle(out: List[str]) -> None:
    out.append(
        f'<text x="{TITLE_X}" y="{SUBTITLE_Y}" font-size="{FS_SUBTITLE}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'Anatomy of the in-flight value carried on every pdata edge: '
        f'a Context (unwind path) plus a payload (telemetry).'
        f'</text>'
    )


def _notes(out: List[str]) -> None:
    notes = [
        "Each node that calls effect_handler.subscribe_to(...) pushes one Frame; node_id is the unwind return address.",
        "drain_to_next_subscriber walks the stack top-down on the return path and stops at the first matching frame.",
        "If the same node subscribes twice, the top frame is updated in place: interests merge, calldata replaces.",
        "transport_headers and flow_compute_ns sit on Context but are not part of any Frame; only headers survive topic hops.",
        "OtapPayload variants are mutually exclusive; calldata is at most three 8-byte slots, larger state rides in payload.",
    ]
    out.append(notes_box(NOTES_X, NOTES_Y, NOTES_W, NOTES_H, notes))


def render() -> str:
    out: List[str] = []
    out.append(page_open(PAGE_W, PAGE_H))
    out.append(arrow_marker_defs())
    out.append(title_bar(
        TITLE_X, TITLE_Y, PAGE_W - 2 * SLIDE_MARGIN_X,
        title="OtapPdata anatomy",
        urn="Context (stack of Frames) + payload (OTAP | OTLP)",
        accent=COLOR_OTAP,
    ))
    _subtitle(out)
    _outer_box(out)
    _context_panel(out)
    exploded_box = _stack(out)
    _payload_panel(out)
    _frame_panel(out, exploded_box)
    _leader(out, exploded_box)
    _notes(out)
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
