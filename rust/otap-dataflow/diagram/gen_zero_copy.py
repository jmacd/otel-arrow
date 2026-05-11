#!/usr/bin/env python3
"""Zero-copy & Arrow IPC slide.

A single-scenario timing-style diagram that visualises the lifecycle of
one batch through a small pipeline that exercises both of the engine's
zero-copy paths:

1. **In-process zero-copy** between nodes -- pdata is reference-counted
   (`Arc<…>` for OTAP records, `bytes::Bytes` for OTLP), so handing a
   batch from one node to the next never copies the payload.
2. **Quiver-backed durable buffer** -- the durable_buffer processor
   serialises OTAP record batches to disk via Arrow IPC (the Quiver
   spool format) and reads them back as zero-copy ``OtapArrowRecords``
   on the way out.
3. **Arrow IPC on the wire** -- the OTAP exporter encodes OTAP record
   batches as Arrow IPC framed messages (with optional zstd) and the
   downstream OTAP receiver decodes them back into ``OtapArrowRecords``
   without re-allocating per-row.

Composition (top to bottom):

- Title + subtitle.
- Zone header bar (LOGGER / WIRE / COLLECTOR), same as
  ``experiments.svg``.
- A **node strip**: rounded boxes for each pipeline node, aligned
  horizontally to the timing-trace segments below. The wire band
  between LOGGER and COLLECTOR has no node box; it is a Arrow-IPC
  framed-bytes region only.
- A single **signal-level row**, drawn with the Texas-Instruments
  conventions from ``gen_diagram.py``:
  - LOW = OTLP, HIGH = OTAP, MID = serialised bytes (wire / disk),
  - sloped transitions = real conversion work.
- A small annotation strip below pinning the three zero-copy points
  with short callouts (``Arc / bytes::Bytes``, ``Quiver / Arrow IPC``,
  ``Arrow IPC + zstd``).

Source of truth (file:line) so the picture stays honest:

- ``OtapArrowRecords`` reference-counted Arrow batches --
  rust/otap-dataflow/crates/pdata/src/payload.rs:104.
- ``OtlpProtoBytes`` wraps ``bytes::Bytes`` --
  crates/pdata/src/otlp/mod.rs:40.
- Filter processor coerces inputs to OTAP --
  see ``gen_node_filter.py`` SPEC.
- Durable buffer Arrow-IPC spool format (Quiver) --
  see ``gen_node_durable_buffer.py`` SPEC notes; per-core durable
  storage produces *durable* receipts on Ack.
- OTAP exporter streams Arrow IPC + zstd --
  see ``gen_node_otap_exporter.py`` SPEC notes
  (``streams_per_signal``, double-compression default).
"""

from __future__ import annotations
import math
import sys
from typing import List, Optional, Tuple

# Reuse the palette and constants from the original timing diagram so
# this slide reads as part of the same deck.
from gen_diagram import (
    COLOR_BG, COLOR_TRACK, COLOR_LOW, COLOR_HIGH, COLOR_MID,
    COLOR_GRID, COLOR_LABEL, COLOR_SUBLABEL, COLOR_BOUNDARY,
    FONT, FONT_MONO, LINE_W, GRID_W, RAMP_ANGLE_DEG,
)
from node_lib import (
    page_open, page_close, title_bar,
    COLOR_OTAP,
    FS_SUBTITLE,
    SLIDE_PAGE_H,
)


# ----------------------------------------------------------------- layout

PAGE_W            = 1600
PAGE_MARGIN_X     = 80

TITLE_Y           = 60
SUBTITLE_Y        = 90

ZONE_HEADER_Y     = 110
ZONE_HEADER_H     = 0           # zone header removed

NODE_STRIP_Y      = 220         # moved down from the top
NODE_STRIP_H      = 70

# Timing-trace band geometry. Pushed into the lower third of the page so
# the wide gap above is available for annotations linking each node to
# its signal-level effect.
TRACK_TOP_PAD     = 300
TRACK_BAND        = 110
TRACK_BOT_PAD     = 24

ANNOTATION_H      = 98          # reserved bottom space; annotations TBD
PAGE_BOTTOM_PAD   = 20

# Track horizontal extent.
TRACK_LEFT_PAD    = 20
TRACK_RIGHT_PAD   = 20


# ------------------------------------------------------------------ model

# Each segment: (label, level, sublabel, weight). Levels are
# "low" (OTLP), "high" (OTAP), "mid" (serialised bytes).
SEGMENTS: List[Tuple[str, str, str, float]] = [
    ("otlp_receiver",           "low",  "", 1.4),
    ("batch",                   "low",  "", 0.9),
    ("filter",                  "high", "", 0.9),
    ("durable_buffer\n(spool)", "mid",  "", 1.1),
    ("durable_buffer\n(read)",  "high", "", 0.9),
    ("otap_exporter",           "high", "", 0.9),
    ("wire",                    "mid",  "", 1.1),
    ("otap_receiver",           "high", "", 1.4),
]

# Node strip groups: (node display name, role, [segment indices it covers]).
# Wire (segment index 6) is *not* a node -- it sits in the WIRE zone.
NODES: List[Tuple[str, str, List[int]]] = [
    ("otlp_receiver",  "receiver",  [0]),
    ("batch",          "processor", [1]),
    ("filter",         "processor", [2]),
    ("durable_buffer", "processor", [3, 4]),
    ("otap_exporter",  "exporter",  [5]),
    # gap (WIRE zone, segment 6) -- no node box here
    ("otap_receiver",  "receiver",  [7]),
]

# Zone partitioning -- which segment indices fall into each zone.
ZONES: List[Tuple[str, List[int], str]] = [
    ("LOGGER (otap-dataflow)",    [0, 1, 2, 3, 4, 5], "#eef4fb"),
    ("WIRE",                      [6],                "#f4eefb"),
    ("COLLECTOR (otap-dataflow)", [7],                "#fbf2ee"),
]


# --------------------------------------------------------------- helpers

def _esc(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _level_y(level: str, y_top: float, y_bot: float) -> float:
    if level == "high": return y_top
    if level == "low":  return y_bot
    return (y_top + y_bot) / 2     # mid


def _seg_color(level: str) -> str:
    return {"low": COLOR_LOW, "high": COLOR_HIGH, "mid": COLOR_MID}[level]


def _xs(track_x0: float, track_x1: float) -> List[float]:
    total_w = sum(w for *_x, w in SEGMENTS)
    track_w = track_x1 - track_x0
    xs = [track_x0]
    for *_x, w in SEGMENTS:
        xs.append(xs[-1] + w / total_w * track_w)
    return xs


# ------------------------------------------------------------------ chunks

def _zone_headers(out: List[str], xs: List[float]) -> None:
    for name, segs, fill in ZONES:
        x0 = xs[segs[0]]
        x1 = xs[segs[-1] + 1]
        out.append(
            f'<rect x="{x0}" y="{ZONE_HEADER_Y}" width="{x1 - x0}" '
            f'height="{ZONE_HEADER_H}" fill="{fill}" '
            f'stroke="{COLOR_BOUNDARY}" stroke-width="0.75"/>'
        )
        out.append(
            f'<text x="{(x0 + x1)/2}" y="{ZONE_HEADER_Y + 21}" '
            f'text-anchor="middle" font-family="{FONT}" font-size="15" '
            f'font-weight="700" fill="{COLOR_LABEL}" '
            f'letter-spacing="1.5">{_esc(name)}</text>'
        )


def _node_strip(out: List[str], xs: List[float]) -> None:
    """One rounded rectangle per node, aligned to the segments below."""
    for name, role, segs in NODES:
        x0 = xs[segs[0]]
        x1 = xs[segs[-1] + 1]
        # Tight horizontal padding so adjacent boxes don't touch.
        pad = 6
        bx, bw = x0 + pad, max(40.0, (x1 - x0) - 2 * pad)
        by = NODE_STRIP_Y
        bh = NODE_STRIP_H
        # Node frame
        out.append(
            f'<rect x="{bx}" y="{by}" width="{bw}" height="{bh}" '
            f'rx="8" ry="8" fill="white" stroke="{COLOR_TRACK}" '
            f'stroke-width="1.4"/>'
        )
        # Top accent stripe (neutral grey -- same convention as the
        # per-node slides: format is read from the trace below, never
        # from the box itself).
        out.append(
            f'<rect x="{bx}" y="{by}" width="{bw}" height="4" '
            f'rx="2" ry="2" fill="{COLOR_SUBLABEL}"/>'
        )
        # Name (bold mono) + role (italic).
        cx = (bx + bx + bw) / 2
        out.append(
            f'<text x="{cx}" y="{by + bh/2 + 5}" text-anchor="middle" '
            f'font-family="{FONT_MONO}" font-size="17" '
            f'font-weight="700" fill="{COLOR_LABEL}">{_esc(name)}</text>'
        )

    # Faint connecting arrows between adjacent nodes. They stop at the
    # WIRE gap and resume on the COLLECTOR side; the wire arrow itself
    # is drawn separately so it can be styled differently (dashed).
    arrow_y = NODE_STRIP_Y + NODE_STRIP_H / 2
    for i in range(len(NODES) - 1):
        _, _, segs_a = NODES[i]
        _, _, segs_b = NODES[i + 1]
        x_end_a = xs[segs_a[-1] + 1] - 6     # tail-side padding
        x_start_b = xs[segs_b[0]] + 6
        if segs_b[0] - segs_a[-1] == 1:
            # In-thread (same engine, same zone).
            out.append(
                f'<line x1="{x_end_a}" y1="{arrow_y}" '
                f'x2="{x_start_b}" y2="{arrow_y}" '
                f'stroke="{COLOR_TRACK}" stroke-width="1.4" '
                f'marker-end="url(#tip)"/>'
            )
        else:
            # Crosses the wire (gap of one or more segments).
            out.append(
                f'<line x1="{x_end_a}" y1="{arrow_y}" '
                f'x2="{x_start_b}" y2="{arrow_y}" '
                f'stroke="{COLOR_MID}" stroke-width="1.4" '
                f'stroke-dasharray="6,4" marker-end="url(#tip)"/>'
            )
            # Caption under the wire arrow.
            mx = (x_end_a + x_start_b) / 2
            out.append(
                f'<text x="{mx}" y="{arrow_y - 8}" text-anchor="middle" '
                f'font-family="{FONT_MONO}" font-size="13" '
                f'fill="{COLOR_SUBLABEL}">network</text>'
            )


def _level_legend(out: List[str], track_x0: float,
                  band_top: float, band_bot: float) -> None:
    legend_x = track_x0 - 10
    out.append(
        f'<text x="{legend_x}" y="{band_top + 5}" text-anchor="end" '
        f'font-family="{FONT}" font-size="16" font-weight="700" '
        f'fill="{COLOR_HIGH}">OTAP</text>'
    )
    out.append(
        f'<text x="{legend_x}" y="{band_bot + 5}" text-anchor="end" '
        f'font-family="{FONT}" font-size="16" font-weight="700" '
        f'fill="{COLOR_LOW}">OTLP</text>'
    )


def _grid_lines(out: List[str], track_x0: float, track_x1: float,
                band_top: float, band_bot: float, mid_y: float) -> None:
    out.append(
        f'<line x1="{track_x0}" y1="{band_top}" x2="{track_x1}" y2="{band_top}" '
        f'stroke="{COLOR_GRID}" stroke-width="{GRID_W}" stroke-dasharray="2,4"/>'
    )
    out.append(
        f'<line x1="{track_x0}" y1="{band_bot}" x2="{track_x1}" y2="{band_bot}" '
        f'stroke="{COLOR_GRID}" stroke-width="{GRID_W}" stroke-dasharray="2,4"/>'
    )
    out.append(
        f'<line x1="{track_x0}" y1="{mid_y}" x2="{track_x1}" y2="{mid_y}" '
        f'stroke="{COLOR_GRID}" stroke-width="{GRID_W}" stroke-dasharray="1,5"/>'
    )


def _trace(out: List[str], xs: List[float],
           band_top: float, band_bot: float) -> None:
    mid_y = (band_top + band_bot) / 2

    ys = [_level_y(level, band_top, band_bot)
          for _name, level, _sub, _w in SEGMENTS]

    tan_a = math.tan(math.radians(RAMP_ANGLE_DEG))
    def ramp_run(dy: float) -> float:
        return abs(dy) / tan_a

    n = len(SEGMENTS)
    half_run_left  = [0.0] * n
    half_run_right = [0.0] * n
    boundary_ramp: List[Optional[Tuple[float, float, float]]] = [None] * n
    for i in range(1, n):
        if ys[i - 1] == ys[i]:
            continue
        run = ramp_run(ys[i] - ys[i - 1])
        half = run / 2.0
        max_left  = (xs[i]     - xs[i - 1]) * 0.45
        max_right = (xs[i + 1] - xs[i])     * 0.45
        half = min(half, max_left, max_right)
        half_run_right[i - 1] = half
        half_run_left[i]      = half
        boundary_ramp[i] = (ys[i - 1], ys[i], half)

    for i, (label, level, sub, _w) in enumerate(SEGMENTS):
        x0, x1 = xs[i], xs[i + 1]
        color = _seg_color(level)
        y = ys[i]
        xa = x0 + half_run_left[i]
        xb = x1 - half_run_right[i]
        out.append(
            f'<line x1="{xa}" y1="{y}" x2="{xb}" y2="{y}" '
            f'stroke="{color}" stroke-width="{LINE_W}" '
            f'stroke-linecap="square"/>'
        )
        br = boundary_ramp[i]
        if br is not None:
            y_prev, y_next, half = br
            xL = xs[i] - half
            xR = xs[i] + half
            out.append(
                f'<line x1="{xL}" y1="{y_prev}" x2="{xR}" y2="{y_next}" '
                f'stroke="{COLOR_TRACK}" stroke-width="{LINE_W}" '
                f'stroke-linecap="round"/>'
            )

        # Faint vertical tick at every segment boundary (matches
        # gen_diagram.render_row) so the eye sees the segmentation.
        out.append(
            f'<line x1="{x0}" y1="{band_top - 6}" x2="{x0}" y2="{band_bot + 6}" '
            f'stroke="{COLOR_BOUNDARY}" stroke-width="0.75" '
            f'stroke-dasharray="2,3"/>'
        )

    # Closing tick at the right-most boundary.
    out.append(
        f'<line x1="{xs[-1]}" y1="{band_top - 6}" x2="{xs[-1]}" y2="{band_bot + 6}" '
        f'stroke="{COLOR_BOUNDARY}" stroke-width="0.75" stroke-dasharray="2,3"/>'
    )


def _annotations(out: List[str], xs: List[float],
                 band_bot: float) -> None:
    """Three short callouts beneath the trace, anchored to the segments
    they describe. Each callout has an arrow pointing up to its anchor
    x and a short label below."""
    callouts: List[Tuple[float, str, str]] = [
        # Anchor x, headline, sub.
        (((xs[3] + xs[5]) / 2),
         "Quiver \u00b7 Arrow IPC \u2192 disk \u2192 zero-copy",
         "OtapArrowRecords are Arc-shared; spool & restore without re-allocating rows"),
        (((xs[6] + xs[7]) / 2),
         "Arrow IPC + zstd on the wire",
         "OTAP gRPC streams; per-row payload never copied between Arrow buffers"),
        (((xs[7] + xs[8]) / 2),
         "decoded back to OTAP",
         "OTAP receiver lifts framed bytes into OtapArrowRecords"),
    ]
    base_y = band_bot + TRACK_BOT_PAD + 6
    for ax, head, sub in callouts:
        # Vertical leader from the trace down to the callout.
        out.append(
            f'<line x1="{ax}" y1="{band_bot + 26}" '
            f'x2="{ax}" y2="{base_y - 6}" '
            f'stroke="{COLOR_SUBLABEL}" stroke-width="1" '
            f'stroke-dasharray="2,3"/>'
        )
        # Headline + sub stacked beneath.
        out.append(
            f'<text x="{ax}" y="{base_y + 12}" text-anchor="middle" '
            f'font-family="{FONT}" font-size="15" font-weight="700" '
            f'fill="{COLOR_LABEL}">{_esc(head)}</text>'
        )
        out.append(
            f'<text x="{ax}" y="{base_y + 30}" text-anchor="middle" '
            f'font-family="{FONT}" font-size="13" font-style="italic" '
            f'fill="{COLOR_SUBLABEL}">{_esc(sub)}</text>'
        )


def _zero_copy_strip(out: List[str], xs: List[float],
                     band_bot: float) -> None:
    """A single thin band running underneath the in-process portion of
    the trace, captioned 'in-process zero-copy (Arc / bytes::Bytes)'.
    Establishes the first of the three zero-copy points without an
    extra callout that would crowd the figure.
    """
    x0 = xs[0]
    x1 = xs[6]      # up to (but not including) the wire segment
    y = band_bot + 38
    out.append(
        f'<rect x="{x0}" y="{y}" width="{x1 - x0}" height="14" '
        f'rx="3" ry="3" fill="{COLOR_HIGH}" fill-opacity="0.12" '
        f'stroke="{COLOR_HIGH}" stroke-width="0.8"/>'
    )
    out.append(
        f'<text x="{(x0 + x1)/2}" y="{y + 11}" text-anchor="middle" '
        f'font-family="{FONT_MONO}" font-size="13" font-weight="700" '
        f'fill="{COLOR_HIGH}">'
        f'in-process zero-copy \u2014 Arc&lt;OtapArrowRecords&gt; / '
        f'bytes::Bytes</text>'
    )


def _legend(out: List[str], y: float) -> None:
    items = [
        (COLOR_LOW,  "OTLP"),
        (COLOR_HIGH, "OTAP"),
        (COLOR_MID,  "framed bytes (wire / disk)"),
    ]
    x = PAGE_MARGIN_X
    for color, label in items:
        out.append(
            f'<line x1="{x}" y1="{y}" x2="{x + 28}" y2="{y}" '
            f'stroke="{color}" stroke-width="{LINE_W}"/>'
        )
        out.append(
            f'<text x="{x + 36}" y="{y + 5}" font-family="{FONT}" '
            f'font-size="15" fill="{COLOR_LABEL}">{_esc(label)}</text>'
        )
        x += 36 + 8 * len(label) + 30


# ------------------------------------------------------------------ render

def _zero_copy_spans(out: List[str], xs: List[float]) -> None:
    """Two horizontal spans drawn just below the phys diagram, each
    bracketing a pair of adjacent nodes whose pdata edge is zero-copy.

    The span color matches the signal level at that point: OTLP-red for
    the OTLP receiver -> batch hop (``bytes::Bytes`` shared), OTAP-blue
    for the durable_buffer(read) -> otap_exporter hop
    (``Arc<OtapArrowRecords>`` shared).
    """
    spans: List[Tuple[float, float, str, str]] = [
        # (left_x, right_x, color, label)
        (xs[0], xs[2], COLOR_LOW,
         "zero-copy \u00b7 bytes::Bytes"),
        (xs[2], xs[3], COLOR_TRACK,
         "OTLP to OTAP"),
        (xs[4], xs[6], COLOR_HIGH,
         "zero-copy \u00b7 Arc<OtapArrowRecords>"),
        (xs[6], xs[7], COLOR_MID,
         "Arrow IPC (zstd)"),
    ]
    y = NODE_STRIP_Y + NODE_STRIP_H + 22       # bar y, just below phys
    pad = 14
    tip_h = 8                                   # rightward arrow size
    tip_w = 10
    for x0, x1, color, label in spans:
        a = x0 + pad
        b = x1 - pad
        # Horizontal span bar terminating in a rightward-pointing tip.
        out.append(
            f'<line x1="{a}" y1="{y}" x2="{b - tip_w + 1}" y2="{y}" '
            f'stroke="{color}" stroke-width="2"/>'
        )
        out.append(
            f'<path d="M{b - tip_w},{y - tip_h} '
            f'L{b},{y} L{b - tip_w},{y + tip_h} z" '
            f'fill="{color}"/>'
        )
        # Label centered below the bar.
        out.append(
            f'<text x="{(a + b) / 2}" y="{y + 22}" text-anchor="middle" '
            f'font-family="{FONT_MONO}" font-size="15" font-weight="700" '
            f'fill="{color}">{_esc(label)}</text>'
        )


def _disk_icon(out: List[str], cx: float, top_y: float,
               w: float = 64, h: float = 36,
               color: str = COLOR_MID) -> None:
    """A simple disk/cylinder glyph drawn as two ellipses + side lines.
    Anchor: ``(cx, top_y)`` is the center of the top ellipse.
    """
    rx = w / 2
    ry = 6
    # Body sides
    out.append(
        f'<rect x="{cx - rx}" y="{top_y}" width="{w}" height="{h}" '
        f'fill="white" stroke="none"/>'
    )
    out.append(
        f'<line x1="{cx - rx}" y1="{top_y}" x2="{cx - rx}" '
        f'y2="{top_y + h}" stroke="{color}" stroke-width="1.4"/>'
    )
    out.append(
        f'<line x1="{cx + rx}" y1="{top_y}" x2="{cx + rx}" '
        f'y2="{top_y + h}" stroke="{color}" stroke-width="1.4"/>'
    )
    # Bottom curve only (front half of the bottom ellipse)
    out.append(
        f'<path d="M{cx - rx},{top_y + h} '
        f'A{rx},{ry} 0 0 0 {cx + rx},{top_y + h}" '
        f'fill="none" stroke="{color}" stroke-width="1.4"/>'
    )
    # Top ellipse (drawn last so it overlays the side strokes cleanly)
    out.append(
        f'<ellipse cx="{cx}" cy="{top_y}" rx="{rx}" ry="{ry}" '
        f'fill="white" stroke="{color}" stroke-width="1.4"/>'
    )


def _spool_loop(out: List[str], xs: List[float]) -> None:
    """Encode-down + zero-copy-decode-up arrows linking the first half
    of the ``durable_buffer`` node to a disk icon below.
    """
    cx_spool = (xs[3] + xs[4]) / 2
    node_bottom = NODE_STRIP_Y + NODE_STRIP_H

    disk_cx = cx_spool
    disk_top = 440
    disk_w, disk_h = 64, 36

    # Two straight vertical arrows, side by side, between the node
    # bottom and the disk top.
    enc_color = COLOR_MID
    dec_color = COLOR_HIGH
    arrow_w = 5
    arrow_h = 8

    gap = 18
    enc_x = cx_spool - gap / 2          # encode (down) on the left
    dec_x = cx_spool + gap / 2          # decode (up)   on the right
    y_top = node_bottom + 4
    y_bot = disk_top - 4

    # ---- encode: down ------------------------------------------
    out.append(
        f'<line x1="{enc_x}" y1="{y_top}" x2="{enc_x}" y2="{y_bot - arrow_h}" '
        f'stroke="{enc_color}" stroke-width="1.6"/>'
    )
    out.append(
        f'<path d="M{enc_x - arrow_w},{y_bot - arrow_h} '
        f'L{enc_x},{y_bot} L{enc_x + arrow_w},{y_bot - arrow_h} z" '
        f'fill="{enc_color}"/>'
    )

    # ---- decode: up --------------------------------------------
    out.append(
        f'<line x1="{dec_x}" y1="{y_bot}" x2="{dec_x}" y2="{y_top + arrow_h}" '
        f'stroke="{dec_color}" stroke-width="1.6"/>'
    )
    out.append(
        f'<path d="M{dec_x - arrow_w},{y_top + arrow_h} '
        f'L{dec_x},{y_top} L{dec_x + arrow_w},{y_top + arrow_h} z" '
        f'fill="{dec_color}"/>'
    )

    # ---- single label, left of the arrow pair ------------------
    label_lines = ["Arrow IPC", "uncompressed"]
    label_y0 = (y_top + y_bot) / 2 - (len(label_lines) - 1) * 8
    for i, line in enumerate(label_lines):
        out.append(
            f'<text x="{enc_x - 14}" y="{label_y0 + i * 16}" '
            f'text-anchor="end" font-family="{FONT_MONO}" font-size="14" '
            f'fill="{COLOR_LABEL}">{_esc(line)}</text>'
        )

    # ---- companion label, right of the arrow pair --------------
    right_lines = ["Zero-copy", "mmap"]
    for i, line in enumerate(right_lines):
        out.append(
            f'<text x="{dec_x + 14}" y="{label_y0 + i * 16}" '
            f'text-anchor="start" font-family="{FONT_MONO}" font-size="14" '
            f'fill="{COLOR_LABEL}">{_esc(line)}</text>'
        )

    # ---- disk icon ---------------------------------------------
    _disk_icon(out, disk_cx, disk_top, disk_w, disk_h)
    out.append(
        f'<text x="{disk_cx}" y="{disk_top + disk_h + 18}" '
        f'text-anchor="middle" font-family="{FONT_MONO}" font-size="13" '
        f'fill="{COLOR_SUBLABEL}">Quiver</text>'
    )


def _filter_annotation(out: List[str], xs: List[float]) -> None:
    """Small ``OTLP -> OTAP`` label centered below the filter node,
    on the same baseline as the zero-copy span labels.
    """
    cx = (xs[2] + xs[3]) / 2
    y = NODE_STRIP_Y + NODE_STRIP_H + 22 + 22       # span baseline
    out.append(
        f'<text x="{cx}" y="{y}" text-anchor="middle" '
        f'font-family="{FONT_MONO}" font-size="15" font-weight="700" '
        f'fill="{COLOR_LABEL}">OTLP \u2192 OTAP</text>'
    )


def render() -> str:
    track_x0 = PAGE_MARGIN_X + TRACK_LEFT_PAD
    track_x1 = PAGE_W - PAGE_MARGIN_X - TRACK_RIGHT_PAD
    xs = _xs(track_x0, track_x1)

    band_top = NODE_STRIP_Y + NODE_STRIP_H + TRACK_TOP_PAD
    band_bot = band_top + TRACK_BAND
    mid_y = (band_top + band_bot) / 2

    page_h = max(SLIDE_PAGE_H,
                 band_bot + TRACK_BOT_PAD + ANNOTATION_H + PAGE_BOTTOM_PAD)

    out: List[str] = []
    out.append(page_open(PAGE_W, page_h))
    # Arrow tip marker for the node-strip connectors.
    out.append(
        '<defs>'
        '<marker id="tip" viewBox="0 0 10 10" refX="8" refY="5" '
        'markerWidth="6" markerHeight="6" orient="auto">'
        f'<path d="M0,0 L10,5 L0,10 z" fill="{COLOR_TRACK}"/>'
        '</marker>'
        '</defs>'
    )

    # Title bar -- same chrome as the rest of the deck.
    out.append(title_bar(
        PAGE_MARGIN_X, TITLE_Y, PAGE_W - 2 * PAGE_MARGIN_X,
        title="Zero-copy & Arrow IPC",
        urn="signal level view",
        accent=COLOR_OTAP,
    ))
    out.append(
        f'<text x="{PAGE_MARGIN_X}" y="{SUBTITLE_Y}" font-family="{FONT}" '
        f'font-size="{FS_SUBTITLE}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'OTLP to OTAP on demand, Arrow IPC serialization'
        f'</text>'
    )

    _node_strip(out, xs)
    _zero_copy_spans(out, xs)
    _spool_loop(out, xs)
    _grid_lines(out, track_x0, track_x1, band_top, band_bot, mid_y)
    _level_legend(out, track_x0, band_top, band_bot)
    _trace(out, xs, band_top, band_bot)

    out.append(page_close())
    return "\n".join(out)


def main(argv: List[str]) -> int:
    out = argv[1] if len(argv) > 1 else "zero_copy.svg"
    svg = render()
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
