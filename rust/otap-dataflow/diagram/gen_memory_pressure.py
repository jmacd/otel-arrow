#!/usr/bin/env python3
"""Memory-pressure & backpressure slide.

A single overhead slide that makes visible *every* place data can sit
inside the dataflow engine, and shows that those places are all bounded
and named. The point of the slide is the design invariant: there is no
unbounded growth anywhere -- every buffer / queue / slot is sized at
construction, every node's state list is finite and known, and the
process-memory-limiter has a privileged view into the steady-state cost
of holding the pipeline. Backpressure under constrained memory works
because Ack/Nack drains those finite slots.

Composition:

- Left: ``Controller process`` box, holding the
  ``process-memory-limiter`` accessory task and the
  ``memory_pressure watch`` shared resource. Inside the limiter tile we
  expose the process-wide ``MemoryPressureState`` atomics and the three
  classified levels (Normal / Soft / Hard) as chips. Effective limits
  (soft_limit, hard_limit, retry_after_secs, mode) are listed beneath.
- Right: three ``Engine Core`` tiles stacked vertically. Each holds the
  same illustrative pipeline (``otap_receiver -> batch -> retry ->
  otlp_grpc_exporter``) drawn as four node boxes with bounded
  inter-node channel capsules between them. The intent is that a
  viewer can count the boxes and channels and convince themselves the
  total memory footprint of one core is bounded by the sum of those
  named pieces. (Per-node fixed-state lists are tracked in
  ``PIPELINE_NODES`` for reference but are intentionally not drawn on
  this slide -- they made the right column too busy.)
- Cross-thread broadcast arrows (dashed grey) run from the
  ``memory_pressure watch`` resource into each core, conveying that
  every receiver subscribes and that admission decisions are made
  locally on each core from the same shared signal.
- A bottom strip of three short takeaway statements pins the design
  invariant in words.

Source of truth (file:line) so the picture stays honest:

- ``MemoryPressureState`` atomics --
  rust/otap-dataflow/crates/engine/src/memory_limiter.rs:100,
  :104 (level/usage_bytes/soft_limit/hard_limit/mode/retry_after_secs).
- ``MemoryPressureLevel`` (Normal / Soft / Hard) --
  crates/engine/src/memory_limiter.rs:39.
- ``MemoryLimiterPolicy`` (mode, source, check_interval, soft_limit,
  hard_limit, hysteresis, retry_after_secs, fail_readiness_on_hard,
  purge_on_hard, purge_min_interval) --
  crates/config/src/policy.rs:392.
- ``process-memory-limiter`` accessory task spawn + tick loop --
  crates/controller/src/lib.rs:1252.
- ``memory_pressure_tx`` watch broadcast to per-core pipelines --
  crates/controller/src/lib.rs:1205, :1823.
- Per-node state lists are mirrored from the per-node SPECs:
  ``gen_node_otap_receiver.py``, ``gen_node_batch.py``,
  ``gen_node_retry.py``, ``gen_node_otlp_grpc_exporter.py``.

Visual grammar follows ``node_lib`` -- same palette, title bar, notes
chrome, and arrow markers as the rest of the deck.
"""

from __future__ import annotations
import sys
from typing import List, Tuple

from node_lib import (
    SLIDE_PAGE_W, SLIDE_PAGE_H, SLIDE_MARGIN_X, SLIDE_MARGIN_Y,
    COLOR_OTAP, COLOR_OTLP, COLOR_CTRL, COLOR_CTRL_SOFT, COLOR_CTX,
    COLOR_OK, COLOR_FAIL,
    FS_TITLE, FS_SUBTITLE, FS_NODE, FS_NODE_SUB, FS_LABEL, FS_TINY,
    W_PDATA, W_CTRL, W_FRAME,
    page_open, page_close, title_bar, arrow_marker_defs,
    ctrl_edge,
    _esc,
)
from gen_diagram import COLOR_BG, COLOR_LABEL, COLOR_SUBLABEL, FONT, FONT_MONO


# ----------------------------------------------------------------- layout

PAGE_W = SLIDE_PAGE_W
PAGE_H = SLIDE_PAGE_H

TITLE_X = SLIDE_MARGIN_X
TITLE_Y = 60
SUBTITLE_Y = 90

# Left: controller / memory-limiter panel.
LEFT_X = SLIDE_MARGIN_X
LEFT_Y = 130
LEFT_W = 480
LEFT_H = 600

# Right: stack of three engine cores.
RIGHT_X = LEFT_X + LEFT_W + 60
RIGHT_W = PAGE_W - RIGHT_X - SLIDE_MARGIN_X    # 940 with default margins
CORE_H = 175
CORE_GAP = 26
CORE_Y0 = LEFT_Y

# Bottom takeaway strip.
TAKEAWAY_Y = LEFT_Y + LEFT_H + 30
TAKEAWAY_H = 70


# ----------------------------------------------------------- primitives

def _process_box(x: float, y: float, w: float, h: float,
                 title: str, struct_name: str,
                 accent: str = COLOR_CTRL) -> str:
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" ry="14" '
        f'fill="white" stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>',
        f'<rect x="{x}" y="{y}" width="{w}" height="6" rx="3" ry="3" '
        f'fill="{accent}"/>',
        f'<text x="{x + 22}" y="{y + 34}" font-size="{FS_NODE}" '
        f'font-weight="700" fill="{COLOR_LABEL}">{_esc(title)}</text>',
        f'<text x="{x + w - 16}" y="{y + 28}" text-anchor="end" '
        f'font-size="{FS_LABEL}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_SUBLABEL}">{_esc(struct_name)}</text>',
    ]
    return "".join(parts)


def _section_label(x: float, y: float, text: str) -> str:
    return (
        f'<text x="{x}" y="{y}" font-size="{FS_LABEL}" '
        f'font-weight="700" fill="{COLOR_LABEL}">{_esc(text)}</text>'
    )


def _kv_row(x: float, y: float, w: float, name: str, type_: str,
            mono: bool = True, name_color: str = None,
            type_color: str = None) -> str:
    name_color = name_color or COLOR_LABEL
    type_color = type_color or COLOR_SUBLABEL
    fam = FONT_MONO if mono else FONT
    return (
        f'<text x="{x}" y="{y}" font-size="{FS_TINY}" '
        f'font-family="{fam}" font-weight="700" '
        f'fill="{name_color}">{_esc(name)}</text>'
        f'<text x="{x + w}" y="{y}" text-anchor="end" '
        f'font-size="{FS_TINY}" font-family="{fam}" '
        f'fill="{type_color}">{_esc(type_)}</text>'
    )


def _level_chip(x: float, y: float, w: float, h: float,
                label: str, fill: str, fg: str = "white") -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" ry="6" '
        f'fill="{fill}" stroke="{fill}"/>'
        f'<text x="{x + w/2}" y="{y + h/2 + 5}" text-anchor="middle" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'font-weight="700" fill="{fg}">{_esc(label)}</text>'
    )


def _shared_bubble(x: float, y: float, w: float, h: float,
                   name: str, sub: str = "") -> str:
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" ry="14" '
        f'fill="#fbf3f2" stroke="{COLOR_OTLP}" stroke-width="1.4"/>',
        f'<text x="{x + 12}" y="{y + h/2 + 5}" font-size="{FS_LABEL}" '
        f'font-family="{FONT_MONO}" font-weight="700" '
        f'fill="{COLOR_LABEL}">{_esc(name)}</text>',
    ]
    if sub:
        parts.append(
            f'<text x="{x + w - 12}" y="{y + h/2 + 5}" text-anchor="end" '
            f'font-size="{FS_TINY}" font-style="italic" '
            f'fill="{COLOR_SUBLABEL}">{_esc(sub)}</text>'
        )
    return "".join(parts)


# ---------------------------------------------------- left column render

def _render_controller_left(out: List[str]) -> Tuple[float, float]:
    """Render the controller / memory-limiter panel.

    Returns the (x, y) of the right-edge midpoint of the
    ``memory_pressure watch`` shared bubble so cross-thread broadcast
    arrows can originate from a single, visually-anchored point.
    """
    out.append(_process_box(
        LEFT_X, LEFT_Y, LEFT_W, LEFT_H,
        title="Controller process",
        struct_name="ControllerRuntime",
        accent=COLOR_CTRL,
    ))

    pad = 22
    inner_x = LEFT_X + pad
    inner_w = LEFT_W - 2 * pad

    # ---- process-memory-limiter accessory task (a sub-box) -----------
    lim_y = LEFT_Y + 64
    lim_h = 290

    out.append(
        f'<rect x="{inner_x}" y="{lim_y}" width="{inner_w}" '
        f'height="{lim_h}" rx="10" ry="10" fill="white" '
        f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1.4"/>'
    )
    # Tile header
    out.append(
        f'<text x="{inner_x + 12}" y="{lim_y + 22}" '
        f'font-size="{FS_NODE_SUB}" font-family="{FONT_MONO}" '
        f'font-weight="700" fill="{COLOR_LABEL}">'
        f'process-memory-limiter</text>'
    )
    # ---- MemoryPressureState atomics list --------------------------
    state_y = lim_y + 48
    out.append(_section_label(
        inner_x + 12, state_y, "MemoryPressureState"))
    out.append(
        f'<text x="{inner_x + inner_w - 12}" y="{state_y}" '
        f'text-anchor="end" font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">Arc&lt;…Inner&gt; \u2014 fixed</text>'
    )
    rows: List[Tuple[str, str]] = [
        ("level",                 "AtomicU8"),
        ("usage_bytes",           "AtomicU64"),
        ("soft_limit_bytes",      "AtomicU64"),
        ("hard_limit_bytes",      "AtomicU64"),
        ("retry_after_secs",      "AtomicU32"),
        ("mode",                  "AtomicU8"),
        ("fail_readiness_on_hard", "AtomicBool"),
    ]
    rx = inner_x + 18
    rw = inner_w - 36
    ry = state_y + 18
    for name, type_ in rows:
        out.append(_kv_row(rx, ry, rw, name, type_))
        ry += 18

    # Pressure-level chips
    chip_y = ry + 12
    chip_h = 26
    chip_gap = 8
    chip_w = (inner_w - 24 - 2 * chip_gap) / 3
    cx = inner_x + 12
    out.append(_level_chip(cx, chip_y, chip_w, chip_h,
                           "Normal", COLOR_OK))
    out.append(_level_chip(cx + (chip_w + chip_gap), chip_y,
                           chip_w, chip_h, "Soft", "#c9943a"))
    out.append(_level_chip(cx + 2 * (chip_w + chip_gap), chip_y,
                           chip_w, chip_h, "Hard", COLOR_OTLP))

    # ---- MemoryLimiterPolicy summary -------------------------------
    pol_y = lim_y + lim_h + 22
    out.append(_section_label(inner_x, pol_y, "MemoryLimiterPolicy"))
    out.append(
        f'<text x="{inner_x + inner_w}" y="{pol_y}" '
        f'text-anchor="end" font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">policies.resources.memory_limiter</text>'
    )
    pol_rows: List[Tuple[str, str]] = [
        ("mode",                   "MemoryLimiterMode"),
        ("source",                 "MemoryLimiterSource"),
        ("check_interval",         "Duration"),
        ("soft_limit / hard_limit", "Option<u64>"),
        ("hysteresis",             "Option<u64>"),
        ("retry_after_secs",       "u32"),
        ("fail_readiness_on_hard", "bool"),
        ("purge_on_hard",          "bool"),
    ]
    py = pol_y + 18
    for name, type_ in pol_rows:
        out.append(_kv_row(inner_x + 6, py, inner_w - 12, name, type_))
        py += 16

    # ---- memory_pressure watch shared bubble (broadcast root) ------
    bub_h = 36
    bub_y = LEFT_Y + LEFT_H - bub_h - 22
    bub_x = inner_x
    bub_w = inner_w
    out.append(_shared_bubble(bub_x, bub_y, bub_w, bub_h,
                              "memory_pressure watch",
                              "tokio::sync::watch"))
    return (bub_x + bub_w, bub_y + bub_h / 2)


# ---------------------------------------------------- right column render

# One illustrative pipeline shared across all three cores. Each entry:
#   (display_name, role, struct/module, [bounded state lines])
# The state-lines column is preserved for documentation purposes; the
# slide intentionally does not draw it any more (it was too busy in
# the engine-core tiles).
PIPELINE_NODES: List[Tuple[str, str, str, List[str]]] = [
    ("receiver", "receiver", "OTAPReceiver",
     ["shared admission state",
      "ack subscription registry",
      "rejection counters"]),
    ("batch", "processor", "BatchProcessor",
     ["pending batch buffers",
      "inbound  ack/nack slots",
      "outbound ack/nack slots"]),
    ("retry", "processor", "RetryProcessor",
     ["local timer wheel",
      "(engine-held payload)"]),
    ("exporter", "exporter", "OTLPExporter",
     ["in-flight queue (max_in_flight)",
      "parked-message slot",
      "proto encoders + buffers"]),
]

# Channel-size labels between adjacent nodes (illustrative; the real
# numbers come from per-node config -- the point is they are *named
# bounds*, not "unlimited").
CHANNEL_LABELS: List[str] = [
    "channel\n(bounded)",
    "channel\n(bounded)",
    "channel\n(bounded)",
]


def _node_tile(x: float, y: float, w: float, h: float,
               name: str, role: str,
               accent: str = COLOR_CTRL) -> str:
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" ry="6" '
        f'fill="white" stroke="{COLOR_CTRL}" stroke-width="1.4"/>',
        f'<rect x="{x}" y="{y}" width="{w}" height="4" rx="2" ry="2" '
        f'fill="{accent}"/>',
        f'<text x="{x + w/2}" y="{y + h/2 + 1}" text-anchor="middle" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'font-weight="700" fill="{COLOR_LABEL}">{_esc(name)}</text>',
        f'<text x="{x + w/2}" y="{y + h/2 + 16}" text-anchor="middle" '
        f'font-size="{FS_TINY - 3}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">{_esc(role)}</text>',
    ]
    return "".join(parts)


def _channel_capsule(x: float, y: float, w: float, h: float) -> str:
    """A small capsule between two nodes -- represents a *bounded*
    in-thread channel. Drawn as a stadium with a fixed-capacity glyph
    inside (three small ticks) so the eye reads "fixed slots".
    """
    cy = y + h / 2
    parts = [
        # Capsule shape (fully rounded ends)
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" '
        f'rx="{h/2}" ry="{h/2}" fill="#f4f6f8" '
        f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>',
    ]
    # Three vertical tick marks = three fixed slots.
    tick_h = h - 10
    tick_y0 = y + 5
    n_ticks = 3
    span = w - 18
    x0 = x + 9
    for i in range(n_ticks):
        tx = x0 + (span / (n_ticks - 1)) * i
        parts.append(
            f'<line x1="{tx}" y1="{tick_y0}" x2="{tx}" '
            f'y2="{tick_y0 + tick_h}" '
            f'stroke="{COLOR_CTRL}" stroke-width="1.2"/>'
        )
    # "bounded" caption above the capsule
    parts.append(
        f'<text x="{x + w/2}" y="{y - 4}" text-anchor="middle" '
        f'font-size="{FS_TINY - 4}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">bounded</text>'
    )
    return "".join(parts)


def _slotmap_rect(x: float, y: float, w: float, h: float,
                  label: str) -> str:
    """Small labeled rectangle representing an internal slotmap that
    a node uses to track per-message state. The shape is intentionally
    minimal -- the point is to make visible that the node's tracking
    storage is itself a finite, named structure.
    """
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="4" ry="4" '
        f'fill="#f4f6f8" stroke="{COLOR_CTRL}" stroke-width="1"/>',
        f'<text x="{x + w/2}" y="{y + h/2 + 4}" text-anchor="middle" '
        f'font-size="{FS_TINY - 2}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">{_esc(label)}</text>',
        f'<text x="{x + w/2}" y="{y - 4}" text-anchor="middle" '
        f'font-size="{FS_TINY - 4}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">slotmap</text>',
    ]
    return "".join(parts)


# Internal-state slotmaps to draw above each node. A node not present
# in this mapping gets nothing drawn above it (the retry node holds
# its parked payload in the engine, not in a node-local slotmap).
NODE_SLOTMAPS: dict = {
    "receiver": ["pending"],
    "batch":    ["inbound", "outbound"],
    "exporter": ["inflight"],
}


def _render_one_core(out: List[str], y: float,
                     core_label: str) -> Tuple[float, float]:
    """Render one engine-core tile and return (left_edge_x, mid_y)
    of the receiver's left port so the broadcast arrow can land there.
    """
    x = RIGHT_X
    w = RIGHT_W

    # Outer core frame
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{CORE_H}" '
        f'rx="10" ry="10" fill="white" stroke="{COLOR_CTRL}" '
        f'stroke-width="{W_FRAME}"/>'
    )
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="6" '
        f'rx="3" ry="3" fill="{COLOR_OTAP}"/>'
    )
    out.append(
        f'<text x="{x + 16}" y="{y + 30}" font-size="{FS_NODE_SUB}" '
        f'font-weight="700" fill="{COLOR_LABEL}">{_esc(core_label)}</text>'
    )

    # Pipeline strip geometry
    n = len(PIPELINE_NODES)
    pad_l = 90       # leave room for the broadcast arrow tip on the left
    pad_r = 40
    strip_x = x + pad_l
    strip_w = w - pad_l - pad_r
    node_w = 132
    cap_h = 24
    cap_gap = 14
    total_caps_w = (n - 1) * (cap_gap + 70 + cap_gap)
    # Recompute: distribute remaining horizontal space to capsules
    remaining = strip_w - n * node_w
    # n-1 capsules; each capsule has some horizontal padding on each side
    cap_w = max(60, (remaining - 2 * cap_gap * (n - 1)) / (n - 1))

    node_h = 50
    # Place the pipeline strip near the bottom of the core so we have
    # room above each node to hang slotmap rectangles.
    slotmap_h = 32
    slotmap_top_pad = 14            # gap below the header
    slotmap_to_node_gap = 22        # gap between slotmap row and nodes
    node_y = y + 36 + slotmap_top_pad + slotmap_h + slotmap_to_node_gap
    strip_y_mid = node_y + node_h / 2
    slotmap_y = node_y - slotmap_to_node_gap - slotmap_h

    # Walk left to right placing nodes + capsules
    cx = strip_x
    receiver_left_x = strip_x       # for the broadcast arrow
    for i, (name, role, _struct, _state_lines) in enumerate(PIPELINE_NODES):
        # Node accent: receiver/exporter neutral grey, processors too
        # (all neutral; format chips would distract from the memory
        # story).
        out.append(_node_tile(cx, node_y, node_w, node_h,
                              name=name, role=role,
                              accent=COLOR_CTRL))
        # Slotmap rectangle(s) above the node (if any)
        labels = NODE_SLOTMAPS.get(name, [])
        if labels:
            inner_gap = 8
            sm_w = (node_w - inner_gap * (len(labels) - 1)) / len(labels)
            for j, lbl in enumerate(labels):
                sx = cx + j * (sm_w + inner_gap)
                out.append(_slotmap_rect(sx, slotmap_y, sm_w, slotmap_h,
                                         lbl))
                # Thin connector down to the node body
                out.append(
                    f'<line x1="{sx + sm_w/2}" y1="{slotmap_y + slotmap_h}" '
                    f'x2="{sx + sm_w/2}" y2="{node_y}" '
                    f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1" '
                    f'stroke-dasharray="2,2"/>'
                )
        # Capsule between this node and the next
        if i < n - 1:
            cap_x = cx + node_w + cap_gap
            cap_y = strip_y_mid - cap_h / 2
            out.append(_channel_capsule(cap_x, cap_y, cap_w, cap_h))
            # pdata edge through the capsule (visual continuity)
            arrow_y = strip_y_mid
            out.append(
                f'<line x1="{cx + node_w}" y1="{arrow_y}" '
                f'x2="{cap_x}" y2="{arrow_y}" '
                f'stroke="{COLOR_CTRL}" stroke-width="1.4"/>'
            )
            out.append(
                f'<line x1="{cap_x + cap_w}" y1="{arrow_y}" '
                f'x2="{cap_x + cap_w + cap_gap - 2}" y2="{arrow_y}" '
                f'stroke="{COLOR_CTRL}" stroke-width="1.4" '
                f'marker-end="url(#ah-pdata)"/>'
            )
            cx = cap_x + cap_w + cap_gap
        else:
            cx = cx + node_w

    return (receiver_left_x, strip_y_mid)


def _render_cores(out: List[str]) -> List[Tuple[float, float]]:
    """Render three engine cores. Returns receiver-port anchor points."""
    anchors: List[Tuple[float, float]] = []
    y = CORE_Y0
    for label in ("Engine Core 0", "Engine Core 1", "Engine Core N"):
        anchors.append(_render_one_core(out, y, label))
        y += CORE_H + CORE_GAP
    return anchors


# ---------------------------------------------------- broadcast arrows

def _render_broadcast(out: List[str],
                      origin: Tuple[float, float],
                      anchors: List[Tuple[float, float]]) -> None:
    """Dashed arrows from the ``memory_pressure watch`` shared bubble
    out to each core's left wall. The vertical trunk is centered in
    the gap between the controller panel and the engine cores so it
    doesn't crowd either side, and arrowheads terminate on the core
    boundary (not inside it).
    """
    ox, oy = origin
    # Center the vertical trunk in the gap between the controller
    # panel and the engine cores.
    trunk_x = (LEFT_X + LEFT_W + RIGHT_X) / 2
    # Terminate arrows on the core's outer left wall.
    core_left = RIGHT_X
    for i, (_ax, ay) in enumerate(anchors):
        path = (
            f'M{ox},{oy} L{trunk_x},{oy} '
            f'L{trunk_x},{ay} L{core_left - 4},{ay}'
        )
        out.append(
            f'<path d="{path}" fill="none" stroke="{COLOR_OTLP}" '
            f'stroke-width="1.2" stroke-dasharray="5,3" '
            f'marker-end="url(#ah-ctrl)" opacity="0.85"/>'
        )


# ---------------------------------------------------------- takeaways

def _render_takeaways(out: List[str]) -> None:
    """Three short factual statements pinning the design invariant."""
    y = TAKEAWAY_Y
    h = TAKEAWAY_H
    out.append(
        f'<rect x="{LEFT_X}" y="{y}" '
        f'width="{PAGE_W - 2 * SLIDE_MARGIN_X}" height="{h}" '
        f'rx="10" ry="10" fill="#f4f6f8" stroke="{COLOR_CTRL_SOFT}" '
        f'stroke-width="1"/>'
    )
    items: List[Tuple[str, str]] = [
        ("All buffers are sized at construction.",
         "avoid unbounded growth"),
        ("Backpressure flows backwards via Ack/Nack.",
         "drains the in-flight slots"),
        ("Hard pressure sheds at ingress.",
         "receivers reject with Retry-After"),
    ]
    n = len(items)
    col_w = (PAGE_W - 2 * SLIDE_MARGIN_X) / n
    for i, (line, sub) in enumerate(items):
        x = LEFT_X + i * col_w
        # Numeral chip
        out.append(
            f'<circle cx="{x + 22}" cy="{y + h/2}" r="13" '
            f'fill="{COLOR_OTAP}"/>'
        )
        out.append(
            f'<text x="{x + 22}" y="{y + h/2 + 5}" text-anchor="middle" '
            f'font-size="{FS_LABEL}" font-weight="700" fill="white">'
            f'{i+1}</text>'
        )
        out.append(
            f'<text x="{x + 46}" y="{y + h/2 - 3}" '
            f'font-size="{FS_LABEL}" font-weight="700" '
            f'fill="{COLOR_LABEL}">{_esc(line)}</text>'
        )
        out.append(
            f'<text x="{x + 46}" y="{y + h/2 + 16}" '
            f'font-size="{FS_TINY}" font-style="italic" '
            f'fill="{COLOR_SUBLABEL}">{_esc(sub)}</text>'
        )


# ---------------------------------------------------------------- render

def _subtitle(out: List[str]) -> None:
    out.append(
        f'<text x="{TITLE_X}" y="{SUBTITLE_Y}" font-size="{FS_SUBTITLE}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'Every queue and slot is named, sized, and visible to the '
        f'process-memory-limiter.'
        f'</text>'
    )


def render() -> str:
    out: List[str] = []
    out.append(page_open(PAGE_W, PAGE_H))
    out.append(arrow_marker_defs())
    out.append(title_bar(
        TITLE_X, TITLE_Y, PAGE_W - 2 * SLIDE_MARGIN_X,
        title="Memory and backpressure",
        urn="MemoryPressureState + bounded per-node state",
        accent=COLOR_OTAP,
    ))
    _subtitle(out)
    bubble_right_mid = _render_controller_left(out)
    anchors = _render_cores(out)
    _render_broadcast(out, bubble_right_mid, anchors)
    _render_takeaways(out)
    out.append(page_close())
    return "".join(out)


def main(argv: List[str]) -> int:
    out = argv[1] if len(argv) > 1 else "memory_pressure.svg"
    svg = render()
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
