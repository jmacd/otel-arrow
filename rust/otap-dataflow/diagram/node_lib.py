#!/usr/bin/env python3
"""Shared SVG primitives for per-node "overhead" slides.

Each per-node script (``gen_node_<name>.py``) imports this module to get
the page chrome, palette, and a small set of primitives so that all node
slides share a visual vocabulary while leaving the composition (which
panels, where, with which annotations) entirely to the per-node script.

Conventions
-----------
- Color = pdata type. OTAP = blue, OTLP = red. We import these from
  ``gen_diagram`` so all slides re-flow if the palette changes.
- pdata edges are thick and colored by signal type. ack/nack edges are
  thin grey, with a glyph (\u2713 / \u2717) at the head. Control-plane
  flows are dashed grey.
- A "puck" is a small rounded rectangle representing one in-flight
  request. It can carry a context chip below it and a small badge above.
- A "self-loop" gesture (used for ``requeue_later``, retry bounces,
  etc.) is a single canonical curved arc. The same gesture is reused
  across the overhead panel and the per-case mini-strips so the visual
  rhyme reads.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
import math
import re

# Reuse the palette from gen_diagram so the node slides match the
# rest of the deck.
from gen_diagram import (
    COLOR_HIGH as COLOR_OTAP,   # OTAP = blue/teal
    COLOR_LOW  as COLOR_OTLP,   # OTLP = red
    COLOR_BG,
    COLOR_LABEL,
    COLOR_SUBLABEL,
    COLOR_GRID,
    FONT,
    FONT_MONO,
)

# Control-plane / structural greys (not tied to a pdata format).
COLOR_CTRL       = "#52606d"   # ack/nack/control edges
COLOR_CTRL_SOFT  = "#9aa5b1"   # subtle frame strokes
COLOR_CTX        = "#c9b458"   # context-chip tint (state riding on payload)
COLOR_FAIL       = "#b15a4f"   # terminal failure glyphs (reuse OTLP red)
COLOR_OK         = "#3f8f5f"   # terminal success glyphs (cool green)

# Typography sizes.
FS_TITLE     = 34
FS_SUBTITLE  = 20
FS_NODE      = 26
FS_NODE_SUB  = 18
FS_LABEL     = 18
FS_TINY      = 16

# Stroke weights.
W_PDATA      = 3.0
W_CTRL       = 1.4
W_FRAME      = 1.6


# ----------------------------------------------------------------- helpers

def _esc(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def color_for_signal(signal: str) -> str:
    """Return the pdata color for a signal type name."""
    s = signal.lower()
    if s in ("otap", "blue"):
        return COLOR_OTAP
    if s in ("otlp", "red"):
        return COLOR_OTLP
    return COLOR_CTRL


# ---------------------------------------------------------- page chrome

def page_open(w: int, h: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'viewBox="0 0 {w} {h}" font-family="{FONT}">'
        f'<rect width="{w}" height="{h}" fill="{COLOR_BG}"/>'
    )


def page_close() -> str:
    return "</svg>"


def title_bar(x: int, y: int, w: int, title: str, urn: str,
              accent: str = COLOR_OTAP) -> str:
    """Top title bar: large title on the left, urn rendered in mono on the right."""
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="2" fill="{accent}"/>',
        f'<text x="{x}" y="{y - 18}" font-size="{FS_TITLE}" font-weight="600" '
        f'fill="{COLOR_LABEL}">{_esc(title)}</text>',
        f'<text x="{x + w}" y="{y - 18}" font-size="{FS_SUBTITLE}" '
        f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}" '
        f'text-anchor="end">{_esc(urn)}</text>',
    ]
    return "".join(parts)


def footer_legend(x: int, y: int, items: List[Tuple[str, str]]) -> str:
    """Footer legend. Each item is (glyph_svg_fragment, label).

    The glyph fragment is rendered as-is at the current cursor; we then
    advance by a fixed swatch+gap to the label.
    """
    out = []
    cx = x
    for glyph, label in items:
        # Caller's glyph is positioned absolutely; we wrap it in a <g> at cx.
        out.append(f'<g transform="translate({cx},{y})">{glyph}</g>')
        out.append(
            f'<text x="{cx + 44}" y="{y + 4}" font-size="{FS_LABEL}" '
            f'fill="{COLOR_SUBLABEL}">{_esc(label)}</text>'
        )
        cx += 44 + 8 * len(label) + 30
    return "".join(out)


# -------------------------------------------------------------- markers

def arrow_marker_defs() -> str:
    """SVG <defs> for the named arrowheads we use throughout."""
    return (
        '<defs>'
        # pdata arrowhead — filled triangle, color inherited via context-stroke
        '<marker id="ah-pdata" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="7" markerHeight="7" orient="auto-start-reverse" '
        'markerUnits="userSpaceOnUse">'
        '<path d="M0,0 L10,5 L0,10 z" fill="context-stroke"/>'
        '</marker>'
        # control / ack-nack arrowhead — narrow open
        '<marker id="ah-ctrl" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="6" markerHeight="6" orient="auto-start-reverse" '
        'markerUnits="userSpaceOnUse">'
        '<path d="M0,1 L10,5 L0,9" fill="none" stroke="context-stroke" '
        'stroke-width="1.4"/>'
        '</marker>'
        '</defs>'
    )


# ---------------------------------------------------------- primitives

def node_box(x: float, y: float, w: float, h: float,
             title: str, sub: str = "",
             signal: str = "otap") -> str:
    """A rounded rectangle representing a pipeline node.

    Drawn with a 2px colored top stripe in the signal's color, then the
    node title (bold), then an italic sub-line.
    """
    color = color_for_signal(signal)
    return (
        f'<g>'
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" ry="10" '
        f'fill="white" stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>'
        f'<rect x="{x}" y="{y}" width="{w}" height="6" rx="3" ry="3" '
        f'fill="{color}"/>'
        f'<text x="{x + w/2}" y="{y + 28}" text-anchor="middle" '
        f'font-size="{FS_NODE}" font-weight="600" fill="{COLOR_LABEL}">'
        f'{_esc(title)}</text>'
        + (f'<text x="{x + w/2}" y="{y + 46}" text-anchor="middle" '
           f'font-size="{FS_NODE_SUB}" font-style="italic" '
           f'fill="{COLOR_SUBLABEL}">{_esc(sub)}</text>' if sub else "")
        + f'</g>'
    )


def pdata_edge(x1: float, y1: float, x2: float, y2: float,
               signal: str = "otap", arrow_at: str = "end") -> str:
    """Thick colored edge for a pdata flow."""
    color = color_for_signal(signal)
    marker = ""
    if arrow_at == "end":
        marker = ' marker-end="url(#ah-pdata)"'
    elif arrow_at == "start":
        marker = ' marker-start="url(#ah-pdata)"'
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="{color}" stroke-width="{W_PDATA}" stroke-linecap="round"{marker}/>'
    )


def ctrl_edge(x1: float, y1: float, x2: float, y2: float,
              dashed: bool = False, arrow_at: str = "end") -> str:
    """Thin grey edge for ack/nack/control flows."""
    dash = ' stroke-dasharray="4,3"' if dashed else ""
    marker = ""
    if arrow_at == "end":
        marker = ' marker-end="url(#ah-ctrl)"'
    elif arrow_at == "start":
        marker = ' marker-start="url(#ah-ctrl)"'
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="{COLOR_CTRL}" stroke-width="{W_CTRL}"{dash}{marker}/>'
    )


def curvy_loop(x1: float, y1: float, x2: float, y2: float,
               bulge: float = 40.0,
               color: str = COLOR_CTRL,
               width: float = W_CTRL,
               arrow: bool = True,
               above: bool = True) -> str:
    """A canonical curved arc from (x1,y1) to (x2,y2).

    ``bulge`` is the perpendicular offset of the control points; positive
    bulges "above" (smaller y) when ``above=True``. Used for the
    self-loop on a node and for retry-bounce arcs in the mini-strip --
    the same gesture in both places.
    """
    sign = -1 if above else 1
    # Place two control points symmetric about the chord midpoint.
    mx = (x1 + x2) / 2.0
    my = (y1 + y2) / 2.0
    dx = x2 - x1
    dy = y2 - y1
    length = math.hypot(dx, dy) or 1.0
    # Perpendicular unit vector.
    px = -dy / length
    py = dx / length
    # Two control points pulled along the chord and out by `bulge`.
    cx1 = x1 + dx * 0.25 + sign * px * bulge
    cy1 = y1 + dy * 0.25 + sign * py * bulge
    cx2 = x1 + dx * 0.75 + sign * px * bulge
    cy2 = y1 + dy * 0.75 + sign * py * bulge
    marker = ' marker-end="url(#ah-ctrl)"' if arrow and color == COLOR_CTRL \
        else (' marker-end="url(#ah-pdata)"' if arrow else "")
    return (
        f'<path d="M{x1},{y1} C{cx1},{cy1} {cx2},{cy2} {x2},{y2}" '
        f'fill="none" stroke="{color}" stroke-width="{width}" '
        f'stroke-linecap="round"{marker}/>'
    )


def puck(cx: float, cy: float, label: str = "",
         color: str = COLOR_OTAP, w: float = 28, h: float = 16,
         badge: str = "") -> str:
    """A small rounded rectangle = one in-flight request."""
    x = cx - w / 2
    y = cy - h / 2
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="4" ry="4" '
        f'fill="{color}" fill-opacity="0.85" stroke="{color}" '
        f'stroke-width="1"/>'
    ]
    if label:
        parts.append(
            f'<text x="{cx}" y="{cy + 3}" text-anchor="middle" '
            f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
            f'fill="white">{_esc(label)}</text>'
        )
    if badge:
        parts.append(
            f'<text x="{cx}" y="{y - 4}" text-anchor="middle" '
            f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
            f'fill="{COLOR_SUBLABEL}">{_esc(badge)}</text>'
        )
    return "".join(parts)


def context_chip(cx: float, cy: float, kvs: List[Tuple[str, str]],
                 width: float = 110) -> str:
    """A small attached chip showing the calldata riding on a payload."""
    pad_x = 14
    pad_y = 12
    line_h = FS_TINY + 6
    h = 2 * pad_y + line_h * len(kvs)
    x = cx - width / 2
    y = cy - h / 2
    parts = [
        f'<rect x="{x}" y="{y}" width="{width}" height="{h}" rx="3" ry="3" '
        f'fill="white" stroke="{COLOR_CTX}" stroke-width="1" '
        f'stroke-dasharray="2,2"/>'
    ]
    for i, (k, v) in enumerate(kvs):
        ty = y + pad_y + i * line_h + line_h - 5
        parts.append(
            f'<text x="{x + pad_x}" y="{ty}" font-size="{FS_TINY}" '
            f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
            f'{_esc(k)}</text>'
        )
        parts.append(
            f'<text x="{x + width - pad_x}" y="{ty}" text-anchor="end" '
            f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
            f'fill="{COLOR_LABEL}">{_esc(v)}</text>'
        )
    return "".join(parts)


def callout(anchor_x: float, anchor_y: float,
            text_x: float, text_y: float,
            text: str,
            anchor_align: str = "start",
            multiline: Optional[List[str]] = None) -> str:
    """A short leader line + a label.

    ``multiline`` overrides ``text`` if provided.
    """
    lines = multiline if multiline else [text]
    line_h = 14
    parts = [
        f'<line x1="{anchor_x}" y1="{anchor_y}" x2="{text_x}" y2="{text_y}" '
        f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>',
        f'<circle cx="{anchor_x}" cy="{anchor_y}" r="2.4" '
        f'fill="{COLOR_CTRL_SOFT}"/>',
    ]
    for i, ln in enumerate(lines):
        parts.append(
            f'<text x="{text_x}" y="{text_y + i * line_h}" '
            f'text-anchor="{anchor_align}" font-size="{FS_LABEL}" '
            f'fill="{COLOR_LABEL}">{_esc(ln)}</text>'
        )
    return "".join(parts)


def mini_panel(x: float, y: float, w: float, h: float,
               title: str, subtitle: str = "") -> str:
    """The framed background for a single per-case mini cartoon."""
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" ry="6" '
        f'fill="white" stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>',
        f'<text x="{x + w/2}" y="{y + h + 16}" text-anchor="middle" '
        f'font-size="{FS_LABEL}" font-weight="600" '
        f'fill="{COLOR_LABEL}">{_esc(title)}</text>',
    ]
    if subtitle:
        parts.append(
            f'<text x="{x + w/2}" y="{y + h + 30}" text-anchor="middle" '
            f'font-size="{FS_TINY}" fill="{COLOR_SUBLABEL}">'
            f'{_esc(subtitle)}</text>'
        )
    return "".join(parts)


def glyph_check(x: float, y: float, color: str = COLOR_OK,
                size: float = 12) -> str:
    """A small \u2713 success glyph centered on (x,y)."""
    return (
        f'<text x="{x}" y="{y + size/3}" text-anchor="middle" '
        f'font-size="{size}" font-weight="700" fill="{color}">\u2713</text>'
    )


def glyph_cross(x: float, y: float, color: str = COLOR_FAIL,
                size: float = 12) -> str:
    """A small \u2717 failure glyph centered on (x,y)."""
    return (
        f'<text x="{x}" y="{y + size/3}" text-anchor="middle" '
        f'font-size="{size}" font-weight="700" fill="{color}">\u2717</text>'
    )


def signal_chip(x: float, y: float, label: str, color: str,
                w: float = 64, h: float = 22) -> str:
    """Filled rounded rect with inverse white text identifying a pdata
    format on a port. Anchored at top-left (x, y)."""
    return (
        f'<g>'
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="5" ry="5" '
        f'fill="{color}" stroke="{color}"/>'
        f'<text x="{x + w/2}" y="{y + h/2 + 5}" text-anchor="middle" '
        f'font-size="{FS_TINY}" font-weight="700" '
        f'font-family="{FONT_MONO}" fill="white">{label}</text>'
        f'</g>'
    )


def node_name_label(cx: float, y: float, name: str) -> str:
    """Bold black node name above its box (mirrors the slide title)."""
    return (
        f'<text x="{cx}" y="{y}" text-anchor="middle" '
        f'font-size="{FS_NODE}" font-weight="700" '
        f'fill="{COLOR_LABEL}">{_esc(name)}</text>'
    )


def mono_list(x: float, y: float, items: List[str],
              size: Optional[float] = None,
              line_h: Optional[float] = None,
              weight: int = 700) -> str:
    """Left-justified monospace list, one item per line, at (x, y).

    Used for any "list of identifiers" rendered on a per-node slide:
    sorted effect-handler function names, controller interaction
    keywords, etc. Pure rendering -- caller handles sorting and any
    name munging.
    """
    fs = size if size is not None else FS_LABEL
    lh = line_h if line_h is not None else fs + 4
    parts = []
    for i, item in enumerate(items):
        parts.append(
            f'<text x="{x}" y="{y + i * lh}" font-size="{fs}" '
            f'font-weight="{weight}" font-family="{FONT_MONO}" '
            f'fill="{COLOR_LABEL}">{_esc(item)}</text>'
        )
    return "".join(parts)


def effect_list(x: float, y: float, names: List[str],
                size: Optional[float] = None,
                line_h: Optional[float] = None) -> str:
    """Mono list of effect-handler function names. Sorts and strips
    the cosmetic ``_with_source_node`` suffix for display."""
    cleaned = sorted(n.replace("_with_source_node", "") for n in names)
    return mono_list(x, y, cleaned, size=size, line_h=line_h)


def controller_list(x: float, y: float, items: List[str],
                    size: Optional[float] = None,
                    line_h: Optional[float] = None) -> str:
    """Mono list of pipeline-controller interaction keywords (e.g.
    ``metrics``, ``reconfig``, ``shutdown``, ``timer``). Sorted."""
    return mono_list(x, y, sorted(items), size=size, line_h=line_h)


def _arrow_pair(x: float, y_top: float, y_bot: float,
                draw_up: bool, draw_down: bool,
                style: str = "ctrl",
                signal: Optional[str] = None,
                pair_dx: float = 22) -> str:
    """Internal: render an up/down arrow pair in either control style
    (thin grey) or pdata style (thick colored)."""
    parts = []
    up_x   = x + pair_dx / 2
    down_x = x + pair_dx * 1.5
    if style == "pdata":
        if draw_up:
            parts.append(pdata_edge(up_x, y_bot, up_x, y_top,
                                    signal=signal or "otap",
                                    arrow_at="end"))
        if draw_down:
            parts.append(pdata_edge(down_x, y_top, down_x, y_bot,
                                    signal=signal or "otap",
                                    arrow_at="end"))
    else:
        if draw_up:
            parts.append(ctrl_edge(up_x, y_bot, up_x, y_top,
                                   arrow_at="end"))
        if draw_down:
            parts.append(ctrl_edge(down_x, y_top, down_x, y_bot,
                                   arrow_at="end"))
    return "".join(parts)


def control_column(x: float, y_top: float, msgs: List[str],
                   label: str = "control",
                   arrow_len: float = 26,
                   list_size: float = 17,
                   list_line_h: float = 22) -> str:
    """Below-box column for the node's control-plane interactions.

    Renders an up/down ``ctrl_edge`` arrow pair (always both, since
    control messages flow both ways between the node and the
    pipeline controller), an italic label underneath, and a sorted
    monospace list of the ``NodeControlMsg`` variant names the node
    handles with a non-empty body. ``Ack`` and ``Nack`` are
    universal/automatic and should not be passed in.
    """
    y_bot = y_top + arrow_len
    parts = [_arrow_pair(x, y_top, y_bot, True, True, style="ctrl")]
    parts.append(
        f'<text x="{x}" y="{y_bot + 18}" font-size="14" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">{_esc(label)}</text>'
    )
    list_y = y_bot + 18 + list_line_h
    parts.append(controller_list(x, list_y, msgs,
                                 size=list_size, line_h=list_line_h))
    return "".join(parts)


def ack_column(x: float, y_top: float, role: str,
               label: str = "ack/nack",
               arrow_len: float = 26,
               list_size: float = 17,
               list_line_h: float = 22) -> str:
    """Below-box column for ack/nack flow.

    Arrows AND text entries vary by node role; both encode the same
    information for redundancy:

    - ``"processor"`` -> arrows ``\u2191`` and ``\u2193``,
                         entries ``["recv", "send"]``
    - ``"receiver"``  -> arrow  ``\u2191`` only,
                         entries ``["recv"]``
    - ``"exporter"``  -> arrow  ``\u2193`` only,
                         entries ``["send"]``

    Arrows are drawn in ``ctrl_edge`` style (thin grey), matching the
    horizontal ack/nack edges on the box. ``\u2191`` = receive
    (ack/nack arriving from downstream), ``\u2193`` = send (ack/nack
    propagating to upstream).
    """
    role = role.lower()
    if role == "processor":
        draw_up, draw_down = True, True
        entries = ["Receive", "Send"]
    elif role == "receiver":
        draw_up, draw_down = True, False
        entries = ["Receive"]
    elif role == "exporter":
        draw_up, draw_down = False, True
        entries = ["Send"]
    else:
        raise ValueError(f"unknown role: {role!r}")
    y_bot = y_top + arrow_len
    parts = [_arrow_pair(x, y_top, y_bot, draw_up, draw_down,
                         style="ctrl")]
    parts.append(
        f'<text x="{x}" y="{y_bot + 18}" font-size="14" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">{_esc(label)}</text>'
    )
    list_y = y_bot + 18 + list_line_h
    parts.append(mono_list(x, list_y, entries,
                           size=list_size, line_h=list_line_h))
    return "".join(parts)


def data_column(x: float, y_top: float, role: str,
                signal: str = "otap",
                label: str = "pdata",
                arrow_len: float = 26) -> str:
    """Below-box column for the node's data-plane direction(s).

    The arrow set varies by role:

    - ``"processor"`` -> up AND down (consumes and emits pdata)
    - ``"receiver"``  -> down only (only emits pdata)
    - ``"exporter"``  -> up only (only consumes pdata)

    Arrows are drawn in ``pdata_edge`` style (thick, colored by the
    primary signal), so the data column reads visually distinct from
    the control column on its left.
    """
    role = role.lower()
    if role == "processor":
        draw_up, draw_down = True, True
    elif role == "receiver":
        draw_up, draw_down = False, True
    elif role == "exporter":
        draw_up, draw_down = True, False
    else:
        raise ValueError(f"unknown role: {role!r}")
    y_bot = y_top + arrow_len
    parts = [_arrow_pair(x, y_top, y_bot, draw_up, draw_down,
                         style="pdata", signal=signal)]
    parts.append(
        f'<text x="{x}" y="{y_bot + 18}" font-size="14" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">{_esc(label)}</text>'
    )
    return "".join(parts)


def state_hint(x: float, y: float, lines: List[str],
               size: Optional[float] = None,
               line_h: Optional[float] = None) -> str:
    """Multi-line state characterization, top-left inside the box.

    Each line is one short phrase identifying *one* state holder
    (e.g. ``"local timer wheel"``, ``"inbound ack/nack slots"``).
    Lines are rendered upright (non-italic), in the primary label
    color, with sentence-case capitalization applied automatically;
    any parenthetical clauses in the source phrase are stripped so
    the column reads as clean labels rather than prose.

    The list is the slide's structured answer to "where does this
    node's operational state live?".
    """
    fs = size if size is not None else FS_NODE_SUB + 4
    lh = line_h if line_h is not None else fs + 6
    parts = []
    for i, raw in enumerate(lines):
        # Strip parenthetical clauses defensively, then sentence-case.
        cleaned = re.sub(r"\s*\([^)]*\)", "", raw).strip()
        if cleaned:
            cleaned = cleaned[0].upper() + cleaned[1:]
        parts.append(
            f'<text x="{x}" y="{y + i * lh}" font-size="{fs}" '
            f'fill="{COLOR_LABEL}">'
            f'{_esc(cleaned)}</text>'
        )
    return "".join(parts)


def port_row_labels(x_left: float, y_pdata: float, y_ctrl: float,
                    pdata_text: str = "PData",
                    ctrl_text: str = "Ack/Nack",
                    above_offset: float = 8,
                    size: float = 14) -> str:
    """Fine-print labels for the standard pair of port rows.

    Left-justified to ``x_left`` (typically the page margin), sitting
    just above each port line. Used on every per-node slide so that a
    viewer can disambiguate the upper (pdata) and lower (ack/nack)
    rows at a glance. Calling this from one place means a later
    relabel touches all slides.
    """
    return (
        f'<text x="{x_left}" y="{y_pdata - above_offset}" '
        f'text-anchor="start" font-size="{size}" '
        f'fill="{COLOR_LABEL}">{_esc(pdata_text)}</text>'
        f'<text x="{x_left}" y="{y_ctrl - above_offset}" '
        f'text-anchor="start" font-size="{size}" '
        f'fill="{COLOR_LABEL}">{_esc(ctrl_text)}</text>'
    )


def notes_box(x: float, y: float, w: float, h: float,
              items: List[str],
              pad: float = 24,
              number_w: float = 26,
              line_gap: float = 8,
              size: Optional[float] = None) -> str:
    """A lightly-shaded rounded box holding a numbered list of operator
    notes. Used in the lower-right corner of every per-node slide.

    Items are auto-wrapped using a rough monospace-agnostic
    character-width estimate. Numbers are rendered in mono so they
    align in a fixed-width gutter on the left.
    """
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" ry="10" '
        f'fill="#f4f6f8" stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>'
    ]
    fs = size if size is not None else FS_LABEL
    line_h = fs + 4
    text_x = x + pad + number_w
    text_w = w - 2 * pad - number_w
    # Average glyph width for the prose font at this size; tuned so the
    # wrapper does not break too early.
    char_w = fs * 0.50
    max_chars = max(1, int(text_w / char_w))
    cy = y + pad + fs
    for idx, body in enumerate(items, start=1):
        # Wrap the body greedily without breaking words.
        words = body.split()
        lines: List[str] = []
        cur = ""
        for word in words:
            cand = (cur + " " + word).strip()
            if len(cand) <= max_chars or not cur:
                cur = cand
            else:
                lines.append(cur)
                cur = word
        if cur:
            lines.append(cur)
        # Number gutter (mono) on the first line of the item.
        parts.append(
            f'<text x="{x + pad}" y="{cy}" font-size="{fs}" '
            f'font-weight="700" font-family="{FONT_MONO}" '
            f'fill="{COLOR_LABEL}">{idx}.</text>'
        )
        for li, line in enumerate(lines):
            parts.append(
                f'<text x="{text_x}" y="{cy + li * line_h}" '
                f'font-size="{fs}" fill="{COLOR_LABEL}">'
                f'{_esc(line)}</text>'
            )
        cy += line_h * len(lines) + line_gap
    return "".join(parts)


# ============================================================ slide spec
#
# A per-node slide is fully described by a NodeSlideSpec: identity,
# config, internal state/effects, port behavior, calldata, control-plane
# flow, and operator notes. ``render_node_slide(spec)`` lays out a full
# SVG using the locked grammar documented in README.md ("Anatomy of a
# per-node slide"). Per-node generator scripts should declare a SPEC and
# call ``render_node_slide`` -- they should not contain layout code.

# Page constants shared by every per-node slide. Tweaks here re-flow
# the whole deck.
SLIDE_PAGE_W   = 1600
SLIDE_PAGE_H   =  850
SLIDE_MARGIN_X =   80
SLIDE_MARGIN_Y =   30

# Inner geometry constants (single source of truth for the slide grid).
_NODE_W            = 720
_NODE_H            = 320
_OVERHEAD_TOP_Y    = 170
_OVERHEAD_BOX_GAP  = 60       # space between overhead-panel top and node box top
_BOX_NAME_GAP      = 18       # gap between node-name label and box top
_INNER_TOP_PAD     = 38       # from box top to first line of inner cols
_INNER_LEFT_PAD    = 20
_INNER_LINE_H      = 26
_INNER_FS          = 20
_PORT_PDATA_FRAC   = 1 / 3    # vertical placement of pdata row inside box
_PORT_CTRL_FRAC    = 11 / 15  # vertical placement of ctrl row inside box
_CHIP_W            = 76
_CHIP_H            = 28
_CHIP_GAP          = 10
_CHIP_OFFSET_X     = 10
_CHIP_OFFSET_Y     = 6
_CTX_CHIP_W        = 220
_CTX_CHIP_PAD_Y    = 12
_CTX_CHIP_LINE_H   = 22       # FS_TINY + 6, see context_chip()
_CTRL_COL_DX       = 0        # control column at left of box
_ACK_COL_DX        = 240      # ack column offset right from box left

# Top-of-page config-listing geometry.
_CFG_TOP_Y         = 104
_CFG_LINE_H        = 22
_CFG_NAME_TYPE_GAP = 200      # px between right-justified name and right-justified type

# Notes-box geometry (lower portion of the slide). Wider and taller
# than the original placement so longer operator notes wrap cleanly.
_NOTES_X           = 820
_NOTES_Y           = 640


@dataclass
class NodeSlideSpec:
    """Declarative description of a per-node slide.

    Renderable by :func:`render_node_slide`. Fields that vary by node:

    - ``name`` / ``urn`` / ``subtitle`` -- title-bar identity.
    - ``config_fields`` -- ``[(field_name, rust_type), ...]`` for the
      upper-right config listing.
    - ``state`` -- left-column lines inside the node box.
    - ``effects`` -- right-column effect-handler names inside the box,
      sorted alphabetically by the caller.
    - ``role`` -- ``"processor"`` | ``"receiver"`` | ``"exporter"``;
      drives the ack-column arrows / list.
    - ``output_formats`` / ``input_formats`` -- subsets of
      ``["OTAP", "OTLP"]``. Pass-through processors typically set only
      ``output_formats``; converters set both; exporters set only
      ``input_formats``.
    - ``calldata`` -- ``[(name, rust_type), ...]`` attached via
      ``effect_handler.subscribe_to(...)`` on outgoing pdata. Empty =
      no chip.
    - ``control_msgs`` -- ``NodeControlMsg`` variants the node handles
      with a meaningful body. Skip ``Ack``/``Nack`` (universal) and
      anything that falls through to ``Ok(())`` / ``unreachable!``.
    - ``notes`` -- short factual operator notes for the lower-right
      box.
    - ``signal`` -- primary signal color of the box; defaults to OTAP.
    """

    name: str
    urn: str
    subtitle: str
    config_fields: List[Tuple[str, str]]
    state: List[str]
    effects: List[str]
    role: str
    output_formats: List[str]
    input_formats:  List[str] = None        # type: ignore[assignment]
    calldata:       List[Tuple[str, str]] = None  # type: ignore[assignment]
    control_msgs:   List[str] = None        # type: ignore[assignment]
    notes:          List[str] = None        # type: ignore[assignment]
    signal:         str = "otap"

    def __post_init__(self) -> None:
        if self.input_formats is None:
            self.input_formats = []
        if self.calldata is None:
            self.calldata = []
        if self.control_msgs is None:
            self.control_msgs = []
        if self.notes is None:
            self.notes = []


# ---- color resolver for chip rows ---------------------------------------

_FORMAT_COLORS = {
    "OTAP": COLOR_OTAP,
    "OTLP": COLOR_OTLP,
}


def _format_chip_row(formats: List[str], anchor_x: float, chip_y: float,
                     anchor: str) -> str:
    """Render a horizontal row of OTAP/OTLP chips.

    ``anchor`` is ``"left"`` (chips grow rightward from anchor_x) or
    ``"right"`` (chips end at anchor_x, growing leftward).
    """
    parts: List[str] = []
    if anchor == "left":
        cx = anchor_x
        for label in formats:
            parts.append(signal_chip(cx, chip_y, label,
                                     _FORMAT_COLORS[label],
                                     w=_CHIP_W, h=_CHIP_H))
            cx += _CHIP_W + _CHIP_GAP
    else:
        # right anchor: lay out then shift
        total_w = (len(formats) * _CHIP_W
                   + max(0, len(formats) - 1) * _CHIP_GAP)
        cx = anchor_x - total_w
        for label in formats:
            parts.append(signal_chip(cx, chip_y, label,
                                     _FORMAT_COLORS[label],
                                     w=_CHIP_W, h=_CHIP_H))
            cx += _CHIP_W + _CHIP_GAP
    return "".join(parts)


# ---- main entry point ---------------------------------------------------

def render_node_slide(spec: NodeSlideSpec) -> str:
    """Render a complete per-node slide SVG from ``spec``.

    The layout follows the locked grammar documented in
    ``diagram/README.md`` ("Anatomy of a per-node slide"). All page-,
    box-, and chip-level geometry constants live in this module so
    visual tweaks happen in one place.
    """
    PAGE_W = SLIDE_PAGE_W
    PAGE_H = SLIDE_PAGE_H
    MARGIN_X = SLIDE_MARGIN_X
    MARGIN_Y = SLIDE_MARGIN_Y

    parts = [page_open(PAGE_W, PAGE_H), arrow_marker_defs()]

    # --- title bar + subtitle ----------------------------------------
    parts.append(title_bar(
        x=MARGIN_X, y=80, w=PAGE_W - 2 * MARGIN_X,
        title=spec.name,
        urn=spec.urn,
        accent=color_for_signal(spec.signal),
    ))
    parts.append(
        f'<text x="{MARGIN_X}" y="{120}" font-size="24" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
    )
    # Wrap the subtitle to the same width as the notes box in the
    # lower-right corner so it never collides with the upper-right
    # config-field listing.
    sub_fs = 24
    sub_line_h = sub_fs + 4
    sub_max_w = (PAGE_W - MARGIN_X) - _NOTES_X
    sub_char_w = sub_fs * 0.50
    sub_max_chars = max(1, int(sub_max_w / sub_char_w))
    sub_words = spec.subtitle.split()
    sub_lines: List[str] = []
    cur = ""
    for word in sub_words:
        cand = (cur + " " + word).strip()
        if len(cand) <= sub_max_chars or not cur:
            cur = cand
        else:
            sub_lines.append(cur)
            cur = word
    if cur:
        sub_lines.append(cur)
    for li, line in enumerate(sub_lines):
        dy = "0" if li == 0 else f"{sub_line_h}"
        parts.append(
            f'<tspan x="{MARGIN_X}" dy="{dy}">{_esc(line)}</tspan>'
        )
    parts.append("</text>")

    # --- upper-right config field listing ----------------------------
    type_x = PAGE_W - MARGIN_X
    name_x = type_x - _CFG_NAME_TYPE_GAP
    for i, (cname, ctype) in enumerate(spec.config_fields):
        y = _CFG_TOP_Y + i * _CFG_LINE_H
        parts.append(
            f'<text x="{name_x}" y="{y}" '
            f'text-anchor="end" font-size="17" font-weight="700" '
            f'font-family="{FONT_MONO}" fill="{COLOR_LABEL}">'
            f'{_esc(cname)}</text>'
        )
        parts.append(
            f'<text x="{type_x}" y="{y}" '
            f'text-anchor="end" font-size="17" '
            f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
            f'{_esc(ctype)}</text>'
        )

    # --- overhead panel ---------------------------------------------
    parts.append(_render_overhead_panel(spec, PAGE_W, MARGIN_X))

    # --- notes (lower-right) ----------------------------------------
    notes_w = (PAGE_W - MARGIN_X) - _NOTES_X
    notes_h = (PAGE_H - MARGIN_Y) - _NOTES_Y
    parts.append(notes_box(
        _NOTES_X, _NOTES_Y, notes_w, notes_h,
        size=16,
        items=spec.notes,
    ))

    parts.append(page_close())
    return "".join(parts)


def _render_overhead_panel(spec: NodeSlideSpec,
                           PAGE_W: int, MARGIN_X: int) -> str:
    """Draw the central node box plus its in/out wiring."""
    parts: List[str] = []

    panel_x = MARGIN_X
    panel_w = PAGE_W - 2 * MARGIN_X

    nx = panel_x + (panel_w - _NODE_W) / 2
    ny = _OVERHEAD_TOP_Y + _OVERHEAD_BOX_GAP + int(_NODE_H * 0.15)
    cx = nx + _NODE_W / 2

    # Node name above the box.
    parts.append(node_name_label(cx, ny - _BOX_NAME_GAP, spec.name))

    # The node itself.
    parts.append(node_box(nx, ny, _NODE_W, _NODE_H,
                          title="", sub="", signal=spec.signal))

    # Inside-box content: state (left) + effect-handler list (right).
    inner_top_y = ny + _INNER_TOP_PAD
    left_col_x  = nx + _INNER_LEFT_PAD
    right_col_x = nx + _NODE_W / 2 + 10
    parts.append(state_hint(left_col_x, inner_top_y, spec.state,
                            size=_INNER_FS, line_h=_INNER_LINE_H))
    parts.append(effect_list(right_col_x, inner_top_y, spec.effects,
                             size=_INNER_FS, line_h=_INNER_LINE_H))

    # Port rows (pdata upper, ack/nack lower).
    in_y_pdata = ny + _NODE_H * _PORT_PDATA_FRAC
    in_y_ctrl  = ny + _NODE_H * _PORT_CTRL_FRAC

    upstream_x   = panel_x
    downstream_x = panel_x + panel_w

    parts.append(pdata_edge(upstream_x, in_y_pdata, nx, in_y_pdata,
                            signal=spec.signal))
    parts.append(pdata_edge(nx + _NODE_W, in_y_pdata, downstream_x,
                            in_y_pdata, signal=spec.signal))

    # Format chips. Output side anchored just right of the box; input
    # side (if any -- only set for converters/exporters) anchored just
    # left of the box.
    chip_y = in_y_pdata - _CHIP_H - _CHIP_OFFSET_Y
    if spec.output_formats:
        parts.append(_format_chip_row(
            spec.output_formats,
            anchor_x=nx + _NODE_W + _CHIP_OFFSET_X,
            chip_y=chip_y, anchor="left"))
    if spec.input_formats:
        parts.append(_format_chip_row(
            spec.input_formats,
            anchor_x=nx - _CHIP_OFFSET_X,
            chip_y=chip_y, anchor="right"))

    # ack/nack edges (thin grey).
    parts.append(ctrl_edge(downstream_x, in_y_ctrl, nx + _NODE_W,
                           in_y_ctrl, arrow_at="end"))
    parts.append(ctrl_edge(nx, in_y_ctrl, upstream_x, in_y_ctrl,
                           arrow_at="end"))

    # Fine-print row labels above each port row, page-margin-anchored.
    parts.append(port_row_labels(
        x_left=MARGIN_X,
        y_pdata=in_y_pdata,
        y_ctrl=in_y_ctrl,
    ))

    # Calldata chip: floats above the outgoing pdata edge with its
    # right edge aligned to the page margin.
    if spec.calldata:
        ctx_w = _CTX_CHIP_W
        chip_cx = (PAGE_W - MARGIN_X) - ctx_w / 2
        ctx_h = 2 * _CTX_CHIP_PAD_Y + _CTX_CHIP_LINE_H * len(spec.calldata)
        # Anchor the chip's bottom edge a clear gap above the format
        # chips so they do not touch.
        chip_bottom = chip_y - 28
        chip_cy = chip_bottom - ctx_h / 2
        parts.append(context_chip(
            cx=chip_cx, cy=chip_cy,
            kvs=spec.calldata,
            width=ctx_w,
        ))
        # Dotted leader from chip down to outgoing pdata edge.
        parts.append(
            f'<line x1="{chip_cx}" y1="{chip_cy + ctx_h / 2}" '
            f'x2="{chip_cx}" y2="{in_y_pdata - 4}" '
            f'stroke="{COLOR_CTX}" stroke-width="1" '
            f'stroke-dasharray="2,2"/>'
        )

    # Below-box columns: control list (left) + ack column (right).
    below_y_top = ny + _NODE_H + 8
    parts.append(control_column(
        x=nx + _CTRL_COL_DX, y_top=below_y_top,
        msgs=spec.control_msgs,
    ))
    parts.append(ack_column(
        x=nx + _ACK_COL_DX, y_top=below_y_top,
        role=spec.role,
    ))

    return "".join(parts)
