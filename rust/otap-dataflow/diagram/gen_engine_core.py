#!/usr/bin/env python3
"""Engine architecture slide #2 -- one core's *pipeline thread*.

Zoom-in to a single core: the OS thread, its single-threaded tokio
runtime + LocalSet, the in-thread actors (RuntimeCtrlMsgManager,
PipelineCompletionMsgDispatcher), a representative receiver -> processor
-> exporter node chain, and the per-core view of the cross-core
TopicBroker.

The visual emphasis matches the group slide: the entire box is one OS
thread driving one tokio current_thread runtime. Boundary stubs leave
each side of the thread box and are labelled with the channel type
that crosses the boundary.

Source of truth (cited file:line):
- ``RuntimePipeline`` -- crates/engine/src/runtime_pipeline.rs:104
  (run loop at :186, tokio current_thread at :215, LocalSet at :219,
  block_on/run_until at :495-534)
- ``RuntimeCtrlMsgManager`` -- crates/engine/src/pipeline_ctrl.rs:242
- ``PipelineCompletionMsgDispatcher`` -- crates/engine/src/pipeline_ctrl.rs:888
- ``NodeLocalScheduler`` -- crates/engine/src/node_local_scheduler.rs:182
- node task wrappers -- crates/engine/src/runtime_pipeline.rs:280, 354, 406
- ``TopicSet`` per-core view -- crates/engine/src/topic/topic_set.rs:15
- thread spawn / affinity -- crates/controller/src/lib.rs:1834, 2020

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
    notes_box, ctrl_edge, pdata_edge,
    _esc,
)
from gen_diagram import COLOR_BG, COLOR_LABEL, COLOR_SUBLABEL, FONT, FONT_MONO


# --------------------------------------------------------------- layout

PAGE_W = SLIDE_PAGE_W
PAGE_H = SLIDE_PAGE_H

TITLE_X = SLIDE_MARGIN_X
TITLE_Y = 60
SUBTITLE_Y = 90

# Big outer "thread" box. Width chosen to leave room on each side for
# labelled cross-thread stubs (~210 px per side).
THREAD_X = 220
THREAD_Y = 130
THREAD_W = PAGE_W - 2 * THREAD_X
THREAD_H = 510
THREAD_BANNER_H = 36
STUB_LEN = 180
STUB_FS  = 12

# In-thread actor boxes (top row).
ACTOR_TOP_Y    = THREAD_Y + THREAD_BANNER_H + 24
ACTOR_H        = 150
RTC_X          = THREAD_X + 28
RTC_W          = 360
DISP_X         = THREAD_X + THREAD_W - 28 - 360
DISP_W         = 360

# Node chain (middle row).
NODE_Y         = ACTOR_TOP_Y + ACTOR_H + 60
NODE_H         = 110
NODE_W         = 220
NODE_GAP       = 90
NODE_ROW_X0    = THREAD_X + (THREAD_W - (3 * NODE_W + 2 * NODE_GAP)) / 2

# Topic tile (bottom inside the thread box).
TOPIC_X = THREAD_X + 28
TOPIC_Y = THREAD_Y + THREAD_H - 64
TOPIC_W = THREAD_W - 56
TOPIC_H = 36

# Notes box: lower-right outside the thread box would push off page;
# instead we place it under the thread box, full width.
NOTES_X = SLIDE_MARGIN_X
NOTES_Y = THREAD_Y + THREAD_H + 18
NOTES_W = PAGE_W - 2 * SLIDE_MARGIN_X
NOTES_H = PAGE_H - NOTES_Y - 20


# ---------------------------------------------------------- primitives

def _thread_box(out: List[str]) -> None:
    """The big outer rectangle = one OS thread + one tokio current_thread."""
    out.append(
        f'<rect x="{THREAD_X}" y="{THREAD_Y}" width="{THREAD_W}" '
        f'height="{THREAD_H}" rx="14" ry="14" fill="white" '
        f'stroke="{COLOR_OTAP}" stroke-width="{W_FRAME + 0.4}"/>'
    )
    # Tinted top banner spelling out the thread-per-core invariant.
    out.append(
        f'<rect x="{THREAD_X}" y="{THREAD_Y}" width="{THREAD_W}" '
        f'height="{THREAD_BANNER_H}" rx="14" ry="14" '
        f'fill="{COLOR_OTAP}" fill-opacity="0.10"/>'
    )
    out.append(
        f'<rect x="{THREAD_X}" y="{THREAD_Y + THREAD_BANNER_H - 4}" '
        f'width="{THREAD_W}" height="4" fill="{COLOR_OTAP}"/>'
    )
    out.append(
        f'<text x="{THREAD_X + 20}" y="{THREAD_Y + 24}" '
        f'font-size="{FS_LABEL}" font-weight="700" '
        f'fill="{COLOR_OTAP}">'
        f'pipeline thread (one core)'
        f'</text>'
    )
    out.append(
        f'<text x="{THREAD_X + THREAD_W - 20}" y="{THREAD_Y + 24}" '
        f'text-anchor="end" font-size="{FS_LABEL}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">'
        f'RuntimePipeline&lt;PData&gt;'
        f'</text>'
    )
    out.append(
        f'<text x="{THREAD_X + THREAD_W/2}" y="{THREAD_Y + 24}" '
        f'text-anchor="middle" font-size="{FS_LABEL}" '
        f'font-style="italic" fill="{COLOR_OTAP}">'
        f'1 OS thread \u00b7 1 tokio current_thread runtime \u00b7 LocalSet (!Send tasks)'
        f'</text>'
    )


def _actor_box(x: float, y: float, w: float, h: float,
               title: str, struct_name: str,
               lines: List[str]) -> str:
    """A medium-weight box for an in-thread actor task."""
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" ry="10" '
        f'fill="white" stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>',
        f'<rect x="{x}" y="{y}" width="{w}" height="6" rx="3" ry="3" '
        f'fill="{COLOR_CTRL}"/>',
        f'<text x="{x + 16}" y="{y + 30}" font-size="{FS_NODE_SUB + 2}" '
        f'font-weight="700" fill="{COLOR_LABEL}">{_esc(title)}</text>',
        # Citation on its own line under the title (avoids colliding
        # with long actor names like PipelineCompletionMsgDispatcher).
        f'<text x="{x + 16}" y="{y + 48}" font-size="{FS_TINY - 2}" '
        f'font-family="{FONT_MONO}" fill="{COLOR_SUBLABEL}">'
        f'{_esc(struct_name)}</text>',
    ]
    line_h = FS_TINY + 4
    ly = y + 70
    for ln in lines:
        parts.append(
            f'<text x="{x + 16}" y="{ly}" font-size="{FS_TINY}" '
            f'font-family="{FONT_MONO}" fill="{COLOR_LABEL}">'
            f'{_esc(ln)}</text>'
        )
        ly += line_h
    return "".join(parts)


def _node_box(x: float, y: float, w: float, h: float,
              role: str, kind: str,
              struct_name: str,
              extra: str = "") -> str:
    """A representative node task in the chain."""
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" ry="10" '
        f'fill="white" stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>',
        f'<rect x="{x}" y="{y}" width="{w}" height="6" rx="3" ry="3" '
        f'fill="{COLOR_OTAP}"/>',
        f'<text x="{x + w/2}" y="{y + 30}" text-anchor="middle" '
        f'font-size="{FS_NODE_SUB + 2}" font-weight="700" '
        f'fill="{COLOR_LABEL}">{_esc(role)}</text>',
        f'<text x="{x + w/2}" y="{y + 50}" text-anchor="middle" '
        f'font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">{_esc(kind)}</text>',
        f'<text x="{x + w/2}" y="{y + 74}" text-anchor="middle" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">{_esc(struct_name)}</text>',
    ]
    if extra:
        parts.append(
            f'<text x="{x + w/2}" y="{y + 96}" text-anchor="middle" '
            f'font-size="{FS_TINY}" font-style="italic" '
            f'fill="{COLOR_OTAP}">{_esc(extra)}</text>'
        )
    return "".join(parts)


def _shared_tile(out: List[str]) -> None:
    out.append(
        f'<rect x="{TOPIC_X}" y="{TOPIC_Y}" width="{TOPIC_W}" '
        f'height="{TOPIC_H}" rx="6" ry="6" fill="#f4f6f8" '
        f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>'
    )
    out.append(
        f'<text x="{TOPIC_X + 14}" y="{TOPIC_Y + TOPIC_H/2 + 5}" '
        f'font-size="{FS_LABEL}" font-family="{FONT_MONO}" '
        f'font-weight="600" fill="{COLOR_LABEL}">'
        f'TopicSet&lt;PData&gt;</text>'
    )
    out.append(
        f'<text x="{TOPIC_X + TOPIC_W - 14}" y="{TOPIC_Y + TOPIC_H/2 + 5}" '
        f'text-anchor="end" font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'per-core view of the controller-owned TopicBroker '
        f'(only cross-core path)'
        f'</text>'
    )


# ---------------------------------------------------------------- render

def _render_subtitle(out: List[str]) -> None:
    out.append(
        f'<text x="{TITLE_X}" y="{SUBTITLE_Y}" font-size="{FS_SUBTITLE}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'Inside one core: every box below runs on the same OS thread '
        f'and the same single-threaded tokio runtime.'
        f'</text>'
    )


def _render_actors(out: List[str]) -> Tuple[float, float]:
    """Draw the two actor boxes; return their bottom ports' x coords."""
    out.append(_actor_box(
        RTC_X, ACTOR_TOP_Y, RTC_W, ACTOR_H,
        title="RuntimeCtrlMsgManager",
        struct_name="pipeline_ctrl.rs:242",
        lines=[
            "local timer wheel  (DelayedData)",
            "tick_timers / telemetry_timers",
            "memory_pressure rx (watch)",
            "drains RuntimeCtrlMsgReceiver",
        ],
    ))
    out.append(_actor_box(
        DISP_X, ACTOR_TOP_Y, DISP_W, ACTOR_H,
        title="PipelineCompletionMsgDispatcher",
        struct_name="pipeline_ctrl.rs:888",
        lines=[
            "routes Ack / Nack back to upstream",
            "looks up node by id in ControlSenders",
            "drains PipelineCompletionMsgReceiver",
        ],
    ))
    return RTC_X + RTC_W / 2, DISP_X + DISP_W / 2


def _render_node_chain(out: List[str]) -> List[Tuple[float, float, float, float]]:
    """Draw three nodes in a row; return their bounding rects."""
    rects: List[Tuple[float, float, float, float]] = []
    nodes = [
        ("receiver",  "ingests external requests",
         "ReceiverWrapper", ""),
        ("processor", "transforms / filters",
         "ProcessorWrapper",
         "+ NodeLocalScheduler (delayed resume)"),
        ("exporter",  "delivers to external sink",
         "ExporterWrapper", ""),
    ]
    x = NODE_ROW_X0
    for role, kind, struct, extra in nodes:
        out.append(_node_box(x, NODE_Y, NODE_W, NODE_H,
                             role, kind, struct, extra))
        rects.append((x, NODE_Y, NODE_W, NODE_H))
        x += NODE_W + NODE_GAP
    return rects


def _render_in_thread_edges(out: List[str],
                            actor_centers: Tuple[float, float],
                            node_rects: List[Tuple[float, float, float, float]]
                            ) -> None:
    rtc_cx, disp_cx = actor_centers
    rtc_bottom_y = ACTOR_TOP_Y + ACTOR_H
    disp_bottom_y = rtc_bottom_y

    # 1. pdata flow along the chain (thick OTAP-blue).
    for i in range(len(node_rects) - 1):
        x1, y1, w1, h1 = node_rects[i]
        x2, y2, w2, h2 = node_rects[i + 1]
        ey = y1 + h1 / 2 - 8
        out.append(pdata_edge(x1 + w1, ey, x2, ey, signal="otap"))

    # 2. ack/nack rail running BELOW the chain (so it does not overdraw
    #    the node boxes).
    first = node_rects[0]
    last = node_rects[-1]
    rail_y = first[1] + first[3] + 28
    out.append(ctrl_edge(last[0] + last[2] - 8, rail_y,
                         first[0] + 8, rail_y,
                         dashed=False, arrow_at="end"))
    out.append(
        f'<text x="{(first[0] + last[0] + last[2]) / 2}" y="{rail_y - 6}" '
        f'text-anchor="middle" font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'ack / nack rail (control plane)'
        f'</text>'
    )

    # 3. NodeControlMsg fan-out from RuntimeCtrlMsgManager to each node.
    for x, y, w, h in node_rects:
        out.append(ctrl_edge(rtc_cx, rtc_bottom_y,
                             x + w / 2 - 16, y, dashed=False))
    out.append(
        f'<text x="{rtc_cx + 6}" y="{rtc_bottom_y + 18}" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">NodeControlMsg</text>'
    )

    # 4. completion fan-in from each node to the dispatcher.
    for x, y, w, h in node_rects:
        out.append(ctrl_edge(x + w / 2 + 16, y,
                             disp_cx, disp_bottom_y, dashed=False))
    out.append(
        f'<text x="{disp_cx - 6}" y="{disp_bottom_y + 18}" '
        f'text-anchor="end" font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">PipelineCompletionMsg</text>'
    )

    # 5. nodes touch the per-core TopicSet (dotted, no arrow).
    for x, y, w, h in node_rects:
        out.append(
            f'<line x1="{x + w/2}" y1="{y + h}" '
            f'x2="{x + w/2}" y2="{TOPIC_Y}" '
            f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1" '
            f'stroke-dasharray="2,3"/>'
        )


def _render_boundary_stubs(out: List[str]) -> None:
    """Short labelled stubs leaving the thread box on its left & right edges.

    Each stub is dashed -- consistent with the cross-thread-channel
    convention from the group slide.
    """
    # Left edge: control in, completion out, exit out.
    left_x_in  = THREAD_X
    left_x_out = THREAD_X - STUB_LEN
    stub_y0 = ACTOR_TOP_Y + 30
    items_left: List[Tuple[str, str]] = [
        ("RuntimeCtrlMsgReceiver",        "in"),   # from controller
        ("PipelineCompletionMsgSender",   "out"),  # to controller
        ("note_instance_exit (Weak)",     "out"),  # to controller
    ]
    for i, (label, direction) in enumerate(items_left):
        y = stub_y0 + i * 28
        if direction == "in":
            out.append(ctrl_edge(left_x_out, y, left_x_in, y,
                                 dashed=True, arrow_at="end"))
        else:
            out.append(ctrl_edge(left_x_in, y, left_x_out, y,
                                 dashed=True, arrow_at="end"))
        out.append(
            f'<text x="{left_x_in - 6}" y="{y - 4}" text-anchor="end" '
            f'font-size="{STUB_FS}" font-family="{FONT_MONO}" '
            f'fill="{COLOR_LABEL}">{_esc(label)}</text>'
        )

    # Right edge: memory_pressure in, topics bidirectional.
    right_x_in  = THREAD_X + THREAD_W
    right_x_out = THREAD_X + THREAD_W + STUB_LEN
    items_right: List[Tuple[str, str]] = [
        ("memory_pressure (watch) rx", "in"),
        ("topics (TopicBroker)",        "both"),
    ]
    for i, (label, direction) in enumerate(items_right):
        y = stub_y0 + i * 28
        if direction == "in":
            out.append(ctrl_edge(right_x_out, y, right_x_in, y,
                                 dashed=True, arrow_at="end"))
        elif direction == "out":
            out.append(ctrl_edge(right_x_in, y, right_x_out, y,
                                 dashed=True, arrow_at="end"))
        else:
            out.append(ctrl_edge(right_x_in, y - 2, right_x_out, y - 2,
                                 dashed=True, arrow_at="end"))
            out.append(ctrl_edge(right_x_out, y + 2, right_x_in, y + 2,
                                 dashed=True, arrow_at="end"))
        out.append(
            f'<text x="{right_x_in + 6}" y="{y - 4}" '
            f'font-size="{STUB_FS}" font-family="{FONT_MONO}" '
            f'fill="{COLOR_LABEL}">{_esc(label)}</text>'
        )

    # Captions pointing back to the controller / broker world.
    out.append(
        f'<text x="{left_x_in - 6}" y="{stub_y0 + 3 * 28 + 10}" '
        f'text-anchor="end" font-size="{STUB_FS}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'\u2190 to controller process'
        f'</text>'
    )
    out.append(
        f'<text x="{right_x_in + 6}" y="{stub_y0 + 2 * 28 + 10}" '
        f'font-size="{STUB_FS}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'\u2192 controller-owned shared state'
        f'</text>'
    )


def _render_notes(out: List[str]) -> None:
    notes = [
        "Everything inside the blue box runs on a single OS thread driven by RuntimePipeline::run_forever.",
        "Node tasks use LocalSet::spawn_local because every wrapper is !Send (hence current_thread, not multi_thread).",
        "RuntimeCtrlMsgManager owns the per-node local timer wheel that backs requeue_later / Control::DelayedData.",
        "PipelineCompletionMsgDispatcher is the universal Ack/Nack router; per-node slides hide it because it is uniform.",
        "The thread reaches the outside world only through the labelled stubs and the shared TopicBroker.",
    ]
    out.append(notes_box(NOTES_X, NOTES_Y, NOTES_W, NOTES_H, notes))


def render() -> str:
    out: List[str] = []
    out.append(page_open(PAGE_W, PAGE_H))
    out.append(arrow_marker_defs())
    out.append(title_bar(
        TITLE_X, TITLE_Y, PAGE_W - 2 * SLIDE_MARGIN_X,
        title="Per-core pipeline thread",
        urn="RuntimePipeline (single-threaded tokio runtime)",
        accent=COLOR_OTAP,
    ))
    _render_subtitle(out)
    _thread_box(out)
    actor_centers = _render_actors(out)
    node_rects = _render_node_chain(out)
    _shared_tile(out)
    _render_in_thread_edges(out, actor_centers, node_rects)
    _render_boundary_stubs(out)
    _render_notes(out)
    out.append(page_close())
    return "".join(out)


def main(argv: List[str]) -> int:
    out = argv[1] if len(argv) > 1 else "engine_core.svg"
    svg = render()
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
