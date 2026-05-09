#!/usr/bin/env python3
"""Engine architecture slide #2 -- one core's *pipeline thread*.

Zoom-in to a single ``RuntimePipeline``: the OS thread, its
single-threaded tokio runtime, the in-thread actors
(``RuntimeCtrlMsgManager`` and ``PipelineCompletionMsgDispatcher``),
a small representative DAG of node tasks, and the per-core
``TopicSet`` view of the cross-core ``TopicBroker``.

Source of truth (cited file:line):
- ``RuntimePipeline``                 -- crates/engine/src/runtime_pipeline.rs:104
- ``RuntimeCtrlMsgManager``           -- crates/engine/src/pipeline_ctrl.rs:242
- ``PipelineCompletionMsgDispatcher`` -- crates/engine/src/pipeline_ctrl.rs:888
- ``NodeLocalScheduler``              -- crates/engine/src/node_local_scheduler.rs:182
- node task wrappers                  -- crates/engine/src/runtime_pipeline.rs:280, 354, 406
- ``TopicSet`` per-core view          -- crates/engine/src/topic/topic_set.rs:15

Visual grammar follows ``node_lib`` -- same palette, title bar, arrow
markers as the per-node slides.
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

# Boundary stubs are drawn ONLY on the left side of the thread box,
# all going to / coming from the controller process. This avoids the
# previous design where stubs exited both left and right, which read
# as if the thread had two different external worlds.
STUB_LEN = 200
STUB_FS  = 12

THREAD_X = SLIDE_MARGIN_X + STUB_LEN + 24
THREAD_Y = 130
THREAD_W = PAGE_W - SLIDE_MARGIN_X - THREAD_X
THREAD_H = 580
THREAD_BANNER_H = 36

# Top-row actor boxes (slim).
ACTOR_TOP_Y = THREAD_Y + THREAD_BANNER_H + 24
ACTOR_H     = 70
RTC_W       = 320
DISP_W      = 360
RTC_X       = THREAD_X + 28
DISP_X      = THREAD_X + THREAD_W - 28 - DISP_W

# Pipeline DAG region.
DAG_TOP_Y    = ACTOR_TOP_Y + ACTOR_H + 70
DAG_BOT_Y    = THREAD_Y + THREAD_H - 110
NODE_W       = 110
NODE_H       = 56

# Topic tile.
TOPIC_X = THREAD_X + 28
TOPIC_Y = THREAD_Y + THREAD_H - 56
TOPIC_W = THREAD_W - 56
TOPIC_H = 36


# ---------------------------------------------------------- primitives

def _thread_box(out: List[str]) -> None:
    """The big outer box = one OS thread + single-threaded tokio runtime."""
    out.append(
        f'<rect x="{THREAD_X}" y="{THREAD_Y}" width="{THREAD_W}" '
        f'height="{THREAD_H}" rx="14" ry="14" fill="white" '
        f'stroke="{COLOR_OTAP}" stroke-width="{W_FRAME + 0.4}"/>'
    )
    # Top banner.
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
        f'fill="{COLOR_OTAP}">RuntimePipeline</text>'
    )
    out.append(
        f'<text x="{THREAD_X + THREAD_W / 2}" y="{THREAD_Y + 24}" '
        f'text-anchor="middle" font-size="{FS_LABEL}" '
        f'font-style="italic" fill="{COLOR_OTAP}">'
        f'1 OS thread \u00b7 single-threaded tokio runtime \u00b7 LocalSet'
        f'</text>'
    )


def _slim_box(x: float, y: float, w: float, h: float,
              title: str,
              accent: str = COLOR_CTRL) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" ry="8" '
        f'fill="white" stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>'
        f'<rect x="{x}" y="{y}" width="{w}" height="5" rx="2.5" ry="2.5" '
        f'fill="{accent}"/>'
        f'<text x="{x + w/2}" y="{y + h/2 + 8}" text-anchor="middle" '
        f'font-size="{FS_NODE_SUB}" font-weight="700" '
        f'fill="{COLOR_LABEL}">{_esc(title)}</text>'
    )


def _node_box(x: float, y: float, w: float, h: float,
              role: str, kind: str = "") -> str:
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" ry="8" '
        f'fill="white" stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>',
        f'<rect x="{x}" y="{y}" width="{w}" height="5" rx="2.5" ry="2.5" '
        f'fill="{COLOR_OTAP}"/>',
        f'<text x="{x + w/2}" y="{y + 26}" text-anchor="middle" '
        f'font-size="{FS_LABEL}" font-weight="700" '
        f'fill="{COLOR_LABEL}">{_esc(role)}</text>',
    ]
    if kind:
        parts.append(
            f'<text x="{x + w/2}" y="{y + 46}" text-anchor="middle" '
            f'font-size="{FS_TINY - 1}" font-style="italic" '
            f'fill="{COLOR_SUBLABEL}">{_esc(kind)}</text>'
        )
    return "".join(parts)


def _pdata_ack_pair(out: List[str],
                    x1: float, y1: float, x2: float, y2: float,
                    sep: float = 4.0) -> None:
    """Draw the two-line connection between adjacent DAG nodes:

      - heavy OTAP-blue pdata line ABOVE the centerline (with filled
        arrowhead), forward direction
      - thin grey ack/nack line BELOW the centerline (with open
        arrowhead), backward direction

    Heavy on top, thin below -- consistent with the per-node slides
    where pdata is the upper port row and ack/nack is the lower.
    Lines stay parallel via a pure y-offset so the convention reads
    the same on horizontal and diagonal edges.
    """
    out.append(
        f'<line x1="{x1}" y1="{y1 - sep}" x2="{x2}" y2="{y2 - sep}" '
        f'stroke="{COLOR_OTAP}" stroke-width="{W_PDATA}" '
        f'stroke-linecap="round" marker-end="url(#ah-pdata)"/>'
    )
    out.append(
        f'<line x1="{x2}" y1="{y2 + sep}" x2="{x1}" y2="{y1 + sep}" '
        f'stroke="{COLOR_CTRL}" stroke-width="{W_CTRL}" '
        f'marker-end="url(#ah-ctrl)"/>'
    )


# ---------------------------------------------------------------- render

def _subtitle(out: List[str]) -> None:
    out.append(
        f'<text x="{TITLE_X}" y="{SUBTITLE_Y}" font-size="{FS_SUBTITLE}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'One pipeline. One OS thread. One tokio runtime.'
        f'</text>'
    )


def _actors(out: List[str]) -> Tuple[Tuple[float, float], Tuple[float, float]]:
    out.append(_slim_box(
        RTC_X, ACTOR_TOP_Y, RTC_W, ACTOR_H,
        title="RuntimeCtrlMsgManager",
    ))
    out.append(_slim_box(
        DISP_X, ACTOR_TOP_Y, DISP_W, ACTOR_H,
        title="PipelineCompletionMsgDispatcher",
    ))
    rtc_anchor  = (RTC_X + RTC_W / 2, ACTOR_TOP_Y + ACTOR_H)
    disp_anchor = (DISP_X + DISP_W / 2, ACTOR_TOP_Y + ACTOR_H)
    return rtc_anchor, disp_anchor


def _dag(out: List[str]) -> dict:
    """Draw a DAG with branching, geometrically centered in the
    thread box. The fanout sits early (right after the receiver) and
    each branch carries its own processor before reaching its
    exporter. Returns named anchor points for actor wiring.
    """
    cy_mid = (DAG_TOP_Y + DAG_BOT_Y) / 2

    col_spread = 600
    span = col_spread + NODE_W
    pad = (THREAD_W - span) / 2
    col0 = THREAD_X + pad + NODE_W / 2
    cols_x = [col0, col0 + 200, col0 + 410, col0 + 600]

    branch_offset = 60

    rx  = (cols_x[0], cy_mid)
    fo  = (cols_x[1], cy_mid)
    prA = (cols_x[2], cy_mid - branch_offset)
    prB = (cols_x[2], cy_mid + branch_offset)
    eA  = (cols_x[3], cy_mid - branch_offset)
    eB  = (cols_x[3], cy_mid + branch_offset)

    nodes = [
        (rx,  "receiver"),
        (fo,  "fanout"),
        (prA, "processor"),
        (prB, "processor"),
        (eA,  "exporter"),
        (eB,  "exporter"),
    ]
    for (cx, cy), role in nodes:
        out.append(_node_box(
            cx - NODE_W / 2, cy - NODE_H / 2, NODE_W, NODE_H,
            role=role,
        ))

    def edge(a, b):
        ax, ay = a
        bx, by = b
        x1 = ax + NODE_W / 2
        y1 = ay
        x2 = bx - NODE_W / 2
        y2 = by
        _pdata_ack_pair(out, x1, y1, x2, y2)

    edge(rx, fo)
    edge(fo, prA)
    edge(fo, prB)
    edge(prA, eA)
    edge(prB, eB)

    return {
        "receiver_top": (rx[0], rx[1] - NODE_H / 2),
        "fanout_top":   (fo[0], fo[1] - NODE_H / 2),
        "expA_top":     (eA[0], eA[1] - NODE_H / 2),
    }


def _actor_to_node_links(out: List[str],
                          rtc_anchor: Tuple[float, float],
                          disp_anchor: Tuple[float, float],
                          dag_anchors: dict) -> None:
    """Manager -> receiver (NodeControlMsg).
    Dispatcher <- {receiver, fanout, exporter A} (PipelineCompletionMsg).

    Channel-type labels sit *above* the actor boxes so they never
    cross the arrow paths underneath.
    """
    rcx, rcy = rtc_anchor
    rxx, rxy = dag_anchors["receiver_top"]
    out.append(
        f'<path d="M{rcx},{rcy} C{rcx},{rcy + 40} {rxx},{rxy - 40} '
        f'{rxx},{rxy}" fill="none" stroke="{COLOR_CTRL}" '
        f'stroke-width="{W_CTRL}" marker-end="url(#ah-ctrl)"/>'
    )

    # Three connections INTO the dispatcher.
    dcx, dcy = disp_anchor
    targets = [
        ("receiver_top", dag_anchors["receiver_top"]),
        ("fanout_top",   dag_anchors["fanout_top"]),
        ("expA_top",     dag_anchors["expA_top"]),
    ]
    for _name, (nx, ny) in targets:
        out.append(
            f'<path d="M{nx},{ny} C{nx},{ny - 40} {dcx},{dcy + 40} '
            f'{dcx},{dcy}" fill="none" stroke="{COLOR_CTRL}" '
            f'stroke-width="{W_CTRL}" marker-end="url(#ah-ctrl)"/>'
        )

    # Channel-type labels: positioned in the empty band ABOVE each
    # actor box, between the thread banner and the actor box itself.
    label_y = ACTOR_TOP_Y - 6
    out.append(
        f'<text x="{RTC_X + RTC_W / 2}" y="{label_y}" text-anchor="middle" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">NodeControlMsg \u2195</text>'
    )
    out.append(
        f'<text x="{DISP_X + DISP_W / 2}" y="{label_y}" text-anchor="middle" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">PipelineCompletionMsg \u2195</text>'
    )


def _topic_tile(out: List[str]) -> None:
    out.append(
        f'<rect x="{TOPIC_X}" y="{TOPIC_Y}" width="{TOPIC_W}" '
        f'height="{TOPIC_H}" rx="6" ry="6" fill="#f4f6f8" '
        f'stroke="{COLOR_CTRL_SOFT}" stroke-width="1"/>'
    )
    out.append(
        f'<text x="{TOPIC_X + 14}" y="{TOPIC_Y + TOPIC_H/2 + 5}" '
        f'font-size="{FS_LABEL}" font-family="{FONT_MONO}" '
        f'font-weight="600" fill="{COLOR_LABEL}">TopicSet</text>'
    )
    out.append(
        f'<text x="{TOPIC_X + TOPIC_W - 14}" y="{TOPIC_Y + TOPIC_H/2 + 5}" '
        f'text-anchor="end" font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'per-pipeline view of the controller-owned TopicBroker'
        f'</text>'
    )


def _boundary_stubs(out: List[str]) -> None:
    """All boundary stubs on the LEFT side, going to the controller.

    Removes the previous left/right split which suggested the thread
    talked to two different external worlds. Everything outside the
    thread box is the controller process.
    """
    items: List[Tuple[str, str]] = [
        ("RuntimeCtrlMsgReceiver",      "in"),
        ("PipelineCompletionMsgSender", "out"),
        ("note_instance_exit",          "out"),
        ("memory_pressure rx",          "in"),
        ("topics",                      "both"),
    ]
    x_in   = THREAD_X
    x_out  = THREAD_X - STUB_LEN
    y0     = ACTOR_TOP_Y + 40
    step   = 30

    for i, (label, direction) in enumerate(items):
        y = y0 + i * step
        if direction == "in":
            out.append(
                f'<line x1="{x_out}" y1="{y}" x2="{x_in}" y2="{y}" '
                f'stroke="{COLOR_CTRL}" stroke-width="{W_CTRL}" '
                f'stroke-dasharray="4,3" marker-end="url(#ah-ctrl)"/>'
            )
        elif direction == "out":
            out.append(
                f'<line x1="{x_in}" y1="{y}" x2="{x_out}" y2="{y}" '
                f'stroke="{COLOR_CTRL}" stroke-width="{W_CTRL}" '
                f'stroke-dasharray="4,3" marker-end="url(#ah-ctrl)"/>'
            )
        else:  # both
            out.append(
                f'<line x1="{x_in}" y1="{y - 2}" x2="{x_out}" y2="{y - 2}" '
                f'stroke="{COLOR_CTRL}" stroke-width="{W_CTRL}" '
                f'stroke-dasharray="4,3" marker-end="url(#ah-ctrl)"/>'
            )
            out.append(
                f'<line x1="{x_out}" y1="{y + 2}" x2="{x_in}" y2="{y + 2}" '
                f'stroke="{COLOR_CTRL}" stroke-width="{W_CTRL}" '
                f'stroke-dasharray="4,3" marker-end="url(#ah-ctrl)"/>'
            )
        out.append(
            f'<text x="{x_in - 6}" y="{y - 4}" text-anchor="end" '
            f'font-size="{STUB_FS}" font-family="{FONT_MONO}" '
            f'fill="{COLOR_LABEL}">{_esc(label)}</text>'
        )

    # Single caption pointing back to the controller world.
    out.append(
        f'<text x="{x_in - 6}" y="{y0 + len(items) * step + 10}" '
        f'text-anchor="end" font-size="{STUB_FS}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'\u2190 to controller process'
        f'</text>'
    )


def render() -> str:
    out: List[str] = []
    out.append(page_open(PAGE_W, PAGE_H))
    out.append(arrow_marker_defs())
    out.append(title_bar(
        TITLE_X, TITLE_Y, PAGE_W - 2 * SLIDE_MARGIN_X,
        title="Pipeline thread",
        urn="RuntimePipeline",
        accent=COLOR_OTAP,
    ))
    _subtitle(out)
    _thread_box(out)
    rtc_anchor, disp_anchor = _actors(out)
    dag_anchors = _dag(out)
    _actor_to_node_links(out, rtc_anchor, disp_anchor, dag_anchors)
    _topic_tile(out)
    _boundary_stubs(out)
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
