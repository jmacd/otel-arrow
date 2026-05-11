#!/usr/bin/env python3
"""Engine architecture slide #1 -- the *pipeline group*.

Zoomed-out picture of one pipeline group at runtime: the controller
process (with its accessory thread-local tasks and shared state), the
admin server thread, and the N per-core pipeline-instance threads.

The visual emphasis of this slide is **thread-per-core**: the right
column draws every assigned core as its own boxed `RuntimePipeline`,
each labelled with a single OS thread running a single-threaded tokio
runtime. The cross-thread channels between the controller and each
pipeline instance are drawn as dashed grey edges.

Source of truth (cited file:line so the picture stays honest):
- ``Controller`` / ``ControllerRuntime`` --
  rust/otap-dataflow/crates/controller/src/lib.rs:123,
  rust/otap-dataflow/crates/controller/src/live_control/mod.rs:60
- per-core thread spawn -- crates/controller/src/lib.rs:1834
  (``std::thread::spawn`` -> ``run_pipeline_thread`` at :1995)
- per-core tokio current_thread + LocalSet --
  crates/engine/src/runtime_pipeline.rs:215,219
- accessory thread-local tasks (``spawn_thread_local_task`` names) --
  crates/controller/src/lib.rs:1251 (``process-memory-limiter``),
  :1388 (``metrics-aggregator``), :1396 (``metrics-dispatcher``),
  :1407 (``observed-state-store``), :1420 (``engine-metrics``),
  :1518 (``http-admin``)
- control-plane channels --
  crates/engine/src/control.rs:478 (``runtime_ctrl_msg_channel``),
  :486 (``pipeline_completion_msg_channel``)
- memory pressure broadcast -- crates/controller/src/lib.rs:1792
- topic sharing -- crates/engine/src/topic/broker.rs:27
- admin / control plane wiring -- crates/admin/src/lib.rs:83, :147

Visual grammar follows ``node_lib`` -- same palette, title bar, notes
box and arrow markers as the per-node slides.
"""

from __future__ import annotations
import sys
from typing import List, Tuple

from node_lib import (
    SLIDE_PAGE_W, SLIDE_PAGE_H, SLIDE_MARGIN_X, SLIDE_MARGIN_Y,
    COLOR_OTAP, COLOR_OTLP, COLOR_CTRL, COLOR_CTRL_SOFT, COLOR_CTX,
    FS_TITLE, FS_SUBTITLE, FS_NODE, FS_NODE_SUB, FS_LABEL, FS_TINY,
    W_FRAME, W_CTRL,
    page_open, page_close, title_bar, arrow_marker_defs,
    ctrl_edge,
    _esc,
)
from gen_diagram import COLOR_BG, COLOR_LABEL, COLOR_SUBLABEL, FONT, FONT_MONO


# --------------------------------------------------------------- layout

PAGE_W = SLIDE_PAGE_W
PAGE_H = SLIDE_PAGE_H

TITLE_X = SLIDE_MARGIN_X
TITLE_Y = 60
SUBTITLE_Y = 90

CONTROLLER_X = SLIDE_MARGIN_X
CONTROLLER_Y = 130
CONTROLLER_W = 580
CONTROLLER_H = 440

ADMIN_X = SLIDE_MARGIN_X
ADMIN_Y = CONTROLLER_Y + CONTROLLER_H + 50
ADMIN_W = CONTROLLER_W
ADMIN_H = 64

INSTANCE_X = 940
INSTANCE_W = 580
INSTANCE_H = 140
INSTANCE_GAP = 18
INSTANCE_Y0 = 130
INSTANCE_ELLIPSIS_GAP = 36   # extra gap where "..." sits

EDGE_LEFT_X  = CONTROLLER_X + CONTROLLER_W
EDGE_RIGHT_X = INSTANCE_X


# ---------------------------------------------------------- primitives

def _process_box(x: float, y: float, w: float, h: float,
                 title: str, sub: str,
                 struct_name: str,
                 accent: str = COLOR_CTRL) -> str:
    """A heavyweight rounded rectangle = one OS process or dedicated thread.

    Top stripe in the accent color; bold title; the Rust struct name
    is rendered top-right in monospace. The italic ``sub`` line is
    only emitted when ``sub`` is non-empty.
    """
    parts = [
        f'<g>',
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
    if sub:
        parts.append(
            f'<text x="{x + 22}" y="{y + 56}" font-size="{FS_NODE_SUB}" '
            f'font-style="italic" fill="{COLOR_SUBLABEL}">{_esc(sub)}</text>'
        )
    parts.append('</g>')
    return "".join(parts)


def _accessory_tile(x: float, y: float, w: float, h: float,
                    name: str, sub: str = "",
                    shared: bool = False) -> str:
    """A compact tile for an accessory thread-local task or shared resource.

    When ``shared=True``, the tile renders as a red-shaded bubble to
    flag that the resource is reachable from multiple pipeline threads
    (and therefore subject to Send-required cross-thread access). The
    color matches the SHARED badge convention used on the per-node
    slides for the same reason.
    """
    if shared:
        #bg_fill = "#fbf3f2"          # warm tint matching the SHARED badge
        bg_fill = "#f4f6f8"
        stroke_color = COLOR_OTLP    # red outline -> Send-required
        stroke_width = 1.4
    else:
        bg_fill = "#f4f6f8"
        stroke_color = COLOR_CTRL_SOFT
        stroke_width = 1.0
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" ry="14" '
        f'fill="{bg_fill}" stroke="{stroke_color}" '
        f'stroke-width="{stroke_width}"/>',
        f'<text x="{x + 12}" y="{y + h/2 + 5}" font-size="{FS_LABEL}" '
        f'font-family="{FONT_MONO}" font-weight="600" '
        f'fill="{COLOR_LABEL}">{_esc(name)}</text>',
    ]
    if sub:
        parts.append(
            f'<text x="{x + w - 12}" y="{y + h/2 + 5}" text-anchor="end" '
            f'font-size="{FS_TINY}" font-style="italic" '
            f'fill="{COLOR_SUBLABEL}">{_esc(sub)}</text>'
        )
    return "".join(parts)


def _instance_tile(x: float, y: float, w: float, h: float,
                   core_label: str) -> str:
    """An Engine Core box -- the *host* for one or more pipelines
    (one OS thread per pipeline). Each pipeline is drawn as a tiny
    DAG of nodes; pipelines on the same core are linked by a faint
    intra-group connector to show they form a group within the core.
    """
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" ry="10" '
        f'fill="white" stroke="{COLOR_CTRL}" stroke-width="{W_FRAME}"/>',
        f'<rect x="{x}" y="{y}" width="{w}" height="6" rx="3" ry="3" '
        f'fill="{COLOR_OTAP}"/>',
        f'<text x="{x + 22}" y="{y + 32}" font-size="{FS_NODE_SUB + 2}" '
        f'font-weight="700" fill="{COLOR_LABEL}">{_esc(core_label)}</text>',
        f'<text x="{x + w - 18}" y="{y + 32}" text-anchor="end" '
        f'font-size="{FS_TINY}" font-style="italic" '
        f'fill="{COLOR_SUBLABEL}">'
        f'one OS thread per pipeline'
        f'</text>',
    ]
    # Two mini DAGs (different shapes to suggest heterogeneous
    # pipelines within a group).
    dag_x = x + 40
    dag_w = w - 80
    pipe1_y = y + 64
    pipe2_y = y + h - 32

    # Buffer the parts list and pass to _mini_dag via closure.
    def add(svg_chunks: List[str], chunk: str) -> None:
        svg_chunks.append(chunk)

    # Inline expansion: build pipeline rows directly into ``parts``.
    p1 = _make_dag_chunks(dag_x, pipe1_y, dag_w, ["rx", "p", "p", "tx"])
    p2 = _make_dag_chunks(dag_x, pipe2_y, dag_w, ["rx", "p", "p", "p", "tx"])
    parts.extend(p1["chunks"])
    parts.extend(p2["chunks"])

    # Faint dashed intra-group connector between the two pipelines:
    # touches the rightmost node of pipeline 1 and the leftmost node
    # of pipeline 2, hinting at the in-engine wiring (topics, etc.)
    # that lets pipelines on the same core exchange data.
    parts.append(
        f'<path d="M{p1["right"]},{p1["cy"] + 6} '
        f'C{p1["right"]},{(p1["cy"] + p2["cy"]) / 2} '
        f'{p2["left"]},{(p1["cy"] + p2["cy"]) / 2} '
        f'{p2["left"]},{p2["cy"] - 6}" '
        f'fill="none" stroke="{COLOR_CTRL_SOFT}" stroke-width="1" '
        f'stroke-dasharray="3,3"/>'
    )

    return "".join(parts)


def _make_dag_chunks(x: float, y: float, w: float,
                     shape: List[str],
                     color: str = COLOR_OTAP) -> dict:
    """Same as ``_mini_dag`` but returns a dict so caller can read the
    DAG's anchor points without managing an output list inline.
    """
    chunks: List[str] = []
    n = len(shape)
    r = 5.5
    x0 = x + r
    x1 = x + w - r
    step = (x1 - x0) / (n - 1) if n > 1 else 0
    cxs = [x0 + i * step for i in range(n)]
    for i in range(n - 1):
        chunks.append(
            f'<line x1="{cxs[i] + r + 1}" y1="{y}" '
            f'x2="{cxs[i+1] - r - 2}" y2="{y}" '
            f'stroke="{color}" stroke-width="1.4" '
            f'marker-end="url(#ah-pdata)"/>'
        )
    for i, kind in enumerate(shape):
        rr = r if kind in ("rx", "tx") else r - 1
        chunks.append(
            f'<circle cx="{cxs[i]}" cy="{y}" r="{rr}" fill="{color}"/>'
        )
    return {"chunks": chunks, "left": cxs[0], "right": cxs[-1], "cy": y}


def _ellipsis(cx: float, cy: float) -> str:
    return (
        f'<text x="{cx}" y="{cy}" text-anchor="middle" '
        f'font-size="{FS_NODE}" fill="{COLOR_SUBLABEL}">\u22ee</text>'
    )


def _labeled_edge(x1: float, y1: float, x2: float, y2: float,
                  label: str, dashed: bool = True,
                  arrow_at: str = "end",
                  label_offset: float = -8,
                  label_left_pad: float = 30) -> str:
    """A horizontal cross-thread edge with a left-justified label
    sitting just above the line, padded in from the leftmost x so it
    does not crowd the controller-side rail.
    """
    edge = ctrl_edge(x1, y1, x2, y2, dashed=dashed, arrow_at=arrow_at)
    return (
        edge
        + f'<text x="{min(x1, x2) + label_left_pad}" y="{y1 + label_offset}" '
          f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
          f'fill="{COLOR_LABEL}">{_esc(label)}</text>'
    )


# ---------------------------------------------------------------- render

def _render_controller(out: List[str]) -> None:
    out.append(_process_box(
        CONTROLLER_X, CONTROLLER_Y, CONTROLLER_W, CONTROLLER_H,
        title="Controller process",
        sub="",
        struct_name="ControllerRuntime",
        accent=COLOR_CTRL,
    ))

    # Accessory thread-local tasks (stacked tiles).
    tile_x = CONTROLLER_X + 22
    tile_w = CONTROLLER_W - 44
    tile_h = 32
    tile_gap = 8
    section_y = CONTROLLER_Y + 80

    out.append(
        f'<text x="{tile_x}" y="{section_y}" font-size="{FS_LABEL}" '
        f'font-weight="700" fill="{COLOR_LABEL}">'
        f'thread-local accessory tasks</text>'
    )
    accessory: List[str] = [
        "process-memory-limiter",
        "metrics-aggregator",
        "metrics-dispatcher",
        "observed-state-store",
        "engine-metrics",
    ]
    ay = section_y + 14
    for name in accessory:
        out.append(_accessory_tile(tile_x, ay, tile_w, tile_h, name))
        ay += tile_h + tile_gap

    # Shared resources block at the bottom of the controller box.
    shared_y = ay + 14
    out.append(
        f'<text x="{tile_x}" y="{shared_y}" font-size="{FS_LABEL}" '
        f'font-weight="700" fill="{COLOR_LABEL}">'
        f'shared resources</text>'
    )
    shared: List[str] = [
        "TopicBroker",
        "memory_pressure",
    ]
    sy = shared_y + 14
    for name in shared:
        out.append(_accessory_tile(tile_x, sy, tile_w, tile_h, name,
                                   shared=True))
        sy += tile_h + tile_gap


def _render_admin(out: List[str]) -> None:
    out.append(_process_box(
        ADMIN_X, ADMIN_Y, ADMIN_W, ADMIN_H,
        title="http-admin",
        sub="",
        struct_name="otap_df_admin::run",
        accent=COLOR_CTX,
    ))

    # Connection from controller down to admin: Arc<dyn ControlPlane>.
    cx = CONTROLLER_X + CONTROLLER_W * 0.55
    out.append(ctrl_edge(
        cx, CONTROLLER_Y + CONTROLLER_H,
        cx, ADMIN_Y, dashed=False, arrow_at="end",
    ))
    out.append(
        f'<text x="{cx + 8}" y="{(CONTROLLER_Y + CONTROLLER_H + ADMIN_Y) / 2 + 4}" '
        f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
        f'fill="{COLOR_LABEL}">Arc&lt;dyn ControlPlane&gt;</text>'
    )


def _render_instances(out: List[str]) -> List[float]:
    """Draw the three per-core instance tiles. Returns their center y values."""
    centers: List[float] = []
    y = INSTANCE_Y0
    for label in ("Engine Group 0", "Engine Group 1"):
        out.append(_instance_tile(
            INSTANCE_X, y, INSTANCE_W, INSTANCE_H,
            core_label=label,
        ))
        centers.append(y + INSTANCE_H / 2)
        y += INSTANCE_H + INSTANCE_GAP
    # Ellipsis row.
    out.append(_ellipsis(INSTANCE_X + INSTANCE_W / 2,
                         y + INSTANCE_ELLIPSIS_GAP / 2 + 8))
    y += INSTANCE_ELLIPSIS_GAP
    # Engine Core N.
    out.append(_instance_tile(
        INSTANCE_X, y, INSTANCE_W, INSTANCE_H,
        core_label="Engine Group N",
    ))
    centers.append(y + INSTANCE_H / 2)
    return centers


def _render_cross_thread_edges(out: List[str], centers: List[float]) -> None:
    """Draw the labeled dashed edges between the controller and each instance.

    Each instance gets the same five edges; we draw each kind on a
    different center so the labels stay readable. The point of the
    slide is that *every* core has the same five-channel relationship
    with the controller.
    """
    # Five edges, drawn against the three visible instance tiles.
    # We rotate so edge 1 of core 0, edge 2 of core 1, edge 3 of core N,
    # then edges 4 and 5 are drawn on core 0 and core 1 respectively.
    # Simpler: draw all five edges to core 0 (representative) plus a
    # short replicated stub on the other two so the viewer sees the
    # pattern repeats. We stack the five labels along the vertical
    # midline between columns.
    mid_x = (EDGE_LEFT_X + EDGE_RIGHT_X) / 2
    rep_y = centers[0]      # representative core
    other_y = centers[1:]   # repeat the pattern as short stubs

    edge_step = 26
    edges = [
        ("RuntimeCtrlMsgSender", "right", False),         # ctrl out
        ("CompletionMsgSender", "left",  False),  # completion in
        ("memory_pressure signal", "right", True),         # broadcast
        ("note_instance_exit", "left", True),             # exit signal
        ("topics", "both", True),                         # bi-dir
    ]

    base_y = rep_y - (len(edges) - 1) * edge_step / 2
    for i, (label, direction, dashed) in enumerate(edges):
        ey = base_y + i * edge_step
        x1, x2 = EDGE_LEFT_X, EDGE_RIGHT_X
        if direction == "right":
            out.append(_labeled_edge(x1, ey, x2, ey, label,
                                     dashed=dashed, arrow_at="end",
                                     label_offset=-6))
        elif direction == "left":
            out.append(_labeled_edge(x1, ey, x2, ey, label,
                                     dashed=dashed, arrow_at="start",
                                     label_offset=-6))
        else:  # both
            # Draw two thin ones with a shared left-justified label.
            edge = (
                ctrl_edge(x1, ey - 2, x2, ey - 2,
                          dashed=dashed, arrow_at="end")
                + ctrl_edge(x1, ey + 2, x2, ey + 2,
                            dashed=dashed, arrow_at="start")
            )
            out.append(
                edge
                + f'<text x="{min(x1, x2) + 30}" y="{ey - 8}" '
                  f'font-size="{FS_TINY}" font-family="{FONT_MONO}" '
                  f'fill="{COLOR_LABEL}">{_esc(label)}</text>'
            )

    # Repeat the pattern as short unlabeled stubs against the other
    # cores (so the viewer reads "every core has the same channels").
    stub_x1 = EDGE_RIGHT_X - 60
    stub_x2 = EDGE_RIGHT_X
    for cy in other_y:
        for i, (_label, direction, dashed) in enumerate(edges):
            sy = cy - (len(edges) - 1) * edge_step / 2 + i * edge_step
            arrow_at = "end" if direction == "right" else (
                "start" if direction == "left" else "end")
            out.append(ctrl_edge(stub_x1, sy, stub_x2, sy,
                                 dashed=dashed, arrow_at=arrow_at))


def _render_thread_emphasis(out: List[str]) -> None:
    """A short banner that calls out the thread-per-core invariant."""
    bx = INSTANCE_X
    by = INSTANCE_Y0 - 32
    bw = INSTANCE_W
    bh = 24
    out.append(
        f'<rect x="{bx}" y="{by}" width="{bw}" height="{bh}" rx="4" ry="4" '
        f'fill="{COLOR_OTAP}" fill-opacity="0.10" '
        f'stroke="{COLOR_OTAP}" stroke-width="1"/>'
    )
    out.append(
        f'<text x="{bx + bw/2}" y="{by + bh/2 + 5}" text-anchor="middle" '
        f'font-size="{FS_LABEL}" font-weight="700" '
        f'fill="{COLOR_OTAP}">'
        f'one single-threaded runtime per pipeline'
        f'</text>'
    )


def _render_subtitle(out: List[str]) -> None:
    out.append(
        f'<text x="{TITLE_X}" y="{SUBTITLE_Y}" font-size="{FS_SUBTITLE}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">'
        f'One controller, one engine group per CPU, one thread per pipeline.'
        f'</text>'
    )


def render() -> str:
    out: List[str] = []
    out.append(page_open(PAGE_W, PAGE_H))
    out.append(arrow_marker_defs())
    out.append(title_bar(
        TITLE_X, TITLE_Y, PAGE_W - 2 * SLIDE_MARGIN_X,
        title="OpenTelemetry Arrow Dataflow Engine",
        urn="ControllerRuntime + N \u00d7 RuntimePipeline",
        accent=COLOR_OTAP,
    ))
    _render_subtitle(out)
    _render_controller(out)
    _render_admin(out)
    _render_thread_emphasis(out)
    centers = _render_instances(out)
    _render_cross_thread_edges(out, centers)
    out.append(page_close())
    return "".join(out)


def main(argv: List[str]) -> int:
    out = argv[1] if len(argv) > 1 else "dataflow_engine.svg"
    svg = render()
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
