#!/usr/bin/env python3
"""Generate an SVG diagram of the OTAP Dataflow Engine architecture.

Shows:
  - Engine (top-level)
  - Controller thread with accessory tasks
  - Pipeline Group containing a pipeline definition
  - Two Engine Cores (CPU 0, CPU 1), each a pinned thread with
    identical pipeline copies (Receiver → Processor → Exporter)
  - Local vs shared connections, internal channels
  - Pipeline controller task per core

Usage:
    python3 tools/otap-diagram/generate_engine_svg.py

Output: tools/otap-diagram/output/otap-engine-architecture.svg
"""

import os

FONT = "Consolas, 'Courier New', monospace"
FONT_SANS = "'Segoe UI', 'Helvetica Neue', Arial, sans-serif"

# ── Colors ──────────────────────────────────────────────────────────

C_BG = "#f8f9fa"
C_ENGINE = "#2c3e50"         # engine border
C_ENGINE_BG = "#ffffff"
C_CTRL = "#34495e"           # controller
C_CTRL_BG = "#ecf0f1"
C_GROUP = "#2980b9"          # pipeline group
C_GROUP_BG = "#eaf2f8"
C_CORE = "#16a085"           # engine core / CPU
C_CORE_BG = "#e8f8f5"
C_NODE_RCV = "#27ae60"       # receiver
C_NODE_PROC = "#8e44ad"      # processor
C_NODE_EXP = "#c0392b"       # exporter
C_NODE_BG = "#ffffff"
C_CHANNEL_LOCAL = "#2ecc71"  # local channel (MPSC, single-thread)
C_CHANNEL_SHARED = "#e67e22" # shared channel (cross-thread)
C_CHANNEL_CTRL = "#95a5a6"   # control channel
C_TOPIC = "#3498db"          # topic / cross-pipeline
C_TEXT = "#2c3e50"
C_SUBTEXT = "#7f8c8d"
C_PIPELINE_CTRL = "#7f8c8d"
C_ACCESSORY = "#bdc3c7"
C_ACCESSORY_BG = "#f4f4f4"


def xml_esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Primitive drawing helpers ───────────────────────────────────────

def rect(x, y, w, h, fill, stroke, stroke_w=1.5, rx=8, dash="", opacity=1.0):
    extra = f' stroke-dasharray="{dash}"' if dash else ""
    extra += f' opacity="{opacity}"' if opacity < 1.0 else ""
    return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" '
            f'rx="{rx}" fill="{fill}" stroke="{stroke}" stroke-width="{stroke_w}"{extra}/>')


def text(x, y, label, size=12, color=C_TEXT, weight="normal", anchor="start", font=None):
    f = font or FONT
    return (f'<text x="{x}" y="{y}" font-family="{f}" font-size="{size}" '
            f'fill="{color}" font-weight="{weight}" text-anchor="{anchor}">'
            f'{xml_esc(label)}</text>')


def arrow_h(x1, y1, x2, y2, color, width=1.5, dash="", marker_id=None):
    """Horizontal/straight arrow."""
    extra = f' stroke-dasharray="{dash}"' if dash else ""
    end = f' marker-end="url(#{marker_id})"' if marker_id else ""
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
            f'stroke="{color}" stroke-width="{width}"{extra}{end}/>')


def arrow_marker(mid, color, size=7):
    return (f'<marker id="{mid}" markerWidth="{size}" markerHeight="{size}" '
            f'refX="{size}" refY="{size // 2}" orient="auto">'
            f'<polygon points="0 0, {size} {size // 2}, 0 {size}" fill="{color}"/>'
            f'</marker>')


def rounded_label(x, y, w, h, label, bg, border, text_color, font_size=11):
    """Small rounded pill with centered text."""
    parts = [
        rect(x, y, w, h, bg, border, 1.2, rx=h // 2),
        text(x + w // 2, y + h // 2 + 4, label, font_size, text_color, "bold", "middle"),
    ]
    return "\n".join(parts)


# ── Main diagram builder ───────────────────────────────────────────

def make_engine_diagram():
    W = 1560
    H = 920
    parts = []

    # Markers
    parts.append(arrow_marker("al", C_CHANNEL_LOCAL, 8))
    parts.append(arrow_marker("as", C_CHANNEL_SHARED, 8))
    parts.append(arrow_marker("ac", C_CHANNEL_CTRL, 6))
    parts.append(arrow_marker("at", C_TOPIC, 8))
    parts.append(arrow_marker("an", "#555555", 9))  # network arrow

    # ── Title ──
    parts.append(text(W // 2, 32, "OTAP Dataflow Engine Architecture", 20, C_ENGINE, "bold",
                       "middle", FONT_SANS))

    # ── ENGINE box ──
    EX, EY, EW, EH = 20, 50, W - 40, H - 70
    parts.append(rect(EX, EY, EW, EH, C_ENGINE_BG, C_ENGINE, 2.5, 12))
    parts.append(text(EX + 16, EY + 20, "Engine", 16, C_ENGINE, "bold", font=FONT_SANS))
    parts.append(text(EX + 92, EY + 20, "(process)", 11, C_SUBTEXT))

    # ═══════════════════════════════════════════════════════════════
    # TOP STRIP: Controller + accessory threads (horizontal)
    # ═══════════════════════════════════════════════════════════════
    CX, CY = EX + 14, EY + 36
    CW, CH = EW - 28, 120
    parts.append(rect(CX, CY, CW, CH, C_CTRL_BG, C_CTRL, 1.5, 8))
    parts.append(text(CX + 12, CY + 16, "Controller", 13, C_CTRL, "bold", font=FONT_SANS))
    parts.append(text(CX + 100, CY + 16, "main thread — accessory tasks", 10, C_SUBTEXT))

    # Accessory boxes in a horizontal row
    acc_tasks = [
        ("HTTP Admin", "/admin /health"),
        ("Metrics\nAggregator", "collect metrics"),
        ("Metrics\nDispatcher", "ITS / Builtin"),
        ("Engine\nMetrics", "RSS, process"),
        ("Observed\nState", "health, events"),
        ("Memory\nLimiter", "watch broadcast"),
        ("Internal Telemetry\nPipeline", "thread-0"),
    ]
    acc_w = (CW - 24 - 8 * (len(acc_tasks) - 1)) // len(acc_tasks)
    acc_h = 62
    acc_y = CY + 30
    for i, (name, desc) in enumerate(acc_tasks):
        ax = CX + 12 + i * (acc_w + 8)
        is_its = i == len(acc_tasks) - 1
        bg = "#fef9e7" if is_its else C_ACCESSORY_BG
        bdr = "#f39c12" if is_its else C_ACCESSORY
        tc = "#e67e22" if is_its else C_TEXT
        parts.append(rect(ax, acc_y, acc_w, acc_h, bg, bdr, 1, 5))
        lines = name.split("\n")
        for li, line in enumerate(lines):
            parts.append(text(ax + acc_w // 2, acc_y + 14 + li * 13, line, 9, tc, "bold",
                               "middle"))
        parts.append(text(ax + acc_w // 2, acc_y + acc_h - 8, desc, 8, C_SUBTEXT, "normal",
                           "middle"))

    # Control channel summary below controller
    ctrl_y = CY + CH + 4
    parts.append(text(CX + 12, ctrl_y + 10,
                       "Control: RuntimeCtrlMsg (shutdown)  •  PipelineCompletionMsg (ack/nack)  "
                       "•  watch::channel (memory pressure)  •  MetricsReporter (MPSC)",
                       9, C_CHANNEL_CTRL))

    # ═══════════════════════════════════════════════════════════════
    # MAIN AREA: Pipeline Group with 2 horizontal-flow cores
    # ═══════════════════════════════════════════════════════════════
    GX = EX + 14
    GY = ctrl_y + 22
    GW = EW - 28
    GH = EH - (GY - EY) - 14
    parts.append(rect(GX, GY, GW, GH, C_GROUP_BG, C_GROUP, 1.5, 8))
    parts.append(text(GX + 14, GY + 18, 'Pipeline Group "default"', 14, C_GROUP, "bold",
                       font=FONT_SANS))
    parts.append(text(GX + 230, GY + 18,
                       "pipeline: otlp-gateway  •  core_allocation: 2", 10, C_SUBTEXT))

    # ── Network ingress (left side) ──
    NET_IN_W = 90
    NET_IN_X = GX + 14
    net_area_top = GY + 32
    net_area_h = GH - 46
    parts.append(rect(NET_IN_X, net_area_top, NET_IN_W, net_area_h,
                       "#f9f9f9", "#888888", 1.5, 6, dash="6,3"))
    parts.append(text(NET_IN_X + NET_IN_W // 2, net_area_top + 18, "Network", 11, "#555555",
                       "bold", "middle"))
    parts.append(text(NET_IN_X + NET_IN_W // 2, net_area_top + 32, "Ingress", 11, "#555555",
                       "bold", "middle"))
    parts.append(text(NET_IN_X + NET_IN_W // 2, net_area_top + 52, "gRPC :4317", 9,
                       C_SUBTEXT, "normal", "middle"))
    parts.append(text(NET_IN_X + NET_IN_W // 2, net_area_top + 66, "SO_REUSEPORT", 8,
                       C_CHANNEL_SHARED, "bold", "middle"))
    parts.append(text(NET_IN_X + NET_IN_W // 2, net_area_top + 80, "OS distributes", 8,
                       C_SUBTEXT, "normal", "middle"))
    parts.append(text(NET_IN_X + NET_IN_W // 2, net_area_top + 92, "across cores", 8,
                       C_SUBTEXT, "normal", "middle"))

    # ── Network egress (right side) ──
    NET_OUT_W = 90
    NET_OUT_X = GX + GW - 14 - NET_OUT_W
    parts.append(rect(NET_OUT_X, net_area_top, NET_OUT_W, net_area_h,
                       "#f9f9f9", "#888888", 1.5, 6, dash="6,3"))
    parts.append(text(NET_OUT_X + NET_OUT_W // 2, net_area_top + 18, "Network", 11, "#555555",
                       "bold", "middle"))
    parts.append(text(NET_OUT_X + NET_OUT_W // 2, net_area_top + 32, "Egress", 11, "#555555",
                       "bold", "middle"))
    parts.append(text(NET_OUT_X + NET_OUT_W // 2, net_area_top + 52, "gRPC client", 9,
                       C_SUBTEXT, "normal", "middle"))
    parts.append(text(NET_OUT_X + NET_OUT_W // 2, net_area_top + 66, "Arc<Channel>", 8,
                       C_CHANNEL_SHARED, "bold", "middle"))
    parts.append(text(NET_OUT_X + NET_OUT_W // 2, net_area_top + 80, "shared conn", 8,
                       C_SUBTEXT, "normal", "middle"))
    parts.append(text(NET_OUT_X + NET_OUT_W // 2, net_area_top + 92, "across cores", 8,
                       C_SUBTEXT, "normal", "middle"))

    # ── Two Engine Core rows (horizontal pipeline flow) ──
    core_left = NET_IN_X + NET_IN_W + 30
    core_right = NET_OUT_X - 30
    core_w = core_right - core_left
    core_h = (net_area_h - 20) // 2
    core_gap = 20

    for core_idx in range(2):
        cy = net_area_top + core_idx * (core_h + core_gap)
        cx = core_left
        cw = core_w
        ch = core_h

        parts.append(rect(cx, cy, cw, ch, C_CORE_BG, C_CORE, 1.5, 8))
        parts.append(text(cx + 10, cy + 16, f"Engine Core {core_idx}", 12, C_CORE, "bold",
                           font=FONT_SANS))
        parts.append(text(cx + 120, cy + 16,
                           f"CPU {core_idx}  •  pinned thread-{core_idx + 1}  •  "
                           f"tokio LocalSet  •  single-threaded async",
                           9, C_SUBTEXT))

        # Horizontal pipeline: Receiver → Processor → Exporter
        node_area_y = cy + 28
        node_h = ch - 66
        pipe_label_y = cy + ch - 28

        # Three nodes equally spaced with gaps for arrows
        arrow_gap = 60   # space between nodes for arrow + label
        available = cw - 20  # padding inside core
        node_w = (available - 2 * arrow_gap) // 3
        node_x_start = cx + 10

        nodes = [
            ("Receiver", "otlp", C_NODE_RCV, [
                "gRPC server",
                "SO_REUSEPORT listener",
                "output →",
            ]),
            ("Processor", "batch", C_NODE_PROC, [
                "accumulate items",
                "flush on timer/size",
                "← input  output →",
            ]),
            ("Exporter", "otlp", C_NODE_EXP, [
                "gRPC client",
                "shared connection",
                "← input",
            ]),
        ]

        node_centers = []  # (right_edge_x, center_y) for arrows

        for ni, (ntype, nname, ncolor, descs) in enumerate(nodes):
            nx = node_x_start + ni * (node_w + arrow_gap)
            ny = node_area_y
            nw = node_w
            nh = node_h

            parts.append(rect(nx, ny, nw, nh, C_NODE_BG, ncolor, 1.5, 6))
            parts.append(text(nx + 10, ny + 16, f"{ntype}: {nname}", 11, ncolor, "bold"))
            for di, d in enumerate(descs):
                color = C_CHANNEL_LOCAL if "→" in d or "←" in d else C_SUBTEXT
                parts.append(text(nx + 10, ny + 32 + di * 14, d, 9, color))

            node_centers.append((nx, nx + nw, ny + nh // 2))

        # Arrows between nodes (horizontal)
        for ni in range(len(nodes) - 1):
            _, x1_right, y1 = node_centers[ni]
            x2_left, _, y2 = node_centers[ni + 1]
            mid_x = (x1_right + x2_left) // 2
            ay = (y1 + y2) // 2
            parts.append(arrow_h(x1_right + 4, ay, x2_left - 4, ay,
                                  C_CHANNEL_LOCAL, 2.5, marker_id="al"))
            parts.append(text(mid_x, ay - 8, "local MPSC", 8, C_CHANNEL_LOCAL, "bold",
                               "middle"))
            parts.append(text(mid_x, ay + 12, "!Send, single-thread", 7, C_SUBTEXT, "normal",
                               "middle"))

        # PipelineCtrl Task (small box at bottom of core)
        pct_w = cw - 20
        pct_h = 24
        pct_x = cx + 10
        pct_y = pipe_label_y
        parts.append(rect(pct_x, pct_y, pct_w, pct_h, "#f4f4f4", C_PIPELINE_CTRL, 0.8, 4))
        parts.append(text(pct_x + pct_w // 2, pct_y + 15,
                           "PipelineCtrl: timers  •  ack/nack dispatch  •  "
                           "metrics reporting  •  memory pressure",
                           8, C_PIPELINE_CTRL, "normal", "middle"))

        # ── Network ingress arrow into receiver ──
        rcv_left = node_x_start
        rcv_cy = node_area_y + node_h // 2
        net_in_right = NET_IN_X + NET_IN_W
        parts.append(arrow_h(net_in_right + 2, rcv_cy, rcv_left - 4, rcv_cy,
                              "#555555", 2, marker_id="an"))

        # ── Network egress arrow from exporter ──
        exp_right = node_x_start + 2 * (node_w + arrow_gap) + node_w
        exp_cy = rcv_cy
        parts.append(arrow_h(exp_right + 4, exp_cy, NET_OUT_X - 2, exp_cy,
                              "#555555", 2, marker_id="an"))

    # ── Control channel arrows from cores back to controller ──
    for core_idx in range(2):
        cy = net_area_top + core_idx * (core_h + core_gap) + core_h - 8
        # small upward dashed arrow from top of core to controller strip
        arr_x = core_left + core_w - 40 - core_idx * 30
        parts.append(f'<line x1="{arr_x}" y1="{cy}" x2="{arr_x}" y2="{ctrl_y + 12}" '
                     f'stroke="{C_CHANNEL_CTRL}" stroke-width="1" stroke-dasharray="3,3" '
                     f'marker-end="url(#ac)"/>')
        parts.append(text(arr_x + 5, cy - 6, "ctrl", 7, C_CHANNEL_CTRL))

    # ═══════════════════════════════════════════════════════════════
    # LEGEND at bottom
    # ═══════════════════════════════════════════════════════════════
    LY = H - 28
    lx = 40
    legend_items = [
        (C_CHANNEL_LOCAL, "──", "Local channel (MPSC, single-thread, !Send)"),
        (C_CHANNEL_SHARED, "- -", "Shared resource (cross-thread, Send+Sync)"),
        (C_CHANNEL_CTRL, "···", "Control channel (to/from controller)"),
        ("#555555", "──▶", "Network data flow (ingress / egress)"),
    ]
    for color, style, desc in legend_items:
        parts.append(f'<line x1="{lx}" y1="{LY}" x2="{lx + 28}" y2="{LY}" '
                     f'stroke="{color}" stroke-width="2"/>')
        parts.append(text(lx + 34, LY + 4, desc, 10, C_TEXT))
        lx += 340

    # ═══════════════════════════════════════════════════════════════
    # Assemble SVG
    # ═══════════════════════════════════════════════════════════════
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {W} {H}" width="{W}" height="{H}">',
        '<defs>',
    ]
    markers = [p for p in parts if p.startswith('<marker')]
    body = [p for p in parts if not p.startswith('<marker')]
    svg.extend(markers)
    svg.append('</defs>')
    svg.append(f'<rect width="{W}" height="{H}" fill="{C_BG}" rx="12"/>')
    svg.extend(body)
    svg.append('</svg>')
    return "\n".join(svg)


def main():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "otap-engine-architecture.svg")
    with open(path, "w") as f:
        f.write(make_engine_diagram())
    print(f"  ✓ {path}")


if __name__ == "__main__":
    main()
