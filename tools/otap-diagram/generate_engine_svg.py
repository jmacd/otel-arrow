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
    W = 1340
    H = 980
    parts = []

    # Markers
    parts.append(arrow_marker("al", C_CHANNEL_LOCAL))
    parts.append(arrow_marker("as", C_CHANNEL_SHARED))
    parts.append(arrow_marker("ac", C_CHANNEL_CTRL, 6))
    parts.append(arrow_marker("at", C_TOPIC))

    # ── ENGINE box ──
    EX, EY, EW, EH = 20, 55, W - 40, H - 75
    parts.append(rect(EX, EY, EW, EH, C_ENGINE_BG, C_ENGINE, 2.5, 12))
    parts.append(text(EX + 16, EY + 22, "Engine", 18, C_ENGINE, "bold", font=FONT_SANS))
    parts.append(text(EX + 100, EY + 22, "(process)", 12, C_SUBTEXT))

    # ── Title ──
    parts.append(text(W // 2, 36, "OTAP Dataflow Engine Architecture", 20, C_ENGINE, "bold",
                       "middle", FONT_SANS))

    # ═══════════════════════════════════════════════════════════════
    # LEFT COLUMN: Controller + accessory threads
    # ═══════════════════════════════════════════════════════════════
    CX, CY, CW, CH = 40, 85, 260, EH - 50
    parts.append(rect(CX, CY, CW, CH, C_CTRL_BG, C_CTRL, 1.8, 10))
    parts.append(text(CX + 12, CY + 20, "Controller", 15, C_CTRL, "bold", font=FONT_SANS))
    parts.append(text(CX + 12, CY + 36, "main thread", 10, C_SUBTEXT))

    # Accessory task boxes inside controller
    acc_tasks = [
        ("HTTP Admin", "/admin, /health, /metrics"),
        ("Metrics Aggregator", "collect from all threads"),
        ("Metrics Dispatcher", "ITS / OTel SDK / Builtin"),
        ("Engine Metrics", "RSS, process stats"),
        ("Observed State", "pipeline health, events"),
        ("Memory Limiter", "watch::channel broadcast"),
    ]
    ay = CY + 52
    for task_name, desc in acc_tasks:
        parts.append(rect(CX + 10, ay, CW - 20, 44, C_ACCESSORY_BG, C_ACCESSORY, 1, 6))
        parts.append(text(CX + 18, ay + 16, task_name, 11, C_TEXT, "bold"))
        parts.append(text(CX + 18, ay + 32, desc, 9, C_SUBTEXT))
        ay += 50

    # ITS (internal telemetry) pipeline
    its_y = ay + 10
    parts.append(rect(CX + 10, its_y, CW - 20, 50, "#fef9e7", "#f39c12", 1.2, 6))
    parts.append(text(CX + 18, its_y + 16, "Internal Telemetry Pipeline", 10, "#e67e22", "bold"))
    parts.append(text(CX + 18, its_y + 30, "ITS receiver → batch → exporter", 9, C_SUBTEXT))
    parts.append(text(CX + 18, its_y + 42, "thread-0 (dedicated)", 9, C_SUBTEXT))

    # Control channels from controller
    ctrl_chan_y = its_y + 70
    parts.append(text(CX + 12, ctrl_chan_y, "Control Channels:", 11, C_CTRL, "bold"))
    parts.append(text(CX + 12, ctrl_chan_y + 16,
                       "• RuntimeCtrlMsg (shutdown, config)", 9, C_SUBTEXT))
    parts.append(text(CX + 12, ctrl_chan_y + 30,
                       "• PipelineCompletionMsg (ack/nack)", 9, C_SUBTEXT))
    parts.append(text(CX + 12, ctrl_chan_y + 44,
                       "• watch::channel (memory pressure)", 9, C_SUBTEXT))
    parts.append(text(CX + 12, ctrl_chan_y + 58,
                       "• MetricsReporter (MPSC to aggregator)", 9, C_SUBTEXT))

    # ═══════════════════════════════════════════════════════════════
    # RIGHT AREA: Pipeline Group containing 2 cores
    # ═══════════════════════════════════════════════════════════════
    GX, GY = 320, 85
    GW, GH = EW - (GX - EX) - 20, EH - 50
    parts.append(rect(GX, GY, GW, GH, C_GROUP_BG, C_GROUP, 1.8, 10))
    parts.append(text(GX + 14, GY + 20, 'Pipeline Group "default"', 15, C_GROUP, "bold",
                       font=FONT_SANS))
    parts.append(text(GX + 14, GY + 36, "pipeline: otlp-gateway", 10, C_SUBTEXT))

    # ── Pipeline definition (small box at top) ──
    PDX, PDY = GX + 14, GY + 48
    PDW, PDH = GW - 28, 42
    parts.append(rect(PDX, PDY, PDW, PDH, "#ffffff", C_GROUP, 1, 6, dash="4,3"))
    parts.append(text(PDX + 10, PDY + 16, "Pipeline Config", 11, C_GROUP, "bold"))
    parts.append(text(PDX + 10, PDY + 30,
                       'receivers: [otlp]  →  processors: [batch]  →  exporters: [otlp]     '
                       'core_allocation: 2', 9, C_SUBTEXT))

    # ── Two Engine Core boxes ──
    core_y_start = PDY + PDH + 16
    core_w = (GW - 50) // 2
    core_h = GH - (core_y_start - GY) - 16

    for core_idx in range(2):
        cx = GX + 14 + core_idx * (core_w + 22)
        cy = core_y_start
        cw = core_w
        ch = core_h

        parts.append(rect(cx, cy, cw, ch, C_CORE_BG, C_CORE, 1.8, 8))
        parts.append(text(cx + 10, cy + 18, f"Engine Core {core_idx}", 13, C_CORE, "bold",
                           font=FONT_SANS))
        parts.append(text(cx + 10, cy + 32,
                           f"CPU {core_idx}  •  pinned thread  •  "
                           f"tokio LocalSet", 9, C_SUBTEXT))

        # ── Thread label ──
        parts.append(text(cx + cw - 10, cy + 18,
                           f"thread-{core_idx + 1}", 10, C_CORE, "bold", "end"))

        # ── Pipeline instance inside core ──
        PX = cx + 10
        PY = cy + 44
        PW = cw - 20
        PH = ch - 56
        parts.append(rect(PX, PY, PW, PH, "#ffffff", C_CORE, 1, 6, opacity=0.5))
        parts.append(text(PX + 8, PY + 14, "RuntimePipeline", 10, C_CORE, "bold"))

        # ── Nodes ──
        node_w = PW - 20
        node_h = 80
        node_x = PX + 10
        node_gap = 16

        # Receiver
        ry = PY + 26
        parts.append(rect(node_x, ry, node_w, node_h, C_NODE_BG, C_NODE_RCV, 1.5, 6))
        parts.append(text(node_x + 8, ry + 16, "Receiver: otlp", 11, C_NODE_RCV, "bold"))
        parts.append(text(node_x + 8, ry + 30, "gRPC server (SO_REUSEPORT)", 9, C_SUBTEXT))
        parts.append(text(node_x + 8, ry + 44, "shared listener across cores", 9, C_CHANNEL_SHARED))
        parts.append(text(node_x + 8, ry + 58, "output: local MPSC →", 9, C_CHANNEL_LOCAL))
        # Port badge
        parts.append(rounded_label(node_x + node_w - 58, ry + 4, 50, 18,
                                    "port 0", C_NODE_BG, C_NODE_RCV, C_NODE_RCV, 9))

        # Local channel arrow receiver → processor
        chan_y1 = ry + node_h
        chan_y2 = chan_y1 + node_gap
        chan_mid = node_x + node_w // 2
        parts.append(arrow_h(chan_mid, chan_y1, chan_mid, chan_y2, C_CHANNEL_LOCAL, 2,
                              marker_id="al"))
        parts.append(text(chan_mid + 6, chan_y1 + node_gap // 2 + 3,
                           "local channel", 8, C_CHANNEL_LOCAL))

        # Processor
        py_proc = chan_y2
        parts.append(rect(node_x, py_proc, node_w, node_h, C_NODE_BG, C_NODE_PROC, 1.5, 6))
        parts.append(text(node_x + 8, py_proc + 16, "Processor: batch", 11, C_NODE_PROC, "bold"))
        parts.append(text(node_x + 8, py_proc + 30, "accumulate → flush on timer/size", 9,
                           C_SUBTEXT))
        parts.append(text(node_x + 8, py_proc + 44, "input: local MPSC ←", 9, C_CHANNEL_LOCAL))
        parts.append(text(node_x + 8, py_proc + 58, "output: local MPSC →", 9, C_CHANNEL_LOCAL))

        # Local channel processor → exporter
        chan2_y1 = py_proc + node_h
        chan2_y2 = chan2_y1 + node_gap
        parts.append(arrow_h(chan_mid, chan2_y1, chan_mid, chan2_y2, C_CHANNEL_LOCAL, 2,
                              marker_id="al"))
        parts.append(text(chan_mid + 6, chan2_y1 + node_gap // 2 + 3,
                           "local channel", 8, C_CHANNEL_LOCAL))

        # Exporter
        py_exp = chan2_y2
        parts.append(rect(node_x, py_exp, node_w, node_h, C_NODE_BG, C_NODE_EXP, 1.5, 6))
        parts.append(text(node_x + 8, py_exp + 16, "Exporter: otlp", 11, C_NODE_EXP, "bold"))
        parts.append(text(node_x + 8, py_exp + 30, "gRPC client (shared connection)", 9,
                           C_SUBTEXT))
        parts.append(text(node_x + 8, py_exp + 44, "shared gRPC channel across cores", 9,
                           C_CHANNEL_SHARED))
        parts.append(text(node_x + 8, py_exp + 58, "input: local MPSC ←", 9, C_CHANNEL_LOCAL))

        # ── Pipeline Controller Task ──
        pct_y = py_exp + node_h + 12
        pct_h = 64
        parts.append(rect(node_x, pct_y, node_w, pct_h, "#f4f4f4", C_PIPELINE_CTRL, 1, 6))
        parts.append(text(node_x + 8, pct_y + 14, "PipelineCtrl Task", 10, C_PIPELINE_CTRL,
                           "bold"))
        parts.append(text(node_x + 8, pct_y + 28, "timer scheduling & expiry", 9, C_SUBTEXT))
        parts.append(text(node_x + 8, pct_y + 40, "completion/ack dispatch", 9, C_SUBTEXT))
        parts.append(text(node_x + 8, pct_y + 52, "metrics reporting", 9, C_SUBTEXT))

        # ── Metrics channel from core to controller ──
        metrics_arrow_x = cx + cw
        metrics_arrow_y = pct_y + pct_h // 2
        # Draw from right edge of core toward controller (leftward)
        if core_idx == 0:
            parts.append(arrow_h(cx, metrics_arrow_y, CX + CW, metrics_arrow_y,
                                  C_CHANNEL_CTRL, 1, "3,3", "ac"))

    # ── Shared resource annotations ──
    # Shared listener bracket between cores
    bracket_y = core_y_start + 70
    core0_right = GX + 14 + core_w
    core1_left = GX + 14 + core_w + 22
    bracket_mid = (core0_right + core1_left) // 2
    parts.append(f'<line x1="{core0_right - 8}" y1="{bracket_y}" '
                 f'x2="{core1_left + 8}" y2="{bracket_y}" '
                 f'stroke="{C_CHANNEL_SHARED}" stroke-width="1.5" stroke-dasharray="4,2"/>')
    parts.append(text(bracket_mid, bracket_y - 4, "SO_REUSEPORT", 8,
                       C_CHANNEL_SHARED, "bold", "middle"))
    parts.append(text(bracket_mid, bracket_y + 10, "shared port", 8,
                       C_CHANNEL_SHARED, "normal", "middle"))

    # Shared gRPC connection bracket
    bracket_y2 = core_y_start + 44 + 26 + 80 + 16 + 80 + 16 + 70
    parts.append(f'<line x1="{core0_right - 8}" y1="{bracket_y2}" '
                 f'x2="{core1_left + 8}" y2="{bracket_y2}" '
                 f'stroke="{C_CHANNEL_SHARED}" stroke-width="1.5" stroke-dasharray="4,2"/>')
    parts.append(text(bracket_mid, bracket_y2 - 4, "Arc<Channel>", 8,
                       C_CHANNEL_SHARED, "bold", "middle"))
    parts.append(text(bracket_mid, bracket_y2 + 10, "shared gRPC conn", 8,
                       C_CHANNEL_SHARED, "normal", "middle"))

    # ═══════════════════════════════════════════════════════════════
    # LEGEND at bottom
    # ═══════════════════════════════════════════════════════════════
    LY = H - 36
    lx = 40
    legend_items = [
        (C_CHANNEL_LOCAL, "Local channel (MPSC, single-thread, !Send)"),
        (C_CHANNEL_SHARED, "Shared resource (cross-thread, Send+Sync)"),
        (C_CHANNEL_CTRL, "Control channel (to/from controller)"),
    ]
    for color, desc in legend_items:
        parts.append(f'<line x1="{lx}" y1="{LY}" x2="{lx + 30}" y2="{LY}" '
                     f'stroke="{color}" stroke-width="2"/>')
        parts.append(text(lx + 36, LY + 4, desc, 10, C_TEXT))
        lx += 340

    # ═══════════════════════════════════════════════════════════════
    # Assemble SVG
    # ═══════════════════════════════════════════════════════════════
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {W} {H}" width="{W}" height="{H}">',
        '<defs>',
    ]
    # Collect markers
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
