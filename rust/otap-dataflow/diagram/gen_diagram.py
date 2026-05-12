#!/usr/bin/env python3
"""
Generate an SVG "racetrack"/digital-timing-style diagram for the OTLP vs OTAP
pipeline-conversion-cost experiments.

Conventions
-----------
Signal levels (Texas-Instruments-style timing diagram):
  - LOW  (0) = OTLP  in-process pdata on a typed channel
  - HIGH (1) = OTAP  in-process pdata on a typed channel
  - MID      = serialized bytes on the wire (encoding labelled below)
  - UNDEF    = pre-ITR ("half-encoded" worker output) -- drawn as a
               hashed/ambiguous band straddling both levels

Each scenario is a row. A row contains a sequence of "segments". A segment
has a level (low/high/mid/undef) plus a label drawn above (the producing
stage) and optional sublabel drawn below (e.g. wire encoding name).

Transitions between segments are vertical edges, like a digital signal.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Literal
import math, sys, os

# Transition "rise/fall" angle from horizontal. 75° => steep but visibly sloped.
RAMP_ANGLE_DEG    = 75.0

Level = Literal["low", "high", "mid", "undef"]

# ---------------------------------------------------------------- geometry
# All sizes in SVG user units (≈px). Slide-friendly aspect.
PAGE_W            = 1600
PAGE_MARGIN_X     = 80
ROW_HEIGHT        = 200          # vertical room per scenario
ROW_GAP           = 40
TOP_MARGIN        = 120          # title + header
BOTTOM_MARGIN     = 60

LEFT_LABEL_W      = 150          # scenario label column on the left
LEVEL_LABEL_W     = 60           # column between scenario label and track for OTAP/OTLP
TRACK_LEFT_PAD    = 20
TRACK_RIGHT_PAD   = 20

# Within a row, a track has an inner band of height TRACK_BAND between
# y_low (bottom) and y_high (top). MID sits halfway between.
TRACK_BAND        = 90
TRACK_TOP_PAD     = 70           # space above the band for labels
TRACK_BOT_PAD     = 40           # space below the band for sublabels

# Visual style
LINE_W            = 3.0
GRID_W            = 1.0
FONT              = "Inter, 'Helvetica Neue', Arial, sans-serif"
FONT_MONO         = "'JetBrains Mono', Menlo, Consolas, monospace"

COLOR_BG          = "#ffffff"
COLOR_TRACK       = "#1f2933"    # signal line
COLOR_LOW         = "#b15a4f"    # OTLP — muted warm red (higher-entropy encoding)
COLOR_HIGH        = "#4f7a8a"    # OTAP — muted cool teal/blue
COLOR_MID         = "#8a929c"    # serialized wire bytes — neutral grey
COLOR_UNDEF       = "#9aa5b1"
COLOR_GRID        = "#d9e1ec"
COLOR_LABEL       = "#1f2933"
COLOR_SUBLABEL    = "#52606d"
COLOR_BOUNDARY    = "#b0b8c4"    # logger/wire/collector zone separators

# ------------------------------------------------------------------ model
@dataclass
class Segment:
    label: str                  # stage name, drawn above
    level: Level
    sublabel: str = ""          # e.g. wire encoding "OTLP" / "OTAP"
    weight: float = 1.0         # relative width


@dataclass
class Scenario:
    title: str                  # left-column label
    subtitle: str = ""
    segments: List[Segment] = field(default_factory=list)


# --------------------------------------------------------------- scenarios
def scenario(name: str, subtitle: str, batch: Level, expt: Level,
             coll: Level, wire_label: str) -> Scenario:
    return Scenario(
        title=name,
        subtitle=subtitle,
        segments=[
            Segment("Logs API", "undef", "struct/bytes", weight=0.9),
            Segment("Logs SDK", "low", weight=1.4),
            Segment("Batch",    batch,                   weight=1.1),
            Segment("Exporter", expt,                    weight=0.9),
            Segment("Wire",     "mid",   wire_label,     weight=1.1),
            Segment("Receiver", coll,                    weight=1.6),
        ],
    )

SCENARIOS: List[Scenario] = [
    scenario("Scenario A", "OTLP end-to-end",         "low",  "low",  "low",  "OTLP bytes"),
    scenario("Scenario B", "OTLP batch, OTAP wire",   "low",  "high", "high", "OTAP bytes"),
    scenario("Scenario C", "OTAP end-to-end",         "high", "high", "high", "OTAP bytes"),
    scenario("Scenario D", "OTAP batch, OTLP wire",   "high", "low",  "low",  "OTLP bytes"),
]


# ------------------------------------------------------------- rendering
def level_y(level: Level, y_top: float, y_bot: float) -> float:
    if level == "high": return y_top
    if level == "low":  return y_bot
    if level == "mid":  return (y_top + y_bot) / 2
    return (y_top + y_bot) / 2

def seg_color(level: Level) -> str:
    return {"low": COLOR_LOW, "high": COLOR_HIGH,
            "mid": COLOR_MID, "undef": COLOR_UNDEF}[level]

def render_row(out: list, y0: float, track_x0: float, track_x1: float,
               scen: Scenario) -> None:
    band_top = y0 + TRACK_TOP_PAD
    band_bot = band_top + TRACK_BAND
    mid_y    = (band_top + band_bot) / 2

    # OTAP / OTLP level legend column.
    legend_x = track_x0 - 10
    out.append(f'<text x="{legend_x}" y="{band_top + 5}" text-anchor="end" '
               f'font-family="{FONT}" font-size="14" font-weight="700" '
               f'fill="{COLOR_HIGH}">OTAP</text>')
    out.append(f'<text x="{legend_x}" y="{band_bot + 5}" text-anchor="end" '
               f'font-family="{FONT}" font-size="14" font-weight="700" '
               f'fill="{COLOR_LOW}">OTLP</text>')

    out.append(f'<line x1="{track_x0}" y1="{band_top}" x2="{track_x1}" y2="{band_top}" '
               f'stroke="{COLOR_GRID}" stroke-width="{GRID_W}" stroke-dasharray="2,4"/>')
    out.append(f'<line x1="{track_x0}" y1="{band_bot}" x2="{track_x1}" y2="{band_bot}" '
               f'stroke="{COLOR_GRID}" stroke-width="{GRID_W}" stroke-dasharray="2,4"/>')
    out.append(f'<line x1="{track_x0}" y1="{mid_y}" x2="{track_x1}" y2="{mid_y}" '
               f'stroke="{COLOR_GRID}" stroke-width="{GRID_W}" stroke-dasharray="1,5"/>')

    out.append(f'<g font-family="{FONT}" fill="{COLOR_LABEL}">')
    out.append(f'<text x="{PAGE_MARGIN_X}" y="{band_top + 28}" font-size="22" font-weight="700">'
               f'{scen.title}</text>')
    out.append(f'<text x="{PAGE_MARGIN_X}" y="{band_top + 52}" font-size="14" fill="{COLOR_SUBLABEL}">'
               f'{scen.subtitle}</text>')
    out.append('</g>')

    total_w = sum(s.weight for s in scen.segments)
    track_w = track_x1 - track_x0
    xs = [track_x0]
    for s in scen.segments:
        xs.append(xs[-1] + s.weight / total_w * track_w)

    # Pre-compute the y for each non-undef segment.
    band_h = band_bot - band_top
    ys: List[Optional[float]] = [
        None if s.level == "undef" else level_y(s.level, band_top, band_bot)
        for s in scen.segments
    ]

    # Ramp horizontal run for a given vertical delta, fixed angle from horizontal.
    tan_a = math.tan(math.radians(RAMP_ANGLE_DEG))
    def ramp_run(dy: float) -> float:
        return abs(dy) / tan_a

    # For each boundary i (between segment i-1 and i), the ramp is centered on
    # x = xs[i]; each adjacent segment loses half_run of its horizontal extent.
    n = len(scen.segments)
    half_run_left  = [0.0] * n   # how much segment i loses on its left side
    half_run_right = [0.0] * n   # how much segment i loses on its right side
    boundary_ramp: List[Optional[tuple]] = [None] * n  # (y_prev, y_next, half_run)
    for i in range(1, n):
        if ys[i - 1] is None or ys[i] is None:
            continue
        if ys[i - 1] == ys[i]:
            continue
        run = ramp_run(ys[i] - ys[i - 1])
        half = run / 2.0
        # don't let the ramp eat more than 45% of either neighbouring segment
        max_left  = (xs[i]     - xs[i - 1]) * 0.45
        max_right = (xs[i + 1] - xs[i])     * 0.45
        half = min(half, max_left, max_right)
        half_run_right[i - 1] = half
        half_run_left[i]      = half
        boundary_ramp[i] = (ys[i - 1], ys[i], half)

    for i, seg in enumerate(scen.segments):
        x0, x1 = xs[i], xs[i + 1]
        color = seg_color(seg.level)

        if seg.level == "undef":
            # Bottom-half hashed box (top edge at mid_y) so it visually
            # aligns with the wire's mid-level signal.
            out.append(f'<rect x="{x0}" y="{mid_y}" width="{x1 - x0}" '
                       f'height="{band_bot - mid_y}" fill="url(#hash)" '
                       f'stroke="{COLOR_UNDEF}" stroke-width="1" opacity="0.9"/>')
        else:
            y = ys[i]
            xa = x0 + half_run_left[i]
            xb = x1 - half_run_right[i]
            out.append(f'<line x1="{xa}" y1="{y}" x2="{xb}" y2="{y}" '
                       f'stroke="{color}" stroke-width="{LINE_W}" '
                       f'stroke-linecap="square"/>')
            # ramp into this segment from previous
            br = boundary_ramp[i]
            if br is not None:
                y_prev, y_next, half = br
                xL = xs[i] - half
                xR = xs[i] + half
                out.append(f'<line x1="{xL}" y1="{y_prev}" x2="{xR}" y2="{y_next}" '
                           f'stroke="{COLOR_TRACK}" stroke-width="{LINE_W}" '
                           f'stroke-linecap="round"/>')

        out.append(f'<line x1="{x0}" y1="{band_top - 6}" x2="{x0}" y2="{band_bot + 6}" '
                   f'stroke="{COLOR_BOUNDARY}" stroke-width="0.75" stroke-dasharray="2,3"/>')

        cx = (x0 + x1) / 2
        label_lines = seg.label.split("\n")
        # stack lines upward from band_top - 14
        for li, line in enumerate(reversed(label_lines)):
            ly = band_top - 14 - li * 16
            out.append(f'<text x="{cx}" y="{ly}" text-anchor="middle" '
                       f'font-family="{FONT}" font-size="14" fill="{COLOR_LABEL}" '
                       f'font-weight="600">{line}</text>')

        if seg.sublabel:
            if seg.level in ("mid", "undef"):
                # place just above the mid-line element
                sub_y = mid_y - 8
            else:
                sub_y = band_bot + 22
            out.append(f'<text x="{cx}" y="{sub_y}" text-anchor="middle" '
                       f'font-family="{FONT}" font-size="12" fill="{COLOR_SUBLABEL}" '
                       f'font-style="italic">{seg.sublabel}</text>')

    out.append(f'<line x1="{xs[-1]}" y1="{band_top - 6}" x2="{xs[-1]}" y2="{band_bot + 6}" '
               f'stroke="{COLOR_BOUNDARY}" stroke-width="0.75" stroke-dasharray="2,3"/>')


def render_zone_headers(out: list, scen: Scenario, track_x0: float,
                        track_x1: float, header_y: float) -> None:
    total_w = sum(s.weight for s in scen.segments)
    track_w = track_x1 - track_x0
    xs = [track_x0]
    for s in scen.segments:
        xs.append(xs[-1] + s.weight / total_w * track_w)

    zones = [
        ("LOGGER (otap-dataflow)",    xs[0], xs[4], "#eef4fb"),
        ("WIRE",                      xs[4], xs[5], "#f4eefb"),
        ("COLLECTOR (otap-dataflow)", xs[5], xs[-1], "#fbf2ee"),
    ]
    for name, x0, x1, fill in zones:
        out.append(f'<rect x="{x0}" y="{header_y}" width="{x1 - x0}" height="32" '
                   f'fill="{fill}" stroke="{COLOR_BOUNDARY}" stroke-width="0.75"/>')
        out.append(f'<text x="{(x0 + x1) / 2}" y="{header_y + 21}" text-anchor="middle" '
                   f'font-family="{FONT}" font-size="13" font-weight="700" '
                   f'fill="{COLOR_LABEL}" letter-spacing="1.5">{name}</text>')


def render(scenarios: List[Scenario]) -> str:
    n = len(scenarios)
    page_h = TOP_MARGIN + n * ROW_HEIGHT + (n - 1) * ROW_GAP + BOTTOM_MARGIN
    track_x0 = PAGE_MARGIN_X + LEFT_LABEL_W + LEVEL_LABEL_W + TRACK_LEFT_PAD
    track_x1 = PAGE_W - PAGE_MARGIN_X - TRACK_RIGHT_PAD

    out: list = []
    out.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {PAGE_W} {page_h}" '
               f'width="{PAGE_W}" height="{page_h}">')
    out.append('<defs>')
    out.append(f'<pattern id="hash" patternUnits="userSpaceOnUse" width="6" height="6" '
               f'patternTransform="rotate(45)">'
               f'<rect width="6" height="6" fill="#eef0f3"/>'
               f'<line x1="0" y1="0" x2="0" y2="6" stroke="{COLOR_UNDEF}" stroke-width="1.2"/>'
               f'</pattern>')
    out.append('</defs>')
    out.append(f'<rect width="100%" height="100%" fill="{COLOR_BG}"/>')

    out.append(f'<text x="{PAGE_MARGIN_X}" y="50" font-family="{FONT}" font-size="26" '
               f'font-weight="800" fill="{COLOR_LABEL}">'
               f'Pipeline-conversion experiments: signal-level view</text>')

    render_zone_headers(out, scenarios[0], track_x0, track_x1, header_y=TOP_MARGIN - 38)

    for i, scen in enumerate(scenarios):
        y0 = TOP_MARGIN + i * (ROW_HEIGHT + ROW_GAP)
        render_row(out, y0, track_x0, track_x1, scen)

    legend_y = page_h - 36
    items = [
        (COLOR_LOW,  "OTLP"),
        (COLOR_HIGH, "OTAP"),
        (COLOR_MID,  "wire bytes"),
        (COLOR_UNDEF,"pre-ITR"),
    ]
    x = PAGE_MARGIN_X
    for color, label in items:
        out.append(f'<line x1="{x}" y1="{legend_y}" x2="{x + 28}" y2="{legend_y}" '
                   f'stroke="{color}" stroke-width="{LINE_W}"/>')
        out.append(f'<text x="{x + 36}" y="{legend_y + 5}" font-family="{FONT}" '
                   f'font-size="13" fill="{COLOR_LABEL}">{label}</text>')
        x += 36 + 8 * len(label) + 30

    out.append('</svg>')
    return "\n".join(out)


def main() -> int:
    out_path = (sys.argv[1] if len(sys.argv) > 1
                else os.path.join(os.path.dirname(__file__), "experiments.svg"))
    svg = render(SCENARIOS)
    with open(out_path, "w") as f:
        f.write(svg)
    print(f"wrote {out_path}  ({len(svg):,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
