#!/usr/bin/env python3
"""Roadmap slide -- a simple unordered bullet list of upcoming work.

Lives outside ``out/`` because ``regenerate.sh`` wipes that directory; the
script is invoked by hand:

    python3 gen_roadmap.py

Edit ``BULLETS`` to add / reorder / rename items. Edit ``TITLE``,
``URN``, ``SUBTITLE`` to tweak the header.
"""

from __future__ import annotations
import os
import sys
from typing import List

from node_lib import (
    page_open, page_close, title_bar,
    COLOR_OTAP, COLOR_LABEL, COLOR_SUBLABEL,
    FS_SUBTITLE,
    SLIDE_PAGE_W, SLIDE_PAGE_H, SLIDE_MARGIN_X,
    FONT,
)


# -------------------------------------------------------------- content

TITLE    = "Roadmap"
URN      = "what's next"
SUBTITLE = "Where OpenTelemetry Arrow is going."

BULLETS: List[str] = [
    "Naming",
    "Extensions",
    "Binary Distro",
    "Multi-tenant",
    "Plugins (Web Assembly)",
    "NUMA configuration",
    "OTel Profiles",
    "More query languages",
]


# --------------------------------------------------------------- layout

PAGE_W = SLIDE_PAGE_W
PAGE_H = SLIDE_PAGE_H

# Where the bullet column starts and how the bullets are spaced.
BULLET_X      = 220       # left x of bullet glyph
TEXT_DX       = 28        # gap between bullet glyph and label
BULLET_R      = 7         # bullet circle radius
ROW_H         = 70        # vertical spacing between bullets
BODY_TOP_Y    = 130       # top of the body area (just below subtitle)
BODY_BOT_Y    = PAGE_H - 40
FONT_SIZE     = 34


def _bullet(cx: float, cy: float, label: str) -> str:
    """One bullet row: a filled OTAP-blue circle plus the label text."""
    return (
        f'<g transform="translate({cx} {cy})">'
        f'<circle cx="0" cy="-10" r="{BULLET_R}" fill="{COLOR_OTAP}"/>'
        f'<text x="{TEXT_DX}" y="0" font-size="{FONT_SIZE}" '
        f'fill="{COLOR_LABEL}">{label}</text>'
        f'</g>'
    )


def render() -> str:
    out: List[str] = []
    out.append(page_open(PAGE_W, PAGE_H))

    out.append(title_bar(
        SLIDE_MARGIN_X, 60, PAGE_W - 2 * SLIDE_MARGIN_X,
        title=TITLE, urn=URN, accent=COLOR_OTAP,
    ))
    out.append(
        f'<text x="{SLIDE_MARGIN_X}" y="90" font-size="{FS_SUBTITLE}" '
        f'font-style="italic" fill="{COLOR_SUBLABEL}">{SUBTITLE}</text>'
    )

    # Vertically center the bullet column in the body area.
    n = len(BULLETS)
    list_h = (n - 1) * ROW_H if n > 0 else 0
    body_cy = (BODY_TOP_Y + BODY_BOT_Y) / 2
    first_y = body_cy - list_h / 2

    for i, label in enumerate(BULLETS):
        out.append(_bullet(BULLET_X, first_y + i * ROW_H, label))

    out.append(page_close())
    return "".join(out)


def main(argv: List[str]) -> int:
    default = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "static", "09b-roadmap.svg",
    )
    out_path = argv[1] if len(argv) > 1 else default
    svg = render()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out_path} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
