#!/usr/bin/env python3
"""Bundle the slideshow into a single PDF.

Reads the SLIDES list from index.html (so it stays in sync with whatever
order you've set in the viewer), generates a print-only HTML where each
slide is one 1600x850 page, then drives headless Chrome to print it.

Requires google-chrome (or chromium) on $PATH. No Python deps.

Usage:
    python3 make_pdf.py                 # writes slides.pdf
    python3 make_pdf.py out.pdf
"""

from __future__ import annotations
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List

HERE = Path(__file__).resolve().parent


def read_slide_paths() -> List[str]:
    """Pull the SLIDES array out of index.html."""
    html = (HERE / "index.html").read_text(encoding="utf-8")
    m = re.search(r"const SLIDES\s*=\s*\[(.*?)\];", html, re.S)
    if not m:
        sys.exit("could not find SLIDES array in index.html")
    return re.findall(r'"([^"]+\.svg)"', m.group(1))


PRINT_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>slides</title>
<style>
  @page {{ size: 1600px 850px; margin: 0; }}
  html, body {{ margin: 0; padding: 0; background: #fff; }}
  .slide {{
    width: 1600px; height: 850px;
    display: flex; align-items: center; justify-content: center;
    page-break-after: always; break-after: page; overflow: hidden;
  }}
  .slide:last-child {{ page-break-after: auto; break-after: auto; }}
  .slide img {{ max-width: 100%; max-height: 100%;
                width: auto; height: auto; display: block; }}
</style></head><body>
{body}
</body></html>
"""


def find_chrome() -> str:
    for name in ("google-chrome", "chromium", "chromium-browser",
                 "google-chrome-stable"):
        p = shutil.which(name)
        if p:
            return p
    sys.exit("no chrome/chromium found on $PATH")


def main(argv: List[str]) -> int:
    out_pdf = Path(argv[1]) if len(argv) > 1 else HERE / "slides.pdf"
    out_pdf = out_pdf.resolve()

    slides = read_slide_paths()
    print(f"found {len(slides)} slides in index.html")

    body = "\n".join(
        f'  <div class="slide"><img src="{p}" alt=""></div>' for p in slides
    )
    html = PRINT_HTML.format(body=body)

    # Drop the temp HTML next to the SVGs so relative paths resolve.
    with tempfile.NamedTemporaryFile(
        "w", suffix=".html", dir=HERE, delete=False, encoding="utf-8",
    ) as fh:
        fh.write(html)
        tmp_html = Path(fh.name)

    try:
        chrome = find_chrome()
        cmd = [
            chrome,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--no-pdf-header-footer",
            f"--print-to-pdf={out_pdf}",
            tmp_html.as_uri(),
        ]
        print("running:", " ".join(cmd))
        rc = subprocess.call(cmd)
    finally:
        tmp_html.unlink(missing_ok=True)

    if rc != 0:
        return rc
    print(f"wrote {out_pdf}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
