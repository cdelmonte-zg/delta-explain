#!/usr/bin/env python3
"""Regenerate the README terminal shots in assets/readme/ from real output.

Each shot is a command run against the built binary, its output framed in
a terminal window (HTML) and rendered by headless Chromium at 2x with a
transparent background:

  cli-demo.png     a two-column predicate on a local table
  cli-verbose.png  the per-file listing behind --verbose
  cli-catalog.png  a Unity Catalog table read by name

The first two use the checked-in fixture; the third needs the catalog
stack from examples/catalogs with a table registered as
sales.events.orders over a RustFS bucket (see RELEASE.md). Run after a
version bump, so the output matches the release:

    cargo build --release
    python3 scripts/render_readme_shots.py [--bin PATH] [--only NAME ...]
"""

import argparse
import html
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "assets" / "readme"
FIXTURE = REPO / "fixtures" / "test-table"

# Local catalog stack (examples/catalogs): the resolver's two variables
# and the RustFS transport, which stays user-controlled in catalog mode.
CATALOG_ENV = {"DATABRICKS_HOST": "http://localhost:8080", "DATABRICKS_TOKEN": "unused"}
RUSTFS_TRANSPORT = [
    "--option", "aws_endpoint=http://localhost:9020",
    "--option", "aws_allow_http=true",
    "--option", "aws_virtual_hosted_style_request=false",
    "--option", "aws_region=us-east-1",
]

# name -> (shown command, argv after the binary, CSS width, env, output filter)
SHOTS = {
    "cli-demo": dict(
        shown='delta-explain ./my-table --where "age > 40 AND country = \'DE\'"',
        argv=["./my-table", "--where", "age > 40 AND country = 'DE'"],
        width=700,
    ),
    "cli-verbose": dict(
        shown='delta-explain ./my-table -w "country = \'DE\'" --verbose',
        argv=["./my-table", "-w", "country = 'DE'", "--verbose"],
        width=1000,
        keep=lambda lines: condense_listing(from_phase(lines)),
    ),
    "cli-catalog": dict(
        shown='delta-explain --catalog unity sales.events.orders --where "age > 40 AND country = \'DE\'"',
        argv=["--catalog", "unity", "sales.events.orders",
              "--where", "age > 40 AND country = 'DE'", *RUSTFS_TRANSPORT],
        width=700,
        env=CATALOG_ENV,
    ),
}

CSS = """
html, body { margin: 0; background: transparent; }
.win { display: inline-block; min-width: %(width)dpx; box-sizing: border-box;
       background: #151a21; border-radius: 10px; overflow: hidden;
       font: 13px/20px "DejaVu Sans Mono", "SF Mono", Menlo, monospace; color: #d0d4da; }
.bar { height: 34px; background: #1e242c; display: flex; align-items: center; padding-left: 16px; }
.dot { width: 12px; height: 12px; border-radius: 50%%; margin-right: 8px; }
pre { margin: 0; padding: 24px 19px 20px 19px; white-space: pre; }
.p { color: #3fb950; } .c { color: #ffffff; font-weight: bold; }
.h { color: #58a6ff; font-weight: bold; }
.d { color: #f85149; } .k { color: #3fb950; }
"""

HEADINGS = ("Delta table:", "Predicate Analysis:", "Phase ", "Total reduction:")


def from_phase(lines):
    return lines[lines.index(next(l for l in lines if l.startswith("Phase 1"))):]


def condense_listing(lines):
    """The per-file rows as the README has always shown them: the file
    name cut to its first id group and the stats to the `age` column, so
    the listing fits a 1000px frame. Every value shown is real."""
    out = []
    for line in lines:
        line = re.sub(r"(part-\d+-[0-9a-f]{8})-[0-9a-f-]+(?:\.c\d+)?(?:\.snappy)?\.parquet",
                      r"\1.parquet", line)
        line = re.sub(r"stats\((.*)\)$",
                      lambda m: "stats(" + ", ".join(
                          part for part in m.group(1).split(", ") if part.startswith("age:")) + ")",
                      line)
        out.append(line)
    return out


def colorize(line):
    text = html.escape(line)
    if line.startswith(HEADINGS):
        return f'<span class="h">{text}</span>'
    text = text.replace("[DROPPED]", '<span class="d">[DROPPED]</span>')
    text = re.sub(r"\[KEPT( *)\]", r'<span class="k">[KEPT\1]</span>', text)
    return text


def frame(shown, lines, width):
    body = "\n".join(colorize(l) for l in lines)
    prompt = f'<span class="p">$</span> <span class="c">{html.escape(shown)}</span>'
    return f"""<!doctype html><meta charset="utf-8"><style>{CSS % {"width": width}}</style>
<div class="win"><div class="bar"><span class="dot" style="background:#ff5f57"></span>
<span class="dot" style="background:#febc2e"></span><span class="dot" style="background:#28c840"></span></div>
<pre>{prompt}

{body}</pre></div>"""


def capture(binary, spec, workdir):
    env = {**os.environ, **spec.get("env", {})}
    proc = subprocess.run([binary, *spec["argv"]], cwd=workdir, env=env,
                          capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"{spec['shown']}\n{proc.stderr}")
    lines = proc.stdout.rstrip("\n").split("\n")
    return spec.get("keep", lambda l: l)(lines)


def render(chromium, html_path, png_path):
    # The snap-confined browser cannot read /tmp: work files stay in the repo.
    subprocess.run([chromium, "--headless=new", "--hide-scrollbars",
                    "--force-device-scale-factor=2", "--default-background-color=00000000",
                    "--window-size=2200,1600", f"--screenshot={png_path}",
                    f"file://{html_path}"], check=True, capture_output=True)
    from PIL import Image
    with Image.open(png_path) as im:
        im = im.convert("RGBA")
        cropped = im.crop(im.getbbox())
        cropped.save(png_path)
        return cropped.size


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--bin", default=str(REPO / "target" / "release" / "delta-explain"))
    parser.add_argument("--only", nargs="*", choices=sorted(SHOTS), default=sorted(SHOTS))
    parser.add_argument("--out", default=str(OUT_DIR))
    args = parser.parse_args()

    chromium = shutil.which("chromium") or shutil.which("chromium-browser") \
        or shutil.which("google-chrome") or sys.exit("no chromium on PATH")
    work = REPO / "target" / "readme-shots"
    work.mkdir(parents=True, exist_ok=True)
    table = work / "my-table"
    if table.exists():
        shutil.rmtree(table)
    shutil.copytree(FIXTURE, table)

    for name in args.only:
        spec = SHOTS[name]
        lines = capture(str(Path(args.bin).resolve()), spec, work)
        html_path = work / f"{name}.html"
        html_path.write_text(frame(spec["shown"], lines, spec["width"]))
        png_path = Path(args.out) / f"{name}.png"
        size = render(chromium, html_path, png_path)
        print(f"{png_path}: {size[0]}x{size[1]}, {len(lines)} lines")


if __name__ == "__main__":
    main()
