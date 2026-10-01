"""Prepare the homepage "what you'd catch" stills.

The Real Trade Stories frames were 1920x1080 with a series header, progress
bars, and a learning-only footer. These stills keep the HappyTrader screen
and the callouts, at 1280 and 800, as WebP.

Run: python scripts/prepare_catch_stills.py
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "app" / "static" / "marketing" / "catch"
CHROME = "google-chrome"
FONT = """
@import url("https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;600;650;700&family=JetBrains+Mono:wght@500;600&display=swap");
"""

CSS = FONT + """
* { box-sizing: border-box; }
html, body { margin: 0; background: #070b14; }
body {
  width: 1280px; height: 720px; overflow: hidden;
  font-family: "Instrument Sans", ui-sans-serif, system-ui, sans-serif;
  color: #e8edf7;
}
.board {
  width: 1280px; height: 720px;
  display: grid;
  grid-template-columns: minmax(0, 1.45fr) 380px;
  gap: 18px;
  padding: 28px 32px;
  align-items: center;
}
.screen {
  background: #0e1628;
  border: 1px solid #2a3550;
  border-radius: 18px;
  padding: 16px 16px 12px;
  min-width: 0;
  height: 640px;
  display: flex;
  flex-direction: column;
}
.screen h2 {
  margin: 0;
  font-size: 20px;
  font-weight: 650;
  letter-spacing: -0.02em;
  display: flex;
  justify-content: space-between;
  gap: 12px;
}
.screen h2 span { color: #e8edf7; font-weight: 650; }
.screen svg { width: 100%; height: auto; flex: 1; }
.legend {
  display: flex; gap: 18px; justify-content: center;
  font-family: "JetBrains Mono", ui-monospace, monospace;
  font-size: 13px; color: #9aa6bd; margin-top: 4px;
}
.legend i {
  display: inline-block; width: 22px; height: 0;
  border-top: 2px dashed currentColor; margin-right: 6px; vertical-align: middle;
}
.calls { display: flex; flex-direction: column; gap: 10px; }
.call, .result {
  background: #121a2e;
  border: 1px solid #2a3550;
  border-radius: 14px;
  padding: 12px 14px;
}
.call {
  display: grid;
  grid-template-columns: 26px minmax(0, 1fr) auto;
  gap: 10px;
  align-items: center;
}
.n {
  width: 26px; height: 26px; border-radius: 50%;
  display: grid; place-items: center;
  font-size: 13px; font-weight: 700; color: #0a0e17;
}
.n.loss { background: #f0556d; }
.n.gain { background: #28c08a; }
.n.flat { background: #9aa6bd; }
.call p { margin: 0; font-size: 15px; line-height: 1.3; color: #d5ddec; }
.amt {
  font-family: "JetBrains Mono", ui-monospace, monospace;
  font-variant-numeric: tabular-nums;
  font-weight: 600; font-size: 18px; white-space: nowrap;
}
.amt.loss, .loss-t { color: #f0556d; }
.amt.gain, .gain-t { color: #28c08a; }
.amt.flat { color: #e8edf7; }
.result { border-width: 1.5px; }
.result.loss { border-color: #f0556d; }
.result.gain { border-color: #28c08a; }
.result .k { margin: 0; color: #c5d0e4; font-size: 15px; }
.result .big {
  margin: 2px 0 0;
  font-family: "JetBrains Mono", ui-monospace, monospace;
  font-variant-numeric: tabular-nums;
  font-size: 32px; font-weight: 600; letter-spacing: -0.03em;
}
.result .sub { margin: 4px 0 0; color: #9aa6bd; font-size: 14px; }
.window {
  background: #121a2e;
  border: 1px solid #2a3550;
  border-radius: 18px;
  height: 640px;
  padding: 14px 16px 18px;
  display: flex; flex-direction: column; min-width: 0;
}
.dots { display: flex; gap: 6px; margin: 2px 0 14px; }
.dots i { width: 9px; height: 9px; border-radius: 50%; background: #3a4560; }
.inner { display: flex; flex-direction: column; gap: 12px; flex: 1; justify-content: center; }
.gold {
  background: #2a2114;
  border-radius: 12px;
  padding: 18px 16px;
  font-size: 26px; font-weight: 700; letter-spacing: -0.02em; line-height: 1.25;
}
.lossbox, .hint {
  border-radius: 12px; padding: 14px 16px; font-size: 20px; font-weight: 650;
}
.lossbox { border: 1.5px solid #5b8cff; color: #f0556d; }
.hint { border: 1.5px solid #5b8cff; color: #e4c27a; line-height: 1.35; }
.pair { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; align-items: stretch; }
.strat {
  background: #0e1628; border: 1px solid #2a3550; border-radius: 14px; padding: 16px;
}
.strat h3 { margin: 0; font-size: 22px; display: flex; justify-content: space-between; align-items: center; }
.pill {
  font-size: 11px; letter-spacing: .04em; font-weight: 700;
  border: 1px solid rgba(91,140,255,.55); color: #8eabff;
  border-radius: 999px; padding: 3px 8px;
}
.meta { color: #9aa6bd; font-size: 13px; margin: 8px 0 12px; }
.stats { display: grid; grid-template-columns: 1fr 1.3fr auto; gap: 8px; }
.stats b {
  display: block; font-family: "JetBrains Mono", ui-monospace, monospace;
  font-variant-numeric: tabular-nums; font-size: 22px; font-weight: 600;
}
.stats span { color: #9aa6bd; font-size: 11px; letter-spacing: .06em; }
.sep { margin-top: 12px; color: #9aa6bd; font-size: 14px; }
.bars { display: flex; gap: 3px; margin-top: 10px; }
.bars i { width: 14px; height: 6px; border-radius: 2px; }
.foot { margin-top: auto; color: #7d8aa3; font-size: 12px; }
"""


def _xy(points, ymin, ymax, left, top, width, height):
    out = []
    n = max(len(points) - 1, 1)
    span = ymax - ymin
    for i, y in enumerate(points):
        x = left + width * (i / n)
        py = top + height * (1 - (y - ymin) / span)
        out.append((x, py))
    return out


def chart(points, ymin, ymax, yticks, hlines, markers, xticks, w=760, h=520):
    left, top, width, height = 54, 18, w - 70, h - 56
    coords = _xy(points, ymin, ymax, left, top, width, height)
    d = " ".join(("M" if i == 0 else "L") + f"{x:.1f},{y:.1f}" for i, (x, y) in enumerate(coords))
    parts = [
        f'<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">'
    ]
    for val, label in yticks:
        y = top + height * (1 - (val - ymin) / (ymax - ymin))
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left+width}" y2="{y:.1f}" stroke="#243049" stroke-width="1"/>'
            f'<text x="{left-8}" y="{y+4:.1f}" fill="#8b97ad" font-size="13" text-anchor="end" '
            f'font-family="JetBrains Mono, monospace">{label}</text>'
        )
    for val, color in hlines:
        y = top + height * (1 - (val - ymin) / (ymax - ymin))
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left+width}" y2="{y:.1f}" '
            f'stroke="{color}" stroke-width="1.5" stroke-dasharray="5 5"/>'
        )
    parts.append(
        f'<path d="{d}" fill="none" stroke="#f4f7fb" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round"/>'
    )
    n = max(len(points) - 1, 1)
    for idx, kind in markers:
        x, y = coords[idx]
        fill = {"loss": "#f0556d", "gain": "#28c08a", "flat": "#9aa6bd"}[kind]
        parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="11" fill="{fill}"/>'
            f'<text x="{x:.1f}" y="{y+4:.1f}" text-anchor="middle" fill="#0a0e17" '
            f'font-size="12" font-weight="700" font-family="Instrument Sans, sans-serif">{idx and markers and ""}</text>'
        )
    # Number markers by their order in the list, matching the callouts.
    parts = [p for p in parts if "font-weight" not in p or "idx and markers" not in p]
    # rebuild marker labels properly — the placeholder above is discarded.
    # (markers redrawn below)
    labeled = []
    for nlab, (idx, kind) in enumerate(markers, start=1):
        x, y = coords[idx]
        fill = {"loss": "#f0556d", "gain": "#28c08a", "flat": "#9aa6bd"}[kind]
        labeled.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="11" fill="{fill}"/>'
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="#f4f7fb" opacity=".35"/>'
            f'<text x="{x:.1f}" y="{y+4:.1f}" text-anchor="middle" fill="#0a0e17" '
            f'font-size="12" font-weight="700" font-family="Instrument Sans, sans-serif">{nlab}</text>'
        )
    # Drop the unlabeled circles emitted in the first loop.
    parts = [p for p in parts if "<circle" not in p]
    parts.extend(labeled)
    for frac, label in xticks:
        x = left + width * frac
        parts.append(
            f'<text x="{x:.1f}" y="{h-8}" fill="#8b97ad" font-size="13" text-anchor="middle" '
            f'font-family="Instrument Sans, sans-serif">{label}</text>'
        )
    parts.append("</svg>")
    return "\n".join(parts)


def page(body: str) -> str:
    return f"<!doctype html><html><head><meta charset='utf-8'><style>{CSS}</style></head><body>{body}</body></html>"


def onon():
    prices = [
        37.6, 37.2, 38.4, 40.4, 39.5, 41.0, 40.6, 42.7, 42.2, 42.9,
        41.8, 43.2, 43.0, 44.6, 46.2, 47.6, 45.8, 46.4, 45.2, 44.6,
        44.8, 46.6, 48.6,
    ]
    svg = chart(
        prices, 36, 50,
        [(46, "$46"), (42, "$42"), (38, "$38")],
        [(41, "#5b8cff")],
        [(4, "loss"), (7, "loss"), (22, "gain")],
        [(0, "Aug 5"), (1, "Sep 13")],
    )
    return page(f"""
    <div class="board">
      <div class="screen">
        <h2>ONON (On Holding) · daily close <span>Sep 13: $48.63</span></h2>
        {svg}
        <div class="legend"><span style="color:#5b8cff"><i></i>$41 strike</span></div>
      </div>
      <div class="calls">
        <div class="call"><div class="n loss">1</div><p>Aug 12 · bought the $41 call: paid</p><div class="amt loss">−$2.85</div></div>
        <div class="call"><div class="n loss">2</div><p>Aug 13 · sold it: received</p><div class="amt gain">+$1.53</div></div>
        <div class="call"><div class="n gain">3</div><p>Sep 13 · worth at expiration</p><div class="amt gain">$7.63</div></div>
        <div class="result loss">
          <p class="k">Realized, the whole trade</p>
          <p class="big loss-t">−$3,333</p>
          <p class="sub">about +$11,933 if held to expiration</p>
        </div>
      </div>
    </div>
    """)


def rklb():
    prices = [
        71.2, 70.4, 72.6, 70.2, 71.6, 70.0, 71.4, 70.6, 72.4, 68.6,
        68.8, 79.2, 68.2, 71.8, 66.4, 67.8, 72.6, 63.4, 57.4, 64.2,
        66.4, 67.8, 66.6, 69.2, 67.4, 70.6, 74.2, 84.8,
    ]
    svg = chart(
        prices, 54, 90,
        [(80, "$80"), (70, "$70"), (60, "$60")],
        [(69, "#9aa6bd"), (63, "#f0556d")],
        [(1, "gain"), (18, "loss"), (21, "loss"), (27, "flat")],
        [(0, "Feb 20"), (1, "Apr 17")],
    )
    return page(f"""
    <div class="board">
      <div class="screen">
        <h2>RKLB (Rocket Lab) · daily close <span>Apr 17: $84.80</span></h2>
        {svg}
        <div class="legend">
          <span><i></i>$69 · my cost</span>
          <span style="color:#f0556d"><i></i>$63 · call six strike</span>
        </div>
      </div>
      <div class="calls">
        <div class="call"><div class="n gain">1</div><p>Feb 27 – Mar 27 · five calls expired</p><div class="amt gain">+$931</div></div>
        <div class="call"><div class="n gain">2</div><p>Mar 31 · call six, $63 strike: collected</p><div class="amt gain">+$0.76</div></div>
        <div class="call"><div class="n loss">3</div><p>Apr 2 · closed $67.73: called away at</p><div class="amt loss">$63</div></div>
        <div class="call"><div class="n flat">4</div><p>Apr 17 · close</p><div class="amt flat">$84.80</div></div>
        <div class="result gain">
          <p class="k">Net, the whole run</p>
          <p class="big gain-t">+$406</p>
          <p class="sub">calls +$1,006 · shares −$600</p>
        </div>
      </div>
    </div>
    """)


def be_close():
    return page("""
    <div class="board">
      <div class="window">
        <div class="dots" aria-hidden="true"><i></i><i></i><i></i></div>
        <div class="inner">
          <div class="gold">Bought back the $320 call (Jun 26) for $6,265</div>
          <div class="lossbox">−$2,357 on the contract</div>
          <div class="hint">In hindsight: this contract expired worthless — the early close gave up $6,265 vs holding.</div>
        </div>
      </div>
      <div class="calls">
        <div class="result loss">
          <p class="k">The loss, on the trade</p>
          <p class="big loss-t">−$2,357</p>
        </div>
        <div class="result loss">
          <p class="k">In hindsight</p>
          <p class="sub" style="font-size:18px;color:#e8edf7;margin-top:8px">the early close gave up <span class="loss-t">$6,265</span></p>
        </div>
      </div>
    </div>
    """)


def _pnl_svg():
    # Shape only: mid-June peak, late-July low, late-September recovery.
    # Values are thousands of dollars, matching the callouts (about).
    total = [
        0, 2, -1, 9, 8, 11, 7, 13, 6, 14, 15, 8, 4, 12, 22, 24, 16, 11,
        4, 2, -2, -6, -4, -12, -15, -8, -3, 1, -2, 2, 6, 3, 7, 8, 5, 11,
    ]
    equity = [v * 0.55 + 1 for v in total]
    options = [v - e for v, e in zip(total, equity)]
    w, h = 820, 500
    left, top, width, height = 58, 16, w - 74, h - 48
    ymin, ymax = -20, 28

    def path(series, color, width_px):
        coords = _xy(series, ymin, ymax, left, top, width, height)
        d = " ".join(("M" if i == 0 else "L") + f"{x:.1f},{y:.1f}" for i, (x, y) in enumerate(coords))
        return f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width_px}" stroke-linejoin="round"/>'

    ticks = [(25, "$25.0k"), (15, "$15.0k"), (5, "$5.0k"), (0, "$0"), (-5, "−$5.0k"), (-15, "−$15.0k")]
    lines = []
    for val, label in ticks:
        y = top + height * (1 - (val - ymin) / (ymax - ymin))
        lines.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left+width}" y2="{y:.1f}" stroke="#243049"/>'
            f'<text x="{left-6}" y="{y+4:.1f}" fill="#8b97ad" font-size="11" text-anchor="end" '
            f'font-family="JetBrains Mono, monospace">{label}</text>'
        )
    zero = top + height * (1 - (0 - ymin) / (ymax - ymin))
    # Highlight the three callout regions.
    def box(i0, i1):
        n = len(total) - 1
        x0 = left + width * (i0 / n)
        x1 = left + width * (i1 / n)
        ys = [_xy(total, ymin, ymax, left, top, width, height)[i][1] for i in range(i0, i1 + 1)]
        y0, y1 = min(ys) - 16, max(ys) + 16
        return (
            f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{x1-x0:.1f}" height="{y1-y0:.1f}" '
            f'rx="8" fill="none" stroke="#5b8cff" stroke-width="1.5"/>'
        )
    dates = ["2026-04-23", "2026-06-11", "2026-07-17", "2026-08-18", "2026-09-22"]
    labels = []
    for i, lab in enumerate(dates):
        x = left + width * (i / (len(dates) - 1))
        labels.append(
            f'<text x="{x:.1f}" y="{h-6}" fill="#8b97ad" font-size="11" text-anchor="middle" '
            f'font-family="JetBrains Mono, monospace">{lab}</text>'
        )
    return f'''<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      {''.join(lines)}
      <line x1="{left}" y1="{zero:.1f}" x2="{left+width}" y2="{zero:.1f}" stroke="#28c08a" stroke-width="1.2"/>
      {path(options, "#7aa2ff", 1.6)}
      {path(equity, "#5b8cff", 1.8)}
      {path(total, "#f4f7fb", 2.2)}
      {box(14, 16)}{box(23, 25)}{box(33, 35)}
      {''.join(labels)}
    </svg>'''


def be_swing():
    return page(f"""
    <div class="board">
      <div class="window">
        <div class="dots" aria-hidden="true"><i></i><i></i><i></i></div>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
          <strong style="font-size:14px">Cumulative P&amp;L</strong>
          <span style="color:#9aa6bd;font-size:12px">Show BE price</span>
        </div>
        <div class="legend" style="justify-content:flex-start;margin:0 0 4px">
          <span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#f4f7fb;margin-right:6px"></span>Total</span>
          <span style="color:#5b8cff">Equity</span>
          <span style="color:#7aa2ff">Options</span>
        </div>
        {_pnl_svg()}
        <p class="foot">Dots mark trade days. Click a day in the review to highlight it.</p>
      </div>
      <div class="calls">
        <div class="result gain"><p class="k">Mid-June peak</p><p class="big gain-t">about +$24k</p></div>
        <div class="result loss"><p class="k">Late-July low</p><p class="big loss-t">about −$15k</p></div>
        <div class="result gain"><p class="k">Late September</p><p class="big gain-t">about +$11k</p></div>
      </div>
    </div>
    """)


def _bars(pattern):
    bits = []
    for kind in pattern:
        color = "#28c08a" if kind == "g" else "#f0556d"
        bits.append(f'<i style="background:{color}"></i>')
    return "".join(bits)


def win_rate():
    return page(f"""
    <div class="board">
      <div class="window">
        <div class="dots" aria-hidden="true"><i></i><i></i><i></i></div>
        <div class="pair" style="margin-top:28px">
          <div class="strat">
            <h3>Long Call <span class="pill">↑ Improving</span></h3>
            <p class="meta">218 trades · 54 symbols · 66 positions</p>
            <div class="stats">
              <div><span>WIN RATE</span><b>44%</b></div>
              <div><span>TOTAL RETURN</span><b class="gain-t">+$45,487</b></div>
              <div><span>AVG HOLD</span><b>20d</b></div>
            </div>
            <div class="bars">{_bars("rrgrrg")}</div>
            <p class="sep">Sep <span class="loss-t">−$4,222</span></p>
          </div>
          <div class="strat">
            <h3>Covered Call <span class="pill">↑ Improving</span></h3>
            <p class="meta">662 trades · 39 symbols · 52 positions</p>
            <div class="stats">
              <div><span>WIN RATE</span><b>74%</b></div>
              <div><span>TOTAL RETURN</span><b class="gain-t">+$14,677</b></div>
              <div><span>AVG HOLD</span><b>11d</b></div>
            </div>
            <div class="bars">{_bars("gggrgg")}</div>
            <p class="sep">Sep <span class="loss-t">−$355</span></p>
          </div>
        </div>
      </div>
      <div class="calls">
        <div class="result gain">
          <p class="k">Covered Call</p>
          <p class="sub" style="font-size:18px;color:#e8edf7">74% win rate · <span class="gain-t">+$14,677</span></p>
        </div>
        <div class="result gain">
          <p class="k">Long Call</p>
          <p class="sub" style="font-size:18px;color:#e8edf7">44% win rate · <span class="gain-t">+$45,487</span></p>
        </div>
      </div>
    </div>
    """)


STORIES = {
    "onon": onon,
    "rklb": rklb,
    "be-close": be_close,
    "be-swing": be_swing,
    "win-rate": win_rate,
}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    work = Path("/tmp/catch-stills")
    work.mkdir(parents=True, exist_ok=True)
    for name, builder in STORIES.items():
        html = work / f"{name}.html"
        html.write_text(builder(), encoding="utf-8")
        png = work / f"{name}.png"
        # Headless Chrome writes the PNG, then stays up. timeout ends it.
        shot = subprocess.run([
            "timeout", "20",
            CHROME, "--headless=new", "--disable-gpu", "--no-sandbox",
            "--hide-scrollbars",
            f"--user-data-dir=/tmp/chrome-catch-{name}",
            "--force-device-scale-factor=1",
            "--virtual-time-budget=5000",
            "--window-size=1280,720",
            f"--screenshot={png}",
            html.as_uri(),
        ], check=False)
        if not png.exists() or png.stat().st_size < 1000:
            raise SystemExit(f"screenshot failed for {name} (chrome {shot.returncode})")
        subprocess.check_call([
            "ffmpeg", "-y", "-i", str(png),
            "-c:v", "libwebp", "-quality", "78", "-compression_level", "6",
            str(OUT / f"{name}.webp"),
        ])
        subprocess.check_call([
            "ffmpeg", "-y", "-i", str(png),
            "-vf", "scale=800:-1",
            "-c:v", "libwebp", "-quality", "76", "-compression_level", "6",
            str(OUT / f"{name}-800.webp"),
        ])
        print(name, png.stat().st_size, (OUT / f"{name}.webp").stat().st_size)


if __name__ == "__main__":
    main()
