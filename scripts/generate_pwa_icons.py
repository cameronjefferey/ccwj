"""Generate PWA / home-screen icons from the favicon.svg design.

Reproduces the mark (navy diagonal gradient, rounded corners, white
chart-line zigzag) as PNGs at the sizes the web app manifest and iOS
need. Pure PIL so it runs anywhere without an SVG rasterizer.

Usage: .venv/bin/python scripts/generate_pwa_icons.py
"""

import pathlib

from PIL import Image, ImageDraw

OUT = pathlib.Path(__file__).resolve().parent.parent / "app" / "static" / "icons"

# favicon.svg geometry, in its 32x32 viewbox: an axis stroke up the left
# and a zigzag price line. (M6 22 V10, then l4 4, 4 -4, 4 8, 4 -4, 4 8)
AXIS = [(6, 22), (6, 10)]
ZIGZAG = [(6, 10), (10, 14), (14, 10), (18, 18), (22, 14), (26, 22)]

NAVY_A = (26, 26, 46)    # #1a1a2e
NAVY_B = (22, 33, 62)    # #16213e


def _gradient(size):
    img = Image.new("RGB", (size, size))
    px = img.load()
    for y in range(size):
        for x in range(size):
            t = (x + y) / (2 * size - 2)
            px[x, y] = tuple(
                round(a + (b - a) * t) for a, b in zip(NAVY_A, NAVY_B)
            )
    return img


def _rounded_mask(size, radius):
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        [0, 0, size - 1, size - 1], radius=radius, fill=255
    )
    return mask


def make_icon(size, *, content_scale=1.0, rounded=True, path):
    """content_scale < 1 shrinks the mark toward the center (maskable
    icons need the mark inside the ~80% safe zone)."""
    img = _gradient(size)
    draw = ImageDraw.Draw(img)

    s = size / 32.0 * content_scale
    off = (size - 32 * s) / 2.0
    stroke = max(2, round(2 * s))

    def pts(seq):
        return [(off + x * s, off + y * s) for x, y in seq]

    for seq in (AXIS, ZIGZAG):
        draw.line(pts(seq), fill=(255, 255, 255), width=stroke, joint="curve")
        # round caps
        r = stroke / 2.0
        for x, y in (pts(seq)[0], pts(seq)[-1]):
            draw.ellipse([x - r, y - r, x + r, y + r], fill=(255, 255, 255))

    if rounded:
        out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        out.paste(img, (0, 0), _rounded_mask(size, round(size * 6 / 32)))
        out.save(path)
    else:
        img.save(path)
    print(f"wrote {path}")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    make_icon(192, path=OUT / "icon-192.png")
    make_icon(512, path=OUT / "icon-512.png")
    # Maskable: full-bleed square, mark shrunk into the OS mask safe zone.
    make_icon(512, content_scale=0.62, rounded=False,
              path=OUT / "icon-maskable-512.png")
    # iOS home screen: opaque, no rounding (iOS applies its own).
    make_icon(180, content_scale=0.78, rounded=False,
              path=OUT / "apple-touch-icon.png")


if __name__ == "__main__":
    main()
