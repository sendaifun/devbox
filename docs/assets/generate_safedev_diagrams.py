#!/usr/bin/env python3
from pathlib import Path
import math
import random

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
WIDTH = 1400
HEIGHT = 820
SCALE = 2
INK = "#27231d"
MUTED = "#5f574f"
BG = "#fbf7ef"
PAPER = "#fffdf8"
YELLOW = "#fff7d6"
GREEN = "#d9f0e8"
BLUE = "#deebff"
RED = "#fff0eb"
OFF_WHITE = "#f8fbff"


def font(size, bold=False, mono=False):
    candidates = []
    if mono:
        candidates = [
            "/System/Library/Fonts/SFNSMono.ttf",
            "/System/Library/Fonts/Menlo.ttc",
        ]
    elif bold:
        candidates = [
            "/System/Library/Fonts/SFNS.ttf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/System/Library/Fonts/HelveticaNeue.ttc",
        ]
    else:
        candidates = [
            "/System/Library/Fonts/SFNS.ttf",
            "/System/Library/Fonts/HelveticaNeue.ttc",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
        ]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size * SCALE)
        except Exception:
            pass
    return ImageFont.load_default()


TITLE = font(42, bold=True)
SUBTITLE = font(22)
LABEL = font(26, bold=True)
BODY = font(18)
SMALL = font(16)
CODE = font(17, mono=True)


class Canvas:
    def __init__(self):
        self.image = Image.new("RGB", (WIDTH * SCALE, HEIGHT * SCALE), BG)
        self.draw = ImageDraw.Draw(self.image)

    def xy(self, values):
        return tuple(int(round(v * SCALE)) for v in values)

    def text(self, xy, value, fill=INK, font_obj=BODY, anchor=None):
        self.draw.text(self.xy(xy), value, fill=fill, font=font_obj, anchor=anchor)

    def rounded(self, box, fill, outline=INK, width=3, radius=18):
        self.draw.rounded_rectangle(
            self.xy(box),
            radius=radius * SCALE,
            fill=fill,
            outline=outline,
            width=width * SCALE,
        )

    def chip(self, box, text, fill=OFF_WHITE, mono=True, text_fill=INK):
        self.rounded(box, fill=fill, width=2, radius=11)
        x1, y1, _, _ = box
        self.text((x1 + 18, y1 + 12), text, fill=text_fill, font_obj=CODE if mono else SMALL)

    def line(self, points, fill=INK, width=3):
        self.draw.line([self.xy(p) for p in points], fill=fill, width=width * SCALE, joint="curve")

    def arrow(self, start, end, fill=INK, width=4):
        self.line([start, end], fill=fill, width=width)
        sx, sy = start
        ex, ey = end
        angle = math.atan2(ey - sy, ex - sx)
        length = 20
        spread = 0.55
        p1 = (ex, ey)
        p2 = (ex - length * math.cos(angle - spread), ey - length * math.sin(angle - spread))
        p3 = (ex - length * math.cos(angle + spread), ey - length * math.sin(angle + spread))
        self.draw.polygon([self.xy(p1), self.xy(p2), self.xy(p3)], fill=fill)

    def dashed(self, points, fill=INK, width=3, dash=12, gap=10):
        for a, b in zip(points, points[1:]):
            ax, ay = a
            bx, by = b
            dx = bx - ax
            dy = by - ay
            distance = math.hypot(dx, dy)
            if distance == 0:
                continue
            steps = int(distance // (dash + gap)) + 1
            for i in range(steps):
                start = i * (dash + gap)
                end = min(start + dash, distance)
                if start >= distance:
                    break
                p1 = (ax + dx * start / distance, ay + dy * start / distance)
                p2 = (ax + dx * end / distance, ay + dy * end / distance)
                self.line([p1, p2], fill=fill, width=width)

    def finish(self, path):
        # Add a little paper texture, then downsample for antialiasing.
        random.seed(42)
        pixels = self.image.load()
        for _ in range(24000):
            x = random.randrange(self.image.width)
            y = random.randrange(self.image.height)
            r, g, b = pixels[x, y]
            delta = random.choice((-3, -2, 2, 3))
            pixels[x, y] = (
                max(0, min(255, r + delta)),
                max(0, min(255, g + delta)),
                max(0, min(255, b + delta)),
            )
        self.image = self.image.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS)
        self.image.save(ROOT / path, "PNG", optimize=True)


def isolation_flow():
    c = Canvas()
    c.rounded((24, 24, 1376, 796), fill=PAPER, outline="#ded4c5", width=2, radius=34)
    c.text((70, 72), "Unknown code runs in the box, not on your Mac", font_obj=TITLE)
    c.text(
        (72, 117),
        "Run installs, dev servers, and Codex inside a disposable per-project VM.",
        fill=MUTED,
        font_obj=SUBTITLE,
    )

    c.rounded((70, 195, 350, 492), fill=YELLOW, width=4, radius=18)
    c.text((104, 240), "Unknown repo", font_obj=LABEL)
    c.text((105, 276), "Clone first. Trust later.", fill=MUTED, font_obj=BODY)
    c.chip((104, 322, 306, 364), "pnpm install", fill="#fff2d9")
    c.chip((104, 379, 276, 421), "pip install", fill="#fff2d9")
    c.chip((104, 436, 284, 478), "cargo build", fill="#fff2d9")

    c.arrow((370, 335), (475, 335))

    c.rounded((500, 260, 690, 410), fill=GREEN, width=4, radius=18)
    c.text((548, 308), "SafeDev", font_obj=LABEL)
    c.text((548, 343), "CLI broker", fill=MUTED, font_obj=BODY)
    c.text((548, 374), "safedev up", font_obj=CODE)

    c.arrow((710, 335), (815, 335))

    c.rounded((835, 175, 1218, 552), fill=BLUE, width=4, radius=18)
    c.text((872, 221), "Per-project Linux VM", font_obj=LABEL)
    c.text((873, 255), "Risky commands execute here.", fill=MUTED, font_obj=BODY)
    c.chip((872, 303, 1057, 345), "/workspaces/repo")
    c.text((1078, 315), "writable project", fill=MUTED, font_obj=SMALL)
    c.chip((872, 360, 1005, 402), "/home/dev")
    c.text((1026, 372), "synthetic home", fill=MUTED, font_obj=SMALL)
    c.chip((872, 417, 1014, 459), "Codex CLI")
    c.chip((1034, 417, 1160, 459), "tools")
    c.chip((872, 474, 1023, 516), "dev server")
    c.chip((1043, 474, 1176, 516), "snapshot")

    c.arrow((1235, 335), (1303, 335))
    c.rounded((1230, 270, 1340, 425), fill="#e8eadf", width=4, radius=18)
    c.text((1285, 310), "Keep", font_obj=LABEL, anchor="mm")
    c.text((1285, 350), "or", font_obj=LABEL, anchor="mm")
    c.text((1285, 390), "destroy", font_obj=LABEL, anchor="mm")

    c.dashed([(70, 628), (320, 604), (600, 618), (875, 644), (1210, 620), (1330, 630)], width=3)
    c.text((72, 645), "Host Mac boundary", font_obj=LABEL)
    c.text(
        (72, 681),
        "SafeDev does not mount broad host credential stores by default.",
        fill=MUTED,
        font_obj=BODY,
    )
    x = 72
    for label, width in [
        ("~/.ssh", 118),
        ("~/.aws", 118),
        ("browser profiles", 198),
        ("Docker socket", 176),
        ("full ~/.codex", 178),
        ("cloud credentials", 212),
    ]:
        c.chip((x, 714, x + width, 756), label, fill=RED)
        x += width + 16
    c.finish("safedev-isolation-flow.png")


def project_detection():
    c = Canvas()
    c.rounded((24, 24, 1376, 796), fill=PAPER, outline="#ded4c5", width=2, radius=34)
    c.text((70, 72), "Monorepos become one VM toolchain plan", font_obj=TITLE)
    c.text(
        (72, 117),
        "SafeDev scans every package, combines the detected needs, then provisions once.",
        fill=MUTED,
        font_obj=SUBTITLE,
    )

    c.rounded((70, 190, 422, 640), fill=YELLOW, width=4, radius=18)
    c.text((108, 236), "Repo scan", font_obj=LABEL)
    c.text((109, 271), "The root can contain many packages.", fill=MUTED, font_obj=BODY)
    c.line([(122, 333), (122, 555)], width=4)
    for y in [355, 415, 475, 535]:
        c.line([(122, y), (156, y)], width=4)
    c.chip((156, 333, 378, 377), "web/package.json", fill="#fff2d9")
    c.chip((156, 393, 382, 437), "agent/Cargo.toml", fill="#fff2d9")
    c.chip((156, 453, 394, 497), "api/pyproject.toml", fill="#fff2d9")
    c.chip((156, 513, 284, 557), "Tiltfile", fill="#fff2d9")

    c.arrow((445, 415), (545, 415))

    c.rounded((570, 222, 838, 608), fill=GREEN, width=4, radius=18)
    c.text((608, 269), "SafeDev profile", font_obj=LABEL)
    c.text((609, 304), "Combined, not first-match.", fill=MUTED, font_obj=BODY)
    for label, desc, y in [
        ("JS/TS", "package managers", 340),
        ("Rust", "cargo builds", 400),
        ("Python", "venv and uv", 460),
        ("Tilt", "orchestration", 520),
    ]:
        c.chip((610, y, 706, y + 42), label, fill="#f7fffc")
        c.text((726, y + 12), desc, fill=MUTED, font_obj=SMALL)

    c.arrow((860, 415), (960, 415))

    c.rounded((985, 190, 1330, 640), fill=BLUE, width=4, radius=18)
    c.text((1023, 236), "Provisioned inside VM", font_obj=LABEL)
    c.text((1024, 271), "The host stays outside setup.", fill=MUTED, font_obj=BODY)
    for text, y, width in [
        ("node + corepack + pnpm", 333, 265),
        ("rustc + cargo", 393, 188),
        ("python + uv", 453, 162),
        ("tilt", 513, 95),
        ("Codex CLI", 573, 145),
    ]:
        c.chip((1024, y, 1024 + width, y + 44), text, fill="#f7fbff")

    c.text(
        (420, 700),
        "A repo can be JavaScript, Rust, Python, Tilt, or all of them at once.",
        fill=MUTED,
        font_obj=BODY,
    )
    c.text(
        (420, 733),
        "SafeDev prepares the box for the whole tree before installs or Codex run.",
        fill=MUTED,
        font_obj=BODY,
    )
    c.finish("safedev-project-detection.png")


if __name__ == "__main__":
    isolation_flow()
    project_detection()
