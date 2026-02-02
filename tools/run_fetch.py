#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Osobný spravodaj – MVP runtime fetch (RSS + basic HTML list)

Adds support for fetch.method:
  - rss
  - html_list (heuristic extraction from urls.home)

Usage:
  python tools/run_fetch.py --registry build/registry.json --sources BA_CITY_NEWS,BSK_RSS,SR_ZJAZD,EU_EP_RSS --outdir out --limit 40
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import tz
from dateutil.parser import parse as dt_parse

UA = "osobny-spravodaj/0.2 (+https://github.com/rewizard001/osobny-spravodaj)"
TIMEOUT = 25

# ---------------- basics ----------------

def now_local() -> dt.datetime:
    return dt.datetime.now(tz=tz.tzlocal())

def to_date(d: dt.datetime) -> dt.date:
    return d.astimezone(tz.tzlocal()).date()

def safe_text(x: Any) -> str:
    return "" if x is None else str(x).strip()

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def norm_key(s: str) -> str:
    s = norm_space(s).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def strip_utm(url: str) -> str:
    if not url:
        return url
    url = re.sub(r"(\?|&)(utm_[^=]+|fbclid|gclid|mc_cid|mc_eid)=[^&#]+", "", url, flags=re.IGNORECASE)
    url = re.sub(r"[?&]+$", "", url)
    return url

def http_get(url: str) -> str:
    r = requests.get(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, text/html;q=0.8, */*;q=0.7",
        },
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.text

def parse_entry_datetime(entry: Any) -> Optional[dt.datetime]:
    for k in ("published_parsed", "updated_parsed"):
        t = getattr(entry, k, None) if hasattr(entry, k) else entry.get(k)
        if t:
            try:
                return dt.datetime(*t[:6], tzinfo=tz.tzutc()).astimezone(tz.tzlocal())
            except Exception:
                pass
    for k in ("published", "updated", "date"):
        s = safe_text(getattr(entry, k, None) if hasattr(entry, k) else entry.get(k))
        if s:
            try:
                d = dt_parse(s, fuzzy=True)
                if d.tzinfo is None:
                    d = d.replace(tzinfo=tz.tzutc())
                return d.astimezone(tz.tzlocal())
            except Exception:
                continue
    return None

# ---------------- scoring ----------------

GEO_WEIGHT = {"BA": 3, "BSK": 2, "SR": 1, "SUSEDIA": 1, "EU_GLOBAL": 1}

def time_score(pub_dt: Optional[dt.datetime], now: dt.datetime) -> int:
    # MVP rule:
    # - today/tomorrow: +2
    # - yesterday: +2
    # - within last 7 days: +1
    if pub_dt is None:
        return 0
    d = to_date(pub_dt)
    today = to_date(now)
    if d == today or d == (today + dt.timedelta(days=1)):
        return 2
    if d == (today - dt.timedelta(days=1)):
        return 2
    if today - dt.timedelta(days=7) <= d <= today:
        return 1
    return 0

def impact_bias_bonus(impact_bias: str) -> int:
    if impact_bias in ("urgent_boost", "practical_boost"):
        return 1
    if impact_bias == "low_impact":
        return -1
    return 0

# ---------------- model ----------------

@dataclass
class SourceCfg:
    source_id: str
    name: str
    urls_feed: Optional[str]
    urls_home: Optional[str]
    fetch_method: str
    geo_default: str
    brief_level: str
    boost: int
    impact_bias: str
    tags_default: List[str]

@dataclass
class Item:
    source_id: str
    source_name: str
    title: str
    url: str
    published: Optional[str]
    summary: str
    geo: str
    brief_level: str
    tags: List[str]
    score: int

    def to_json(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "title": self.title,
            "url": self.url,
            "published": self.published,
            "summary": self.summary,
            "geo": self.geo,
            "brief_level": self.brief_level,
            "tags": self.tags,
            "score": self.score,
        }

# ---------------- registry ----------------

def load_registry(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def pick_sources(reg: Dict[str, Any], source_ids: Optional[List[str]]) -> List[SourceCfg]:
    out: List[SourceCfg] = []
    wanted = set([s.strip() for s in source_ids]) if source_ids else None
    for s in reg.get("sources", []):
        sid = safe_text(s.get("source_id"))
        if wanted is not None and sid not in wanted:
            continue
        if not s.get("enabled", True):
            continue
        urls = s.get("urls", {}) or {}
        feed = urls.get("feed")
        home = urls.get("home")
        fetch = s.get("fetch", {}) or {}
        method = safe_text(fetch.get("method"))
        geo = safe_text((s.get("geo") or {}).get("default"))
        brief = safe_text((s.get("brief") or {}).get("level"))
        scoring = s.get("scoring") or {}
        boost = scoring.get("boost")
        try:
            boost_i = int(boost) if boost is not None else 0
        except Exception:
            boost_i = 0
        impact = safe_text(scoring.get("impact_bias"))
        tags = s.get("tags_default") or []
        out.append(SourceCfg(
            source_id=sid,
            name=safe_text(s.get("name")),
            urls_feed=feed,
            urls_home=home,
            fetch_method=method,
            geo_default=geo,
            brief_level=brief,
            boost=boost_i,
            impact_bias=impact,
            tags_default=[safe_text(t) for t in tags if safe_text(t)],
        ))
    return out

# ---------------- RSS fetch ----------------

def fetch_rss(src: SourceCfg, limit: int, now: dt.datetime) -> Tuple[List[Item], List[str]]:
    warns: List[str] = []
    if not src.urls_feed:
        return [], [f"[WARN] {src.source_id}: missing urls.feed"]
    try:
        xml = http_get(src.urls_feed)
    except Exception as e:
        return [], [f"[WARN] {src.source_id}: fetch failed: {e}"]
    feed = feedparser.parse(xml)
    if feed.bozo and getattr(feed, "bozo_exception", None):
        warns.append(f"[WARN] {src.source_id}: RSS parse bozo: {feed.bozo_exception}")

    items: List[Item] = []
    for entry in feed.entries[:limit]:
        title = norm_space(safe_text(getattr(entry, "title", None) or entry.get("title")))
        link = strip_utm(safe_text(getattr(entry, "link", None) or entry.get("link")))
        if not title or not link:
            continue
        pub_dt = parse_entry_datetime(entry)
        pub_iso = pub_dt.isoformat() if pub_dt else None
        summary = norm_space(safe_text(getattr(entry, "summary", None) or entry.get("summary") or ""))

        score = int(
            time_score(pub_dt, now)
            + GEO_WEIGHT.get(src.geo_default, 1)
            + src.boost
            + impact_bias_bonus(src.impact_bias)
        )

        items.append(Item(
            source_id=src.source_id,
            source_name=src.name,
            title=title,
            url=link,
            published=pub_iso,
            summary=summary,
            geo=src.geo_default,
            brief_level=src.brief_level,
            tags=list(src.tags_default),
            score=score,
        ))
    return items, warns

# ---------------- HTML list fetch (heuristic) ----------------

_BAD_HREF = re.compile(r"^(javascript:|mailto:|tel:|#)", re.IGNORECASE)
_DATE_PATTERNS = [
    # 01.02.2026 / 1.2.2026
    re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b"),
    # 2026-02-01
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),
]

def is_same_domain(url: str, base: str) -> bool:
    try:
        return urlparse(url).netloc.lower() == urlparse(base).netloc.lower()
    except Exception:
        return False

def guess_date_from_text(s: str) -> Optional[dt.datetime]:
    s = norm_space(s)
    for pat in _DATE_PATTERNS:
        m = pat.search(s)
        if not m:
            continue
        try:
            if pat.pattern.startswith("\\b(\\d{1,2})\\."):
                dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
                d = dt_parse(f"{dd}.{mm}.{yyyy}", dayfirst=True, fuzzy=True)
            else:
                yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
                d = dt_parse(f"{yyyy}-{mm}-{dd}", fuzzy=True)
            if d.tzinfo is None:
                d = d.replace(tzinfo=tz.tzlocal())
            return d.astimezone(tz.tzlocal())
        except Exception:
            continue
    return None

def allowlist_filter(src: SourceCfg, url: str) -> bool:
    """Light per-source allowlist to avoid menu noise."""
    if not src.urls_home:
        return True
    if src.source_id == "BA_CITY_NEWS":
        # keep only article-like links under the 'aktuality' section
        return "/transparentne-mesto/aktuality" in url and url.rstrip("/") != src.urls_home.rstrip("/")
    if src.source_id == "SR_ZJAZD":
        # zjazdnost is mostly a dashboard; keep same-domain links but avoid anchors
        return url.rstrip("/") != src.urls_home.rstrip("/")
    return True

def fetch_html_list(src: SourceCfg, limit: int, now: dt.datetime) -> Tuple[List[Item], List[str]]:
    warns: List[str] = []
    if not src.urls_home:
        return [], [f"[WARN] {src.source_id}: missing urls.home for html_list"]
    try:
        html = http_get(src.urls_home)
    except Exception as e:
        return [], [f"[WARN] {src.source_id}: fetch failed: {e}"]

    soup = BeautifulSoup(html, "html.parser")

    # Prefer main/article regions; fallback to full doc
    region = soup.find("main") or soup.find("article") or soup
    links = region.find_all("a", href=True)

    candidates: List[Tuple[str, str, Optional[dt.datetime], str]] = []
    for a in links:
        href = safe_text(a.get("href"))
        if not href or _BAD_HREF.match(href):
            continue
        text = norm_space(a.get_text(" "))
        if len(text) < 12:
            continue
        abs_url = strip_utm(urljoin(src.urls_home, href))
        if not is_same_domain(abs_url, src.urls_home):
            continue
        if not allowlist_filter(src, abs_url):
            continue

        # Try to infer date from surrounding context
        ctx = norm_space(" ".join([
            safe_text(a.get_text(" ")),
            safe_text(a.parent.get_text(" ")) if a.parent else "",
        ]))
        pub_dt = guess_date_from_text(ctx)
        summary = ""

        candidates.append((text, abs_url, pub_dt, summary))

    # Deduplicate by url+title
    seen = set()
    items: List[Item] = []
    for title, url, pub_dt, summary in candidates:
        key = (url, norm_key(title))
        if key in seen:
            continue
        seen.add(key)

        pub_iso = pub_dt.isoformat() if pub_dt else None
        score = int(
            time_score(pub_dt, now)
            + GEO_WEIGHT.get(src.geo_default, 1)
            + src.boost
            + impact_bias_bonus(src.impact_bias)
        )

        items.append(Item(
            source_id=src.source_id,
            source_name=src.name,
            title=title,
            url=url,
            published=pub_iso,
            summary=summary,
            geo=src.geo_default,
            brief_level=src.brief_level,
            tags=list(src.tags_default),
            score=score,
        ))

    # Heuristic: keep only top N by score, then stable order by title
    items = sorted(items, key=lambda it: (-it.score, it.published or "", it.title.lower()))[:limit]

    # If extraction looks empty, warn explicitly
    if not items:
        warns.append(f"[WARN] {src.source_id}: html_list extracted 0 items (page structure may need explicit selectors)")
    return items, warns

# ---------------- output ----------------

def dedupe(items: List[Item]) -> List[Item]:
    seen = set()
    out: List[Item] = []
    for it in items:
        key = (strip_utm(it.url), norm_key(it.title))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def sort_items(items: List[Item]) -> List[Item]:
    return sorted(items, key=lambda it: (-it.score, it.published or "", it.title.lower()))

def write_jsonl(items: List[Item], path: Path) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it.to_json(), ensure_ascii=False) + "\n")

def fmt_date(pub_iso: Optional[str]) -> str:
    if not pub_iso:
        return ""
    try:
        d = dt.datetime.fromisoformat(pub_iso)
        return d.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return pub_iso[:16]

def write_daily_brief(items: List[Item], out_path: Path, title: str = "Denný brief") -> None:
    ensure_dir(out_path.parent)
    lines: List[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(f"_Generované: {now_local().strftime('%Y-%m-%d %H:%M')}_")
    lines.append("")
    if not items:
        lines.append("Žiadne položky.")
        out_path.write_text("\n".join(lines), encoding="utf-8")
        return

    geo_order = ["BA","BSK","SR","SUSEDIA","EU_GLOBAL"]
    by_geo: Dict[str, List[Item]] = {g: [] for g in geo_order}
    other: List[Item] = []
    for it in items:
        (by_geo[it.geo] if it.geo in by_geo else other).append(it)

    def geo_title(g: str) -> str:
        return {"BA": "Bratislava", "BSK": "Bratislavský kraj", "SR": "Slovensko", "SUSEDIA": "Susedia", "EU_GLOBAL": "EÚ / globál"}.get(g, g)

    for g in geo_order:
        chunk = sort_items(by_geo[g])
        if not chunk:
            continue
        lines.append(f"## {geo_title(g)}")
        lines.append("")
        for it in chunk:
            when = fmt_date(it.published)
            when_prefix = f"{when} — " if when else ""
            lines.append(f"**{when_prefix}{it.title}**")
            lines.append(f"Téma: {', '.join(it.tags) if it.tags else '—'}")
            lines.append(f"Zdroj: {it.source_name} | Score: {it.score}")
            lines.append(f"Link: {it.url}")
            lines.append("")
    if other:
        lines.append("## Ostatné")
        lines.append("")
        for it in sort_items(other):
            when = fmt_date(it.published)
            when_prefix = f"{when} — " if when else ""
            lines.append(f"**{when_prefix}{it.title}**")
            lines.append(f"Téma: {', '.join(it.tags) if it.tags else '—'}")
            lines.append(f"Zdroj: {it.source_name} | Score: {it.score}")
            lines.append(f"Link: {it.url}")
            lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")

# ---------------- main ----------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", required=True, help="Path to registry.json (built)")
    ap.add_argument("--sources", default=None, help="Comma-separated source_id list (optional)")
    ap.add_argument("--outdir", default="out", help="Output directory for brief")
    ap.add_argument("--data-dir", default="data", help="Data directory for items.jsonl")
    ap.add_argument("--limit", type=int, default=40, help="Per-source item limit")
    args = ap.parse_args()

    reg_path = Path(args.registry)
    if not reg_path.exists():
        print(f"ERROR: registry not found: {reg_path}", file=sys.stderr)
        return 2

    reg = load_registry(reg_path)
    source_ids = [s.strip() for s in args.sources.split(",")] if args.sources else None
    sources = pick_sources(reg, source_ids)

    if source_ids:
        missing = sorted(set(source_ids) - set(s.source_id for s in sources))
        if missing:
            print(f"WARN: requested source_id not found/enabled in registry: {missing}", file=sys.stderr)

    now = now_local()
    all_items: List[Item] = []
    warns: List[str] = []

    for src in sources:
        if src.fetch_method == "rss":
            items, w = fetch_rss(src, limit=args.limit, now=now)
        elif src.fetch_method == "html_list":
            items, w = fetch_html_list(src, limit=args.limit, now=now)
        else:
            items, w = [], [f"[WARN] {src.source_id}: fetch_method='{src.fetch_method}' not supported (skipped)"]
        all_items.extend(items)
        warns.extend(w)

    all_items = sort_items(dedupe(all_items))

    data_path = Path(args.data_dir) / "items.jsonl"
    write_jsonl(all_items, data_path)

    brief_path = Path(args.outdir) / "daily_brief.md"
    write_daily_brief(all_items, brief_path)

    if warns:
        ensure_dir(Path(args.outdir))
        (Path(args.outdir) / "run_warnings.txt").write_text("\n".join(warns) + "\n", encoding="utf-8")
        print(f"WARNINGS written to: {Path(args.outdir) / 'run_warnings.txt'}", file=sys.stderr)

    print(f"Wrote: {data_path}")
    print(f"Wrote: {brief_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
