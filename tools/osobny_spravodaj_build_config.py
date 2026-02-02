#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Osobný spravodaj – build pipeline (PATCH v2):
Excel (source registry) -> validation -> export YAML/JSON -> (optional) JSON Schema validation

Adds (vs original):
  1) Integer normalization (e.g., 6.0 -> 6) in exported config
  2) Optional tag normalization to stable ASCII ids (via --normalize-tags)
  3) Dataset endpoint heuristic warning (dataset feed looks like landing page)
  4) Graceful YAML dependency (PyYAML optional if exporting yaml)
  5) Variant B: built-in JSON Schema validation (via --schema)

Usage:
  python osobny_spravodaj_build_config_patch_v2.py --input registry.xlsx --outdir build --format both
  python osobny_spravodaj_build_config_patch_v2.py --input registry.xlsx --outdir build --format json --schema registry.schema.json
  python osobny_spravodaj_build_config_patch_v2.py --input registry.xlsx --outdir build --format both --normalize-tags --schema registry.schema.json

Outputs:
  build/registry.yaml            (if requested and PyYAML available)
  build/registry.json            (if requested)
  build/validation_report.txt    (always; Excel + export-time warnings/errors)
  build/schema_report.txt        (if --schema; schema validation results)

Exit code:
  0 = ok (may include warnings)
  2 = validation errors (no exports OR schema fails)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

# Optional YAML
try:
    import yaml  # type: ignore
    HAS_YAML = True
except ModuleNotFoundError:
    yaml = None
    HAS_YAML = False

TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
MACHINE_EXTS = (".xml", ".json", ".zip", ".csv", ".cap", ".atom", ".rss")

REQUIRED_COLUMNS = [
    "source_id","source_name","source_owner","source_type","url_home","url_feed",
    "fetch_method","fetch_frequency_min","geo_default","geo_scope_hint","brief_level",
    "daily_brief_inclusion","notify_policy","notify_class_default","impact_bias",
    "score_boost","dedupe_priority","source_threshold_override","require_term_for_inclusion",
    "allow_push_time_start","allow_push_time_end","cooldown_minutes","topic_default_tags",
    "parser_notes","quality_notes","enabled"
]

REQUIRED_NONEMPTY_IF_ENABLED = [
    "source_id","source_name","source_type","url_home","fetch_method","fetch_frequency_min",
    "geo_default","brief_level","daily_brief_inclusion","notify_policy","enabled"
]


@dataclass
class Issue:
    level: str  # "ERROR" | "WARN"
    source_id: str
    field: str
    message: str

    def format(self) -> str:
        sid = self.source_id or "<no source_id>"
        return f"[{self.level}] {sid} | {self.field}: {self.message}"


def _clean_cell(x: Any) -> Any:
    if pd.isna(x):
        return None
    if isinstance(x, str):
        s = x.strip()
        return s if s != "" else None
    return x


def _opt_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    try:
        if isinstance(val, str) and val.strip() == "":
            return None
        return int(float(val))  # allows "6.0"
    except Exception:
        return None


def slugify_tag(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def load_enums(xlsx: Path) -> Dict[str, Set[str]]:
    df = pd.read_excel(xlsx, sheet_name="enums")
    df["field"] = df["field"].astype(str).str.strip()
    df["allowed_value"] = df["allowed_value"].astype(str).str.strip()
    enums: Dict[str, Set[str]] = {}
    for field, g in df.groupby("field"):
        enums[field] = set(g["allowed_value"].tolist())
    return enums


def load_defaults(xlsx: Path) -> Dict[str, Any]:
    df = pd.read_excel(xlsx, sheet_name="defaults")
    out: Dict[str, Any] = {}
    for row in df.to_dict(orient="records"):
        k = _clean_cell(row.get("key"))
        v = _clean_cell(row.get("value"))
        if k is None:
            continue
        if isinstance(v, str) and v.isdigit():
            out[k] = int(v)
        else:
            out[k] = v
    return out


def load_enabled_tags(xlsx: Path) -> Set[str]:
    df = pd.read_excel(xlsx, sheet_name="tag_dictionary")
    df["tag"] = df["tag"].astype(str).str.strip()
    df["enabled"] = df["enabled"].fillna(False).astype(bool)
    return set(df.loc[df["enabled"], "tag"].tolist())


def load_sources(xlsx: Path) -> pd.DataFrame:
    df = pd.read_excel(xlsx, sheet_name="sources", dtype=object)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    for c in df.columns:
        df[c] = df[c].apply(_clean_cell)

    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    if "enabled" in df.columns:
        df["enabled"] = df["enabled"].map(lambda x: bool(x) if x is not None else False)
    if "require_term_for_inclusion" in df.columns:
        df["require_term_for_inclusion"] = df["require_term_for_inclusion"].map(lambda x: bool(x) if x is not None else False)

    return df


def validate_schema(df: pd.DataFrame) -> List[Issue]:
    issues: List[Issue] = []
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        issues.append(Issue("ERROR", "", "schema", f"Missing required columns: {missing}"))
    return issues


def _looks_like_landing_page(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return False
    if u.endswith(MACHINE_EXTS):
        return False
    if "data.slovensko.sk/datasety" in u:
        return True
    if u.endswith(".html") or "/index" in u:
        return True
    if ("api" not in u) and ("download" not in u) and ("export" not in u):
        return True
    return False


def validate_sources(
    df: pd.DataFrame,
    enums: Dict[str, Set[str]],
    enabled_tags: Set[str],
) -> Tuple[List[Issue], List[Dict[str, Any]]]:
    issues: List[Issue] = []

    ids = [str(x).strip() for x in df["source_id"].dropna().tolist()]
    seen: Set[str] = set()
    dups: Set[str] = set()
    for x in ids:
        if x in seen:
            dups.add(x)
        else:
            seen.add(x)
    for d in sorted(dups):
        issues.append(Issue("ERROR", d, "source_id", "Duplicate source_id"))

    valid_rows: List[Dict[str, Any]] = []

    for row in df.to_dict(orient="records"):
        sid_raw = row.get("source_id")
        sid = (sid_raw or "").strip() if isinstance(sid_raw, str) else (str(sid_raw).strip() if sid_raw is not None else "")
        enabled = bool(row.get("enabled") or False)

        # skip fully empty rows
        if not sid and not enabled:
            any_filled = any(row.get(c) is not None for c in REQUIRED_COLUMNS if c != "enabled")
            if not any_filled:
                continue

        if enabled:
            for c in REQUIRED_NONEMPTY_IF_ENABLED:
                v = row.get(c)
                if v is None or (isinstance(v, str) and v.strip() == ""):
                    issues.append(Issue("ERROR", sid, c, "Required field is empty (enabled source)"))

        # enums
        for f in ["source_type","fetch_method","geo_default","brief_level","daily_brief_inclusion","notify_policy","notify_class_default","impact_bias"]:
            v = row.get(f)
            if v is None:
                continue
            v_str = str(v).strip()
            allowed = enums.get(f)
            if allowed and v_str not in allowed:
                issues.append(Issue("ERROR", sid, f, f"Invalid value '{v_str}'. Allowed: {sorted(allowed)}"))

        # numeric ranges
        def _int_field(name: str, lo: int, hi: int, allow_none: bool = False) -> Optional[int]:
            val = row.get(name)
            if val is None:
                return None if allow_none else 0
            try:
                ival = int(float(val))
            except Exception:
                issues.append(Issue("ERROR", sid, name, f"Not an integer: {val}"))
                return None
            if ival < lo or ival > hi:
                issues.append(Issue("ERROR", sid, name, f"Out of range {lo}..{hi}: {ival}"))
            return ival

        freq = _int_field("fetch_frequency_min", 5, 1440, allow_none=True)
        boost = _int_field("score_boost", -3, 5, allow_none=True)
        dedupe_pri = _int_field("dedupe_priority", 1, 9, allow_none=True)

        thr_ovr_raw = row.get("source_threshold_override")
        thr_ovr = None
        if thr_ovr_raw is not None:
            try:
                thr = int(float(thr_ovr_raw))
                if thr < 0 or thr > 10:
                    issues.append(Issue("ERROR", sid, "source_threshold_override", f"Out of range 0..10: {thr}"))
                else:
                    thr_ovr = thr
            except Exception:
                issues.append(Issue("ERROR", sid, "source_threshold_override", f"Not an integer: {thr_ovr_raw}"))

        cooldown_raw = row.get("cooldown_minutes")
        cooldown = None
        if cooldown_raw is not None:
            try:
                cd = int(float(cooldown_raw))
                if cd < 0 or cd > 720:
                    issues.append(Issue("ERROR", sid, "cooldown_minutes", f"Out of range 0..720: {cd}"))
                else:
                    cooldown = cd
            except Exception:
                issues.append(Issue("ERROR", sid, "cooldown_minutes", f"Not an integer: {cooldown_raw}"))

        # time format
        for tf in ["allow_push_time_start","allow_push_time_end"]:
            tv = row.get(tf)
            if tv is None:
                continue
            tvs = str(tv).strip()
            if not TIME_RE.match(tvs):
                issues.append(Issue("ERROR", sid, tf, f"Invalid time format '{tvs}', expected HH:MM"))

        # URL sanity
        for uf in ["url_home","url_feed"]:
            uv = row.get(uf)
            if uv is None:
                continue
            uvs = str(uv).strip()
            if not (uvs.startswith("http://") or uvs.startswith("https://")):
                issues.append(Issue("WARN", sid, uf, f"URL does not start with http(s): '{uvs}'"))

        fetch_method = (row.get("fetch_method") or "")
        url_feed = row.get("url_feed")
        if fetch_method in {"rss","api","dataset"} and not url_feed:
            issues.append(Issue("ERROR", sid, "url_feed", f"url_feed required for fetch_method='{fetch_method}'"))

        if fetch_method == "dataset" and url_feed:
            u = str(url_feed).strip()
            if _looks_like_landing_page(u):
                issues.append(Issue(
                    "WARN", sid, "url_feed",
                    "fetch_method=dataset: url_feed looks like a landing page, not a machine endpoint (consider adding direct download/API URL)"
                ))

        notify_policy = (row.get("notify_policy") or "")
        notify_class = (row.get("notify_class_default") or "")
        if notify_policy == "always" and notify_class in {"none",""}:
            issues.append(Issue("ERROR", sid, "notify_class_default", "notify_policy=always requires notify_class_default != none"))

        inclusion = (row.get("daily_brief_inclusion") or "")
        if inclusion == "never" and notify_policy != "never":
            issues.append(Issue("WARN", sid, "notify_policy", "daily_brief_inclusion=never but notify_policy != never (check intent)"))

        # tags
        tags_raw = row.get("topic_default_tags") or ""
        tags: List[str] = []
        if tags_raw:
            parts = [t.strip() for t in str(tags_raw).split(",") if t.strip()]
            tags = parts
            for t in parts:
                if t not in enabled_tags:
                    issues.append(Issue("WARN", sid, "topic_default_tags", f"Unknown/disabled tag '{t}' (not in tag_dictionary enabled=true)"))

        if enabled:
            norm = {c: row.get(c) for c in REQUIRED_COLUMNS}
            norm["fetch_frequency_min"] = freq
            norm["score_boost"] = boost
            norm["dedupe_priority"] = dedupe_pri
            norm["cooldown_minutes"] = cooldown
            norm["source_threshold_override"] = thr_ovr
            norm["topic_default_tags"] = tags
            valid_rows.append(norm)

    return issues, valid_rows


def build_export(defaults: Dict[str, Any], rows: List[Dict[str, Any]], normalize_tags: bool) -> Dict[str, Any]:
    def get_int(key: str, fallback: int) -> int:
        v = defaults.get(key, fallback)
        try:
            return int(float(v))
        except Exception:
            return fallback

    export: Dict[str, Any] = {
        "defaults": {
            "thresholds": {
                "BA": get_int("threshold_BA", 5),
                "BSK": get_int("threshold_BSK", 5),
                "SR": get_int("threshold_SR", 4),
                "SUSEDIA": get_int("threshold_SUSEDIA", 6),
                "EU_GLOBAL": get_int("threshold_EU_GLOBAL", 6),
            },
            "daily_limits": {
                "BA": get_int("daily_limit_BA", 10),
                "BSK": get_int("daily_limit_BSK", 8),
                "SR": get_int("daily_limit_SR", 8),
                "SUSEDIA": get_int("daily_limit_SUSEDIA", 5),
                "EU_GLOBAL": get_int("daily_limit_EU_GLOBAL", 5),
            },
            "push_windows": {
                "practical": [defaults.get("push_window_practical_start", "07:00"), defaults.get("push_window_practical_end", "21:00")],
                "decision": [defaults.get("push_window_decision_start", "08:00"), defaults.get("push_window_decision_end", "20:00")],
            },
            "cooldowns_default": {
                "urgent": get_int("cooldown_urgent_default", 30),
                "practical": get_int("cooldown_practical_default", 120),
                "decision": get_int("cooldown_decision_default", 360),
            }
        },
        "sources": []
    }

    tag_display: Dict[str, str] = {}

    def maybe_norm_tags(tags: List[str]) -> List[str]:
        if not normalize_tags:
            return tags
        out: List[str] = []
        for t in tags:
            tid = slugify_tag(t)
            if tid:
                out.append(tid)
                tag_display.setdefault(tid, t)
        seen: Set[str] = set()
        deduped: List[str] = []
        for x in out:
            if x not in seen:
                deduped.append(x)
                seen.add(x)
        return deduped

    for r in rows:
        allow_start = r.get("allow_push_time_start")
        allow_end = r.get("allow_push_time_end")

        notify_class = r.get("notify_class_default") or "none"
        if allow_start and allow_end:
            allow_time = [allow_start, allow_end]
        else:
            if notify_class == "practical":
                allow_time = export["defaults"]["push_windows"]["practical"]
            elif notify_class == "decision":
                allow_time = export["defaults"]["push_windows"]["decision"]
            else:
                allow_time = None

        cooldown = r.get("cooldown_minutes")
        if cooldown is None:
            cooldown = export["defaults"]["cooldowns_default"].get(notify_class, None)

        src_obj = {
            "source_id": r.get("source_id"),
            "name": r.get("source_name"),
            "owner": r.get("source_owner"),
            "type": r.get("source_type"),
            "urls": {
                "home": r.get("url_home"),
                "feed": r.get("url_feed"),
            },
            "fetch": {
                "method": r.get("fetch_method"),
                "frequency_min": _opt_int(r.get("fetch_frequency_min")),
            },
            "geo": {
                "default": r.get("geo_default"),
                "scope_hint": r.get("geo_scope_hint"),
            },
            "brief": {
                "level": r.get("brief_level"),
                "inclusion": r.get("daily_brief_inclusion"),
                "require_term": bool(r.get("require_term_for_inclusion") or False),
                "threshold_override": _opt_int(r.get("source_threshold_override")),
            },
            "notify": {
                "policy": r.get("notify_policy"),
                "class_default": notify_class,
                "allow_time": allow_time,
                "cooldown_min": _opt_int(cooldown),
            },
            "scoring": {
                "boost": _opt_int(r.get("score_boost")),
                "impact_bias": r.get("impact_bias"),
            },
            "dedupe": {
                "priority": _opt_int(r.get("dedupe_priority")),
            },
            "tags_default": maybe_norm_tags(r.get("topic_default_tags") or []),
            "notes": {
                "parser": r.get("parser_notes"),
                "quality": r.get("quality_notes"),
            },
            "enabled": True,
        }

        if not src_obj["urls"].get("feed"):
            src_obj["urls"].pop("feed", None)

        export["sources"].append(src_obj)

    if normalize_tags and tag_display:
        export["tag_display"] = tag_display

    return export


def write_report(outdir: Path, issues: List[Issue]) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    report_path = outdir / "validation_report.txt"
    lines: List[str] = []
    if not issues:
        lines.append("OK: No validation issues.\n")
    else:
        lines.append(f"Issues: {len(issues)}\n")
        for it in issues:
            lines.append(it.format())
        lines.append("")
        errors = [i for i in issues if i.level == "ERROR"]
        warns = [i for i in issues if i.level == "WARN"]
        lines.append(f"ERRORS: {len(errors)}")
        lines.append(f"WARNINGS: {len(warns)}")
    report_path.write_text("\n".join(lines), encoding="utf-8")


def schema_validate(export_obj: Dict[str, Any], schema_path: Path, outdir: Path) -> Tuple[bool, str]:
    """
    Validate export_obj against JSON Schema at schema_path.
    Returns (ok, report_text). Writes build/schema_report.txt.
    """
    try:
        from jsonschema import Draft202012Validator  # type: ignore
    except ModuleNotFoundError:
        msg = "jsonschema is not installed. Install with: python -m pip install jsonschema"
        (outdir / "schema_report.txt").write_text("ERROR: " + msg + "\n", encoding="utf-8")
        return False, msg

    if not schema_path.exists():
        msg = f"Schema file not found: {schema_path}"
        (outdir / "schema_report.txt").write_text("ERROR: " + msg + "\n", encoding="utf-8")
        return False, msg

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(export_obj), key=lambda e: list(e.path))

    if not errors:
        report = "OK: registry matches schema.\n"
        (outdir / "schema_report.txt").write_text(report, encoding="utf-8")
        return True, report

    lines = ["ERRORS:\n"]
    for e in errors:
        path = "/" + "/".join(str(p) for p in e.path) if e.path else "/"
        lines.append(f"- {path}: {e.message}")
    report = "\n".join(lines) + "\n"
    (outdir / "schema_report.txt").write_text(report, encoding="utf-8")
    return False, report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to Excel registry (.xlsx)")
    ap.add_argument("--outdir", default="build", help="Output directory")
    ap.add_argument("--format", default="both", choices=["yaml","json","both"], help="Export format")
    ap.add_argument("--normalize-tags", action="store_true", help="Normalize tags to stable ASCII ids and include tag_display mapping")
    ap.add_argument("--schema", default=None, help="Optional path to registry.schema.json. If provided, validates the exported object and fails on error.")
    args = ap.parse_args()

    xlsx = Path(args.input)
    outdir = Path(args.outdir)

    if not xlsx.exists():
        print(f"ERROR: input file not found: {xlsx}", file=sys.stderr)
        return 2

    df = load_sources(xlsx)
    schema_issues = validate_schema(df)
    if schema_issues and any(i.level == "ERROR" for i in schema_issues):
        write_report(outdir, schema_issues)
        print("Validation failed (Excel schema). See validation_report.txt", file=sys.stderr)
        return 2

    enums = load_enums(xlsx)
    defaults = load_defaults(xlsx)
    enabled_tags = load_enabled_tags(xlsx)

    row_issues, enabled_rows = validate_sources(df, enums, enabled_tags)
    issues = schema_issues + row_issues

    write_report(outdir, issues)

    errors = [i for i in issues if i.level == "ERROR"]
    if errors:
        print(f"Validation failed with {len(errors)} error(s). See {outdir/'validation_report.txt'}", file=sys.stderr)
        return 2

    export_obj = build_export(defaults, enabled_rows, normalize_tags=args.normalize_tags)

    # Variant B: schema validation (optional)
    if args.schema:
        ok, _ = schema_validate(export_obj, Path(args.schema), outdir)
        if not ok:
            print(f"Schema validation failed. See {outdir/'schema_report.txt'}", file=sys.stderr)
            return 2
        else:
            print(f"Schema OK. Report: {outdir/'schema_report.txt'}")

    outdir.mkdir(parents=True, exist_ok=True)

    if args.format in ("yaml","both"):
        if not HAS_YAML:
            print("WARN: PyYAML not installed; skipping YAML export. (Install: python -m pip install pyyaml)", file=sys.stderr)
        else:
            yaml_path = outdir / "registry.yaml"
            assert yaml is not None
            yaml.safe_dump(export_obj, yaml_path.open("w", encoding="utf-8"), sort_keys=False, allow_unicode=True)
            print(f"Wrote {yaml_path}")

    if args.format in ("json","both"):
        json_path = outdir / "registry.json"
        json_path.write_text(json.dumps(export_obj, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {json_path}")

    print(f"Excel validation report: {outdir/'validation_report.txt'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
