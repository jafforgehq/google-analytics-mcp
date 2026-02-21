from __future__ import annotations

from collections import defaultdict
from typing import Any, Sequence
from urllib.parse import urljoin, urlsplit, urlunsplit


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def normalize_url(
    value: str,
    *,
    base_url: str | None = None,
    drop_query: bool = True,
    drop_fragment: bool = True,
    remove_www: bool = False,
) -> str:
    text = (value or "").strip()
    if not text:
        return text

    if not text.startswith(("http://", "https://")) and base_url:
        text = urljoin(base_url.rstrip("/") + "/", text.lstrip("/"))

    parts = urlsplit(text)

    scheme = parts.scheme.lower() or "https"
    netloc = parts.netloc.lower()
    if remove_www and netloc.startswith("www."):
        netloc = netloc[4:]

    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]

    query = "" if drop_query else parts.query
    fragment = "" if drop_fragment else parts.fragment

    if not netloc:
        return path

    return urlunsplit((scheme, netloc, path, query, fragment))


def _weighted_average(sum_weighted: float, sum_weight: float, fallback: float = 0.0) -> float:
    if sum_weight <= 0:
        return fallback
    return sum_weighted / sum_weight


def normalize_gsc_rows_by_page(
    rows: Sequence[dict[str, Any]],
    *,
    dimensions: Sequence[str] | None = None,
    base_url: str | None = None,
) -> dict[str, dict[str, Any]]:
    dimensions = list(dimensions or ["page"])
    if "page" in dimensions:
        page_index = dimensions.index("page")
    else:
        page_index = 0

    bucket: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "gsc_clicks": 0.0,
            "gsc_impressions": 0.0,
            "gsc_ctr_weighted": 0.0,
            "gsc_position_weighted": 0.0,
            "gsc_rows": 0,
        }
    )

    for row in rows:
        keys = row.get("keys", [])
        if not keys:
            continue
        if page_index >= len(keys):
            continue

        raw_url = str(keys[page_index])
        url = normalize_url(raw_url, base_url=base_url)
        if not url:
            continue

        clicks = to_float(row.get("clicks"))
        impressions = to_float(row.get("impressions"))
        ctr = to_float(row.get("ctr"))
        position = to_float(row.get("position"))

        item = bucket[url]
        item["gsc_clicks"] += clicks
        item["gsc_impressions"] += impressions
        item["gsc_ctr_weighted"] += ctr * impressions
        item["gsc_position_weighted"] += position * impressions
        item["gsc_rows"] += 1

    for url, item in bucket.items():
        impressions = item["gsc_impressions"]
        item["gsc_ctr"] = _weighted_average(item["gsc_ctr_weighted"], impressions)
        item["gsc_position"] = _weighted_average(item["gsc_position_weighted"], impressions)
        del item["gsc_ctr_weighted"]
        del item["gsc_position_weighted"]

    return dict(bucket)


def normalize_ga4_rows_by_page(
    rows: Sequence[dict[str, Any]],
    *,
    base_url: str | None = None,
    url_dimension_candidates: Sequence[str] | None = None,
) -> dict[str, dict[str, Any]]:
    candidates = list(
        url_dimension_candidates
        or [
            "landingPagePlusQueryString",
            "landingPage",
            "fullPageUrl",
            "pageLocation",
            "pagePath",
        ]
    )

    bucket: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "ga4_sessions": 0.0,
            "ga4_engaged_sessions": 0.0,
            "ga4_conversions": 0.0,
            "ga4_total_users": 0.0,
            "ga4_screen_page_views": 0.0,
            "ga4_user_engagement_duration": 0.0,
            "ga4_rows": 0,
        }
    )

    for row in rows:
        raw = None
        for dim in candidates:
            value = row.get(dim)
            if value:
                raw = str(value)
                break
        if not raw:
            continue

        url = normalize_url(raw, base_url=base_url)
        if not url:
            continue

        item = bucket[url]
        item["ga4_sessions"] += to_float(row.get("sessions"))
        item["ga4_engaged_sessions"] += to_float(row.get("engagedSessions"))
        item["ga4_conversions"] += to_float(row.get("conversions"))
        item["ga4_total_users"] += to_float(row.get("totalUsers"))
        item["ga4_screen_page_views"] += to_float(row.get("screenPageViews"))
        item["ga4_user_engagement_duration"] += to_float(
            row.get("userEngagementDuration")
        )
        item["ga4_rows"] += 1

    for item in bucket.values():
        sessions = item["ga4_sessions"]
        item["ga4_engagement_rate"] = (
            item["ga4_engaged_sessions"] / sessions if sessions > 0 else 0.0
        )
        item["ga4_conversion_rate"] = (
            item["ga4_conversions"] / sessions if sessions > 0 else 0.0
        )

    return dict(bucket)


def compute_delta_pct(current: float, previous: float) -> float | None:
    if previous <= 0:
        return None
    return (current - previous) / previous
