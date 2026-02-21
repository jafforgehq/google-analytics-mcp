from __future__ import annotations

from typing import Any

from seo_analytics_mcp.config import Settings
from seo_analytics_mcp.core.normalization import compute_delta_pct
from seo_analytics_mcp.core.scoring import score_page


def merge_page_metrics(
    gsc_current: dict[str, dict[str, Any]] | None,
    ga4_current: dict[str, dict[str, Any]] | None,
    *,
    gsc_previous: dict[str, dict[str, Any]] | None = None,
    ga4_previous: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    gsc_current = gsc_current or {}
    ga4_current = ga4_current or {}
    gsc_previous = gsc_previous or {}
    ga4_previous = ga4_previous or {}

    urls = sorted(
        set(gsc_current.keys())
        | set(ga4_current.keys())
        | set(gsc_previous.keys())
        | set(ga4_previous.keys())
    )

    merged: list[dict[str, Any]] = []

    for url in urls:
        current_g = dict(gsc_current.get(url, {}))
        current_a = dict(ga4_current.get(url, {}))
        prev_g = dict(gsc_previous.get(url, {}))
        prev_a = dict(ga4_previous.get(url, {}))

        row: dict[str, Any] = {"url": url}
        row.update(current_g)
        row.update(current_a)

        if prev_g:
            row["gsc_prev_clicks"] = float(prev_g.get("gsc_clicks", 0.0))
            row["gsc_prev_impressions"] = float(prev_g.get("gsc_impressions", 0.0))
        if prev_a:
            row["ga4_prev_sessions"] = float(prev_a.get("ga4_sessions", 0.0))
            row["ga4_prev_conversions"] = float(prev_a.get("ga4_conversions", 0.0))

        row["gsc_clicks_delta_pct"] = compute_delta_pct(
            float(row.get("gsc_clicks", 0.0)),
            float(row.get("gsc_prev_clicks", 0.0)),
        )
        row["gsc_impressions_delta_pct"] = compute_delta_pct(
            float(row.get("gsc_impressions", 0.0)),
            float(row.get("gsc_prev_impressions", 0.0)),
        )
        row["ga4_sessions_delta_pct"] = compute_delta_pct(
            float(row.get("ga4_sessions", 0.0)),
            float(row.get("ga4_prev_sessions", 0.0)),
        )
        row["ga4_conversions_delta_pct"] = compute_delta_pct(
            float(row.get("ga4_conversions", 0.0)),
            float(row.get("ga4_prev_conversions", 0.0)),
        )

        merged.append(row)

    return merged


def summarize_portfolio(merged_pages: list[dict[str, Any]]) -> dict[str, Any]:
    total_pages = len(merged_pages)
    total_clicks = sum(float(p.get("gsc_clicks", 0.0)) for p in merged_pages)
    total_impressions = sum(float(p.get("gsc_impressions", 0.0)) for p in merged_pages)
    total_sessions = sum(float(p.get("ga4_sessions", 0.0)) for p in merged_pages)
    total_conversions = sum(float(p.get("ga4_conversions", 0.0)) for p in merged_pages)

    portfolio_ctr = total_clicks / total_impressions if total_impressions > 0 else 0.0
    portfolio_conversion_rate = (
        total_conversions / total_sessions if total_sessions > 0 else 0.0
    )

    return {
        "total_pages": total_pages,
        "total_gsc_clicks": round(total_clicks, 2),
        "total_gsc_impressions": round(total_impressions, 2),
        "total_ga4_sessions": round(total_sessions, 2),
        "total_ga4_conversions": round(total_conversions, 2),
        "portfolio_ctr": round(portfolio_ctr, 4),
        "portfolio_conversion_rate": round(portfolio_conversion_rate, 4),
    }


def generate_action_items(
    merged_pages: list[dict[str, Any]],
    settings: Settings,
    *,
    max_items: int | None = None,
) -> list[dict[str, Any]]:
    limit = max_items if max_items is not None else settings.default_max_action_items
    items: list[dict[str, Any]] = []

    for page in merged_pages:
        result = score_page(page, settings)
        if result.score <= 0:
            continue

        item = {
            "url": page["url"],
            "score": result.score,
            "priority": result.priority,
            "category": result.categories[0] if result.categories else "opportunity",
            "categories": result.categories,
            "expected_impact": result.expected_impact,
            "effort": result.effort,
            "confidence": result.confidence,
            "reasons": result.reasons,
            "recommended_actions": result.recommendations,
            "evidence": {
                "gsc_impressions": round(float(page.get("gsc_impressions", 0.0)), 2),
                "gsc_clicks": round(float(page.get("gsc_clicks", 0.0)), 2),
                "gsc_ctr": round(float(page.get("gsc_ctr", 0.0)), 4),
                "gsc_position": round(float(page.get("gsc_position", 0.0)), 2),
                "ga4_sessions": round(float(page.get("ga4_sessions", 0.0)), 2),
                "ga4_engagement_rate": round(
                    float(page.get("ga4_engagement_rate", 0.0)), 4
                ),
                "ga4_conversion_rate": round(
                    float(page.get("ga4_conversion_rate", 0.0)), 4
                ),
                "gsc_clicks_delta_pct": page.get("gsc_clicks_delta_pct"),
                "ga4_sessions_delta_pct": page.get("ga4_sessions_delta_pct"),
            },
        }
        items.append(item)

    items.sort(key=lambda i: (i["score"], i["confidence"]), reverse=True)
    return items[: max(1, limit)]


def _top(
    merged_pages: list[dict[str, Any]],
    field: str,
    top_n: int,
) -> list[dict[str, Any]]:
    rows = sorted(
        merged_pages,
        key=lambda p: float(p.get(field, 0.0)),
        reverse=True,
    )
    result: list[dict[str, Any]] = []
    for row in rows[:top_n]:
        result.append(
            {
                "url": row["url"],
                field: round(float(row.get(field, 0.0)), 4),
                "gsc_clicks": round(float(row.get("gsc_clicks", 0.0)), 2),
                "ga4_sessions": round(float(row.get("ga4_sessions", 0.0)), 2),
                "ga4_conversions": round(float(row.get("ga4_conversions", 0.0)), 2),
            }
        )
    return result


def build_popularity_snapshot(
    merged_pages: list[dict[str, Any]],
    *,
    top_n: int = 20,
) -> dict[str, Any]:
    return {
        "top_by_gsc_clicks": _top(merged_pages, "gsc_clicks", top_n),
        "top_by_gsc_impressions": _top(merged_pages, "gsc_impressions", top_n),
        "top_by_ga4_sessions": _top(merged_pages, "ga4_sessions", top_n),
        "top_by_ga4_conversions": _top(merged_pages, "ga4_conversions", top_n),
    }


def build_trend_report(
    merged_pages: list[dict[str, Any]],
    *,
    top_n: int = 20,
) -> dict[str, Any]:
    click_gainers = sorted(
        [p for p in merged_pages if isinstance(p.get("gsc_clicks_delta_pct"), (int, float))],
        key=lambda p: float(p.get("gsc_clicks_delta_pct", 0.0)),
        reverse=True,
    )[:top_n]

    click_decliners = sorted(
        [p for p in merged_pages if isinstance(p.get("gsc_clicks_delta_pct"), (int, float))],
        key=lambda p: float(p.get("gsc_clicks_delta_pct", 0.0)),
    )[:top_n]

    session_gainers = sorted(
        [p for p in merged_pages if isinstance(p.get("ga4_sessions_delta_pct"), (int, float))],
        key=lambda p: float(p.get("ga4_sessions_delta_pct", 0.0)),
        reverse=True,
    )[:top_n]

    session_decliners = sorted(
        [p for p in merged_pages if isinstance(p.get("ga4_sessions_delta_pct"), (int, float))],
        key=lambda p: float(p.get("ga4_sessions_delta_pct", 0.0)),
    )[:top_n]

    def _pack(rows: list[dict[str, Any]], delta_field: str) -> list[dict[str, Any]]:
        return [
            {
                "url": row["url"],
                delta_field: round(float(row.get(delta_field, 0.0)), 4),
                "gsc_clicks": round(float(row.get("gsc_clicks", 0.0)), 2),
                "ga4_sessions": round(float(row.get("ga4_sessions", 0.0)), 2),
            }
            for row in rows
        ]

    return {
        "click_gainers": _pack(click_gainers, "gsc_clicks_delta_pct"),
        "click_decliners": _pack(click_decliners, "gsc_clicks_delta_pct"),
        "session_gainers": _pack(session_gainers, "ga4_sessions_delta_pct"),
        "session_decliners": _pack(session_decliners, "ga4_sessions_delta_pct"),
    }


def build_data_quality_report(
    merged_pages: list[dict[str, Any]],
    *,
    top_n_unmatched: int = 20,
) -> dict[str, Any]:
    pages_with_gsc = [p for p in merged_pages if float(p.get("gsc_impressions", 0.0)) > 0]
    pages_with_ga4 = [p for p in merged_pages if float(p.get("ga4_sessions", 0.0)) > 0]
    pages_with_both = [
        p
        for p in merged_pages
        if float(p.get("gsc_impressions", 0.0)) > 0 and float(p.get("ga4_sessions", 0.0)) > 0
    ]

    gsc_only = [
        p
        for p in merged_pages
        if float(p.get("gsc_impressions", 0.0)) > 0 and float(p.get("ga4_sessions", 0.0)) <= 0
    ]
    ga4_only = [
        p
        for p in merged_pages
        if float(p.get("ga4_sessions", 0.0)) > 0 and float(p.get("gsc_impressions", 0.0)) <= 0
    ]

    gsc_only_top = sorted(
        gsc_only, key=lambda p: float(p.get("gsc_impressions", 0.0)), reverse=True
    )[:top_n_unmatched]
    ga4_only_top = sorted(
        ga4_only, key=lambda p: float(p.get("ga4_sessions", 0.0)), reverse=True
    )[:top_n_unmatched]

    return {
        "counts": {
            "total_merged_pages": len(merged_pages),
            "pages_with_gsc": len(pages_with_gsc),
            "pages_with_ga4": len(pages_with_ga4),
            "pages_with_both": len(pages_with_both),
            "gsc_only_pages": len(gsc_only),
            "ga4_only_pages": len(ga4_only),
        },
        "top_gsc_only_pages": [
            {
                "url": p["url"],
                "gsc_impressions": round(float(p.get("gsc_impressions", 0.0)), 2),
                "gsc_clicks": round(float(p.get("gsc_clicks", 0.0)), 2),
            }
            for p in gsc_only_top
        ],
        "top_ga4_only_pages": [
            {
                "url": p["url"],
                "ga4_sessions": round(float(p.get("ga4_sessions", 0.0)), 2),
                "ga4_conversions": round(float(p.get("ga4_conversions", 0.0)), 2),
            }
            for p in ga4_only_top
        ],
    }
