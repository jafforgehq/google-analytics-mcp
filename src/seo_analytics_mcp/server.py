from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from urllib.parse import urlparse
from typing import Any

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from seo_analytics_mcp.config import Settings, current_and_previous_ranges, load_settings
from seo_analytics_mcp.connectors.ga4 import GA4Connector
from seo_analytics_mcp.connectors.gsc import GSCConnector
from seo_analytics_mcp.core.analysis import (
    build_data_quality_report,
    build_popularity_snapshot,
    build_trend_report,
    generate_action_items,
    merge_page_metrics,
    summarize_portfolio,
)
from seo_analytics_mcp.core.normalization import (
    normalize_ga4_rows_by_page,
    normalize_gsc_rows_by_page,
    normalize_url,
    to_float,
)

load_dotenv()

mcp = FastMCP("seo-analytics-mcp")
_settings = load_settings()
_gsc_connector: GSCConnector | None = None
_ga4_connector: GA4Connector | None = None


def _get_settings() -> Settings:
    return _settings


def _get_gsc_connector() -> GSCConnector:
    global _gsc_connector
    settings = _get_settings()
    if not settings.enable_gsc:
        raise RuntimeError("GSC connector is disabled. Set ENABLE_GSC=true.")
    if _gsc_connector is None:
        _gsc_connector = GSCConnector()
    return _gsc_connector


def _get_ga4_connector() -> GA4Connector:
    global _ga4_connector
    settings = _get_settings()
    if not settings.enable_ga4:
        raise RuntimeError("GA4 connector is disabled. Set ENABLE_GA4=true.")
    if _ga4_connector is None:
        _ga4_connector = GA4Connector()
    return _ga4_connector


def _normalize_gsc_site_url(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("site_url cannot be empty.")

    if raw.startswith("sc-domain:"):
        domain = raw[len("sc-domain:") :].strip().strip(".")
        if domain.startswith("www."):
            domain = domain[len("www.") :]
        if not domain:
            raise ValueError("Invalid sc-domain site_url value.")
        return f"sc-domain:{domain}"

    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.hostname or raw).strip().strip(".")
    if host.startswith("www."):
        host = host[len("www.") :]
    if not host:
        raise ValueError(f"Invalid site_url value: {value!r}")
    return f"sc-domain:{host}"


def _resolve_site_url(site_url: str | None) -> str:
    settings = _get_settings()
    if site_url:
        return _normalize_gsc_site_url(site_url)

    if settings.require_explicit_gsc_site_url:
        raise ValueError(
            "Missing site_url. This server requires explicit site_url in tool arguments."
        )

    if settings.default_gsc_site_url:
        return _normalize_gsc_site_url(settings.default_gsc_site_url)

    raise ValueError("Missing site_url and DEFAULT_GSC_SITE_URL is not set.")


def _resolve_property_id(property_id: str | None) -> str:
    resolved = property_id or _get_settings().default_ga4_property_id
    if not resolved:
        raise ValueError(
            "Missing property_id and DEFAULT_GA4_PROPERTY_ID is not set."
        )
    return resolved


def _default_dates(start_date: str | None, end_date: str | None) -> tuple[str, str]:
    settings = _get_settings()
    today = date.today()
    resolved_end = end_date or (today - timedelta(days=1)).isoformat()
    if start_date:
        return start_date, resolved_end

    start = date.fromisoformat(resolved_end) - timedelta(days=settings.default_lookback_days - 1)
    return start.isoformat(), resolved_end


def _fetch_page_data(
    site_url: str | None,
    property_id: str | None,
    start_date: str | None,
    end_date: str | None,
    *,
    include_previous_period: bool,
    max_rows: int,
) -> dict[str, Any]:
    settings = _get_settings()
    current_start, current_end = _default_dates(start_date, end_date)
    ranges = current_and_previous_ranges(current_start, current_end, settings.default_lookback_days)

    # Ensure current range reflects explicit values.
    ranges["current"] = (current_start, current_end)

    gsc_current: dict[str, dict[str, Any]] = {}
    gsc_previous: dict[str, dict[str, Any]] = {}
    ga4_current: dict[str, dict[str, Any]] = {}
    ga4_previous: dict[str, dict[str, Any]] = {}

    resolved_site_url: str | None = site_url or settings.default_gsc_site_url
    resolved_property_id = property_id or settings.default_ga4_property_id

    if settings.enable_gsc:
        resolved_site_url = _resolve_site_url(site_url)
        gsc = _get_gsc_connector()
        current_resp = gsc.search_analytics_all(
            resolved_site_url,
            ranges["current"][0],
            ranges["current"][1],
            dimensions=["page"],
            search_type="web",
            aggregation_type="byPage",
            max_rows=max_rows,
        )
        gsc_current = normalize_gsc_rows_by_page(
            current_resp["rows"],
            dimensions=["page"],
            base_url=settings.canonical_base_url,
        )

        if include_previous_period:
            prev_resp = gsc.search_analytics_all(
                resolved_site_url,
                ranges["previous"][0],
                ranges["previous"][1],
                dimensions=["page"],
                search_type="web",
                aggregation_type="byPage",
                max_rows=max_rows,
            )
            gsc_previous = normalize_gsc_rows_by_page(
                prev_resp["rows"],
                dimensions=["page"],
                base_url=settings.canonical_base_url,
            )

    if settings.enable_ga4 and resolved_property_id:
        ga4 = _get_ga4_connector()
        common_kwargs: dict[str, Any] = {
            "dimensions": ["landingPagePlusQueryString"],
            "metrics": [
                "sessions",
                "engagedSessions",
                "conversions",
                "totalUsers",
                "screenPageViews",
                "userEngagementDuration",
            ],
            "order_bys": [{"metric": "sessions", "desc": True}],
            "max_rows": max_rows,
        }

        current_resp = ga4.run_report_all(
            resolved_property_id,
            ranges["current"][0],
            ranges["current"][1],
            **common_kwargs,
        )
        ga4_current = normalize_ga4_rows_by_page(
            current_resp["rows"],
            base_url=settings.canonical_base_url,
        )

        if include_previous_period:
            prev_resp = ga4.run_report_all(
                resolved_property_id,
                ranges["previous"][0],
                ranges["previous"][1],
                **common_kwargs,
            )
            ga4_previous = normalize_ga4_rows_by_page(
                prev_resp["rows"],
                base_url=settings.canonical_base_url,
            )

    merged = merge_page_metrics(
        gsc_current,
        ga4_current,
        gsc_previous=gsc_previous if include_previous_period else None,
        ga4_previous=ga4_previous if include_previous_period else None,
    )

    return {
        "ranges": ranges,
        "site_url": resolved_site_url,
        "property_id": resolved_property_id,
        "gsc_pages": len(gsc_current),
        "ga4_pages": len(ga4_current),
        "merged_pages": merged,
    }


@mcp.tool()
def capabilities() -> dict[str, Any]:
    """Show enabled connectors, defaults, and key analysis thresholds."""
    settings = _get_settings()
    return {
        "connectors": {
            "gsc_enabled": settings.enable_gsc,
            "ga4_enabled": settings.enable_ga4,
        },
        "defaults": {
            "gsc_site_url": (
                _normalize_gsc_site_url(settings.default_gsc_site_url)
                if settings.default_gsc_site_url
                else None
            ),
            "ga4_property_id": settings.default_ga4_property_id,
            "lookback_days": settings.default_lookback_days,
            "canonical_base_url": settings.canonical_base_url,
            "require_explicit_gsc_site_url": settings.require_explicit_gsc_site_url,
        },
        "analysis_thresholds": {
            "min_impressions_for_ctr_action": settings.min_impressions_for_ctr_action,
            "min_sessions_for_conversion_action": settings.min_sessions_for_conversion_action,
            "target_ctr": settings.target_ctr,
            "target_conversion_rate": settings.target_conversion_rate,
            "default_max_action_items": settings.default_max_action_items,
        },
        "tools": [
            "gsc_list_sites",
            "gsc_search_analytics_raw",
            "gsc_top_pages",
            "gsc_top_queries",
            "gsc_query_page_pairs",
            "ga4_run_report_raw",
            "ga4_landing_pages",
            "ga4_channel_report",
            "analytics_merge_page_metrics",
            "analytics_generate_action_items",
            "analytics_popularity_snapshot",
            "analytics_trend_report",
            "analytics_data_quality_report",
            "analytics_query_page_opportunities",
            "analytics_topic_clusters",
        ],
    }


@mcp.tool()
def gsc_list_sites() -> dict[str, Any]:
    """List Search Console properties available to the authenticated account."""
    connector = _get_gsc_connector()
    sites = connector.list_sites()
    return {"count": len(sites), "sites": sites}


@mcp.tool()
def gsc_search_analytics_raw(
    site_url: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    dimensions: list[str] | None = None,
    row_limit: int = 25000,
    start_row: int = 0,
    search_type: str = "web",
    data_state: str | None = None,
    aggregation_type: str | None = None,
    dimension_filter_groups: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run a raw Search Console Search Analytics query with full options."""
    connector = _get_gsc_connector()
    resolved_site = _resolve_site_url(site_url)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    response = connector.search_analytics(
        resolved_site,
        resolved_start,
        resolved_end,
        dimensions=dimensions,
        row_limit=row_limit,
        start_row=start_row,
        search_type=search_type,
        data_state=data_state,
        aggregation_type=aggregation_type,
        dimension_filter_groups=dimension_filter_groups,
    )

    return {
        "site_url": resolved_site,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "dimensions": dimensions or [],
        "row_count": len(response.get("rows", [])),
        "response": response,
    }


@mcp.tool()
def gsc_top_pages(
    site_url: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    search_type: str = "web",
    top_n: int = 50,
    max_rows: int = 50000,
) -> dict[str, Any]:
    """Return top pages from GSC with clicks, impressions, CTR, and position."""
    connector = _get_gsc_connector()
    settings = _get_settings()
    resolved_site = _resolve_site_url(site_url)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    response = connector.search_analytics_all(
        resolved_site,
        resolved_start,
        resolved_end,
        dimensions=["page"],
        search_type=search_type,
        aggregation_type="byPage",
        max_rows=max_rows,
    )

    pages = normalize_gsc_rows_by_page(
        response["rows"],
        dimensions=["page"],
        base_url=settings.canonical_base_url,
    )

    rows = [
        {
            "url": url,
            "clicks": round(float(item.get("gsc_clicks", 0.0)), 2),
            "impressions": round(float(item.get("gsc_impressions", 0.0)), 2),
            "ctr": round(float(item.get("gsc_ctr", 0.0)), 4),
            "position": round(float(item.get("gsc_position", 0.0)), 2),
        }
        for url, item in pages.items()
    ]
    rows.sort(key=lambda r: r["clicks"], reverse=True)

    return {
        "site_url": resolved_site,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "total_pages": len(rows),
        "rows": rows[: max(1, top_n)],
    }


@mcp.tool()
def gsc_top_queries(
    site_url: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    search_type: str = "web",
    top_n: int = 50,
    max_rows: int = 50000,
) -> dict[str, Any]:
    """Return top queries from GSC by clicks."""
    connector = _get_gsc_connector()
    resolved_site = _resolve_site_url(site_url)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    response = connector.search_analytics_all(
        resolved_site,
        resolved_start,
        resolved_end,
        dimensions=["query"],
        search_type=search_type,
        max_rows=max_rows,
    )

    rows: list[dict[str, Any]] = []
    for row in response["rows"]:
        keys = row.get("keys", [])
        if not keys:
            continue
        rows.append(
            {
                "query": keys[0],
                "clicks": round(to_float(row.get("clicks")), 2),
                "impressions": round(to_float(row.get("impressions")), 2),
                "ctr": round(to_float(row.get("ctr")), 4),
                "position": round(to_float(row.get("position")), 2),
            }
        )

    rows.sort(key=lambda r: r["clicks"], reverse=True)

    return {
        "site_url": resolved_site,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "total_queries": len(rows),
        "rows": rows[: max(1, top_n)],
    }


@mcp.tool()
def gsc_query_page_pairs(
    site_url: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    search_type: str = "web",
    top_n: int = 100,
    max_rows: int = 100000,
) -> dict[str, Any]:
    """Return query+page combinations from GSC for intent/page matching analysis."""
    connector = _get_gsc_connector()
    resolved_site = _resolve_site_url(site_url)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    response = connector.search_analytics_all(
        resolved_site,
        resolved_start,
        resolved_end,
        dimensions=["query", "page"],
        search_type=search_type,
        max_rows=max_rows,
    )

    rows: list[dict[str, Any]] = []
    for row in response["rows"]:
        keys = row.get("keys", [])
        if len(keys) < 2:
            continue
        rows.append(
            {
                "query": keys[0],
                "page": keys[1],
                "clicks": round(to_float(row.get("clicks")), 2),
                "impressions": round(to_float(row.get("impressions")), 2),
                "ctr": round(to_float(row.get("ctr")), 4),
                "position": round(to_float(row.get("position")), 2),
            }
        )

    rows.sort(key=lambda r: (r["impressions"], r["clicks"]), reverse=True)

    return {
        "site_url": resolved_site,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "total_pairs": len(rows),
        "rows": rows[: max(1, top_n)],
    }


@mcp.tool()
def ga4_run_report_raw(
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    limit: int = 10000,
    offset: int = 0,
    keep_empty_rows: bool = False,
    currency_code: str | None = None,
    dimension_filter: dict[str, Any] | None = None,
    metric_filter: dict[str, Any] | None = None,
    order_bys: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run raw GA4 report with optional dimension/metric filters and ordering."""
    connector = _get_ga4_connector()
    resolved_property = _resolve_property_id(property_id)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    report = connector.run_report(
        resolved_property,
        resolved_start,
        resolved_end,
        dimensions=dimensions or ["landingPagePlusQueryString"],
        metrics=metrics
        or [
            "sessions",
            "engagedSessions",
            "conversions",
            "totalUsers",
            "screenPageViews",
        ],
        limit=limit,
        offset=offset,
        keep_empty_rows=keep_empty_rows,
        currency_code=currency_code,
        dimension_filter=dimension_filter,
        metric_filter=metric_filter,
        order_bys=order_bys,
    )

    return report


@mcp.tool()
def ga4_landing_pages(
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    top_n: int = 50,
    max_rows: int = 50000,
) -> dict[str, Any]:
    """Return GA4 landing pages with engagement and conversion metrics."""
    connector = _get_ga4_connector()
    settings = _get_settings()
    resolved_property = _resolve_property_id(property_id)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    report = connector.run_report_all(
        resolved_property,
        resolved_start,
        resolved_end,
        dimensions=["landingPagePlusQueryString"],
        metrics=[
            "sessions",
            "engagedSessions",
            "conversions",
            "totalUsers",
            "screenPageViews",
            "userEngagementDuration",
        ],
        order_bys=[{"metric": "sessions", "desc": True}],
        max_rows=max_rows,
    )

    pages = normalize_ga4_rows_by_page(
        report["rows"],
        base_url=settings.canonical_base_url,
    )

    rows = [
        {
            "url": url,
            "sessions": round(float(item.get("ga4_sessions", 0.0)), 2),
            "engaged_sessions": round(float(item.get("ga4_engaged_sessions", 0.0)), 2),
            "engagement_rate": round(float(item.get("ga4_engagement_rate", 0.0)), 4),
            "conversions": round(float(item.get("ga4_conversions", 0.0)), 2),
            "conversion_rate": round(float(item.get("ga4_conversion_rate", 0.0)), 4),
            "total_users": round(float(item.get("ga4_total_users", 0.0)), 2),
            "screen_page_views": round(float(item.get("ga4_screen_page_views", 0.0)), 2),
            "user_engagement_duration": round(
                float(item.get("ga4_user_engagement_duration", 0.0)), 2
            ),
        }
        for url, item in pages.items()
    ]
    rows.sort(key=lambda r: r["sessions"], reverse=True)

    return {
        "property_id": resolved_property,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "total_pages": len(rows),
        "rows": rows[: max(1, top_n)],
    }


@mcp.tool()
def ga4_channel_report(
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    top_n: int = 50,
) -> dict[str, Any]:
    """Break down GA4 landing pages by default channel group."""
    connector = _get_ga4_connector()
    resolved_property = _resolve_property_id(property_id)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    report = connector.run_report(
        resolved_property,
        resolved_start,
        resolved_end,
        dimensions=["sessionDefaultChannelGroup", "landingPagePlusQueryString"],
        metrics=["sessions", "engagedSessions", "conversions", "totalUsers"],
        limit=max(1000, top_n),
        order_bys=[{"metric": "sessions", "desc": True}],
    )

    rows = sorted(report["rows"], key=lambda r: to_float(r.get("sessions")), reverse=True)
    return {
        "property_id": resolved_property,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "row_count": len(rows),
        "rows": rows[: max(1, top_n)],
    }


@mcp.tool()
def analytics_merge_page_metrics(
    site_url: str | None = None,
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    include_previous_period: bool = True,
    max_rows: int = 50000,
) -> dict[str, Any]:
    """Merge normalized GSC + GA4 page metrics into one dataset."""
    data = _fetch_page_data(
        site_url,
        property_id,
        start_date,
        end_date,
        include_previous_period=include_previous_period,
        max_rows=max_rows,
    )

    portfolio = summarize_portfolio(data["merged_pages"])

    return {
        "ranges": data["ranges"],
        "site_url": data["site_url"],
        "property_id": data["property_id"],
        "source_counts": {
            "gsc_pages": data["gsc_pages"],
            "ga4_pages": data["ga4_pages"],
            "merged_pages": len(data["merged_pages"]),
        },
        "portfolio_summary": portfolio,
        "pages": data["merged_pages"],
    }


@mcp.tool()
def analytics_generate_action_items(
    site_url: str | None = None,
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    include_previous_period: bool = True,
    max_rows: int = 50000,
    max_items: int | None = None,
    priorities: list[str] | None = None,
) -> dict[str, Any]:
    """Generate prioritized SEO and content action items from merged data."""
    settings = _get_settings()
    data = _fetch_page_data(
        site_url,
        property_id,
        start_date,
        end_date,
        include_previous_period=include_previous_period,
        max_rows=max_rows,
    )

    items = generate_action_items(data["merged_pages"], settings, max_items=max_items)

    if priorities:
        allowed = {p.lower().strip() for p in priorities}
        items = [i for i in items if str(i.get("priority", "")).lower() in allowed]

    priority_counts = Counter(item["priority"] for item in items)
    category_counts = Counter(item["category"] for item in items)

    return {
        "ranges": data["ranges"],
        "site_url": data["site_url"],
        "property_id": data["property_id"],
        "summary": {
            "total_items": len(items),
            "priority_counts": dict(priority_counts),
            "category_counts": dict(category_counts),
            "portfolio": summarize_portfolio(data["merged_pages"]),
        },
        "items": items,
    }


@mcp.tool()
def analytics_popularity_snapshot(
    site_url: str | None = None,
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    top_n: int = 20,
    max_rows: int = 50000,
) -> dict[str, Any]:
    """Get top pages by clicks, impressions, sessions, and conversions."""
    data = _fetch_page_data(
        site_url,
        property_id,
        start_date,
        end_date,
        include_previous_period=False,
        max_rows=max_rows,
    )

    return {
        "ranges": data["ranges"],
        "site_url": data["site_url"],
        "property_id": data["property_id"],
        "snapshot": build_popularity_snapshot(data["merged_pages"], top_n=max(1, top_n)),
    }


@mcp.tool()
def analytics_trend_report(
    site_url: str | None = None,
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    top_n: int = 20,
    max_rows: int = 50000,
) -> dict[str, Any]:
    """Compare current vs previous period to surface gainers and decliners."""
    data = _fetch_page_data(
        site_url,
        property_id,
        start_date,
        end_date,
        include_previous_period=True,
        max_rows=max_rows,
    )

    return {
        "ranges": data["ranges"],
        "site_url": data["site_url"],
        "property_id": data["property_id"],
        "trends": build_trend_report(data["merged_pages"], top_n=max(1, top_n)),
    }


@mcp.tool()
def analytics_data_quality_report(
    site_url: str | None = None,
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    max_rows: int = 50000,
    top_n_unmatched: int = 20,
) -> dict[str, Any]:
    """Show merge coverage and top URL mismatches between GSC and GA4."""
    data = _fetch_page_data(
        site_url,
        property_id,
        start_date,
        end_date,
        include_previous_period=False,
        max_rows=max_rows,
    )
    return {
        "ranges": data["ranges"],
        "site_url": data["site_url"],
        "property_id": data["property_id"],
        "quality": build_data_quality_report(
            data["merged_pages"],
            top_n_unmatched=max(1, top_n_unmatched),
        ),
    }


@mcp.tool()
def analytics_query_page_opportunities(
    site_url: str | None = None,
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    min_impressions: int = 100,
    top_n: int = 50,
    max_rows: int = 100000,
) -> dict[str, Any]:
    """Find high-impression query/page pairs with weak CTR and weak on-page outcomes."""
    settings = _get_settings()
    connector = _get_gsc_connector()
    resolved_site = _resolve_site_url(site_url)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    pairs_resp = connector.search_analytics_all(
        resolved_site,
        resolved_start,
        resolved_end,
        dimensions=["query", "page"],
        search_type="web",
        max_rows=max_rows,
    )

    ga4_pages: dict[str, dict[str, Any]] = {}
    resolved_property = property_id or settings.default_ga4_property_id
    if settings.enable_ga4 and resolved_property:
        ga4 = _get_ga4_connector()
        ga4_resp = ga4.run_report_all(
            _resolve_property_id(resolved_property),
            resolved_start,
            resolved_end,
            dimensions=["landingPagePlusQueryString"],
            metrics=["sessions", "engagedSessions", "conversions"],
            order_bys=[{"metric": "sessions", "desc": True}],
            max_rows=max_rows,
        )
        ga4_pages = normalize_ga4_rows_by_page(
            ga4_resp["rows"],
            base_url=settings.canonical_base_url,
        )

    opportunities: list[dict[str, Any]] = []

    for row in pairs_resp["rows"]:
        keys = row.get("keys", [])
        if len(keys) < 2:
            continue

        query = str(keys[0])
        page = str(keys[1])
        normalized_page = normalize_url(page, base_url=settings.canonical_base_url)

        impressions = to_float(row.get("impressions"))
        clicks = to_float(row.get("clicks"))
        ctr = to_float(row.get("ctr"))
        position = to_float(row.get("position"))

        if impressions < min_impressions:
            continue
        if ctr >= settings.target_ctr:
            continue

        ga4_data = ga4_pages.get(normalized_page) or ga4_pages.get(page)
        sessions = to_float((ga4_data or {}).get("ga4_sessions"))
        conversion_rate = to_float((ga4_data or {}).get("ga4_conversion_rate"))

        ctr_gap = max(0.0, settings.target_ctr - ctr) / max(settings.target_ctr, 1e-6)
        opp_score = min(100.0, ctr_gap * 60 + (impressions / 1000) * 4)
        if sessions > 0 and conversion_rate < settings.target_conversion_rate:
            opp_score += 15

        opportunities.append(
            {
                "query": query,
                "page": page,
                "score": round(opp_score, 2),
                "clicks": round(clicks, 2),
                "impressions": round(impressions, 2),
                "ctr": round(ctr, 4),
                "position": round(position, 2),
                "ga4_sessions": round(sessions, 2),
                "ga4_conversion_rate": round(conversion_rate, 4),
                "recommended_actions": [
                    "Align title/H1 with the query intent and expected SERP promise.",
                    "Add/strengthen content section directly answering this query.",
                    "Improve internal links from related pages using intent-matching anchor text.",
                ],
            }
        )

    opportunities.sort(key=lambda x: x["score"], reverse=True)

    return {
        "site_url": resolved_site,
        "property_id": resolved_property,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "total_opportunities": len(opportunities),
        "opportunities": opportunities[: max(1, top_n)],
    }


@mcp.tool()
def analytics_topic_clusters(
    site_url: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    min_query_impressions: int = 50,
    top_n_topics: int = 30,
    max_rows: int = 100000,
) -> dict[str, Any]:
    """Extract high-impact query token clusters to guide content focus."""
    connector = _get_gsc_connector()
    resolved_site = _resolve_site_url(site_url)
    resolved_start, resolved_end = _default_dates(start_date, end_date)

    response = connector.search_analytics_all(
        resolved_site,
        resolved_start,
        resolved_end,
        dimensions=["query"],
        search_type="web",
        max_rows=max_rows,
    )

    token_stats: dict[str, dict[str, float]] = {}
    stop_words = {
        "the",
        "a",
        "an",
        "to",
        "of",
        "for",
        "in",
        "on",
        "with",
        "and",
        "or",
        "is",
        "are",
        "what",
        "how",
        "why",
        "when",
    }

    for row in response["rows"]:
        keys = row.get("keys", [])
        if not keys:
            continue
        query = str(keys[0]).strip().lower()
        if not query:
            continue

        impressions = to_float(row.get("impressions"))
        clicks = to_float(row.get("clicks"))
        ctr = to_float(row.get("ctr"))

        if impressions < min_query_impressions:
            continue

        tokens = [t for t in query.replace("-", " ").split() if len(t) >= 3]
        tokens = [t for t in tokens if t not in stop_words]
        unique_tokens = set(tokens)
        for token in unique_tokens:
            data = token_stats.setdefault(
                token,
                {
                    "queries": 0.0,
                    "impressions": 0.0,
                    "clicks": 0.0,
                    "ctr_weighted": 0.0,
                },
            )
            data["queries"] += 1
            data["impressions"] += impressions
            data["clicks"] += clicks
            data["ctr_weighted"] += ctr * impressions

    topics: list[dict[str, Any]] = []
    for token, stats in token_stats.items():
        impressions = stats["impressions"]
        weighted_ctr = stats["ctr_weighted"] / impressions if impressions > 0 else 0.0
        topics.append(
            {
                "topic": token,
                "query_count": int(stats["queries"]),
                "impressions": round(impressions, 2),
                "clicks": round(stats["clicks"], 2),
                "ctr": round(weighted_ctr, 4),
            }
        )

    topics.sort(key=lambda t: (t["impressions"], t["query_count"]), reverse=True)

    return {
        "site_url": resolved_site,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "total_topics": len(topics),
        "topics": topics[: max(1, top_n_topics)],
    }


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
