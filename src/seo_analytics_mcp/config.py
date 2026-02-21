from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def as_date(value: str | None, fallback: date) -> date:
    if not value:
        return fallback
    return parse_date(value)


@dataclass(frozen=True)
class Settings:
    enable_gsc: bool
    enable_ga4: bool
    require_explicit_gsc_site_url: bool
    default_gsc_site_url: str | None
    default_ga4_property_id: str | None
    default_lookback_days: int
    canonical_base_url: str | None

    min_impressions_for_ctr_action: int
    min_sessions_for_conversion_action: int
    target_ctr: float
    target_conversion_rate: float
    default_max_action_items: int


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    return Settings(
        enable_gsc=_parse_bool(os.getenv("ENABLE_GSC"), True),
        enable_ga4=_parse_bool(os.getenv("ENABLE_GA4"), True),
        require_explicit_gsc_site_url=_parse_bool(
            os.getenv("REQUIRE_EXPLICIT_GSC_SITE_URL"),
            True,
        ),
        default_gsc_site_url=os.getenv("DEFAULT_GSC_SITE_URL") or None,
        default_ga4_property_id=os.getenv("DEFAULT_GA4_PROPERTY_ID") or None,
        default_lookback_days=_parse_int(os.getenv("DEFAULT_LOOKBACK_DAYS"), 28),
        canonical_base_url=os.getenv("CANONICAL_BASE_URL") or None,
        min_impressions_for_ctr_action=_parse_int(
            os.getenv("MIN_IMPRESSIONS_FOR_CTR_ACTION"), 200
        ),
        min_sessions_for_conversion_action=_parse_int(
            os.getenv("MIN_SESSIONS_FOR_CONVERSION_ACTION"), 50
        ),
        target_ctr=float(os.getenv("TARGET_CTR", "0.03")),
        target_conversion_rate=float(os.getenv("TARGET_CONVERSION_RATE", "0.02")),
        default_max_action_items=_parse_int(os.getenv("DEFAULT_MAX_ACTION_ITEMS"), 30),
    )


def current_and_previous_ranges(
    start_date: str | None,
    end_date: str | None,
    lookback_days: int,
) -> dict[str, tuple[str, str]]:
    today = date.today()
    end = as_date(end_date, today - timedelta(days=1))

    if start_date:
        start = parse_date(start_date)
    else:
        start = end - timedelta(days=lookback_days - 1)

    span_days = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=span_days - 1)

    return {
        "current": (start.isoformat(), end.isoformat()),
        "previous": (prev_start.isoformat(), prev_end.isoformat()),
    }
