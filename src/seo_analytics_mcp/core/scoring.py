from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from seo_analytics_mcp.config import Settings


@dataclass
class ScoreResult:
    score: float
    categories: list[str]
    reasons: list[str]
    recommendations: list[str]
    priority: str
    expected_impact: str
    effort: str
    confidence: float


def _priority_from_score(score: float) -> str:
    if score >= 70:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


def _expected_impact(score: float) -> str:
    if score >= 70:
        return "high"
    if score >= 45:
        return "medium"
    return "low"


def _effort(categories: list[str]) -> str:
    if "conversion_optimization" in categories and "content_refresh" in categories:
        return "medium"
    if "conversion_optimization" in categories:
        return "medium"
    return "low"


def _log_scale(value: float, multiplier: float = 8.0) -> float:
    if value <= 0:
        return 0.0
    return min(20.0, math.log10(value + 1.0) * multiplier)


def score_page(page: dict[str, Any], settings: Settings) -> ScoreResult:
    score = 0.0
    categories: list[str] = []
    reasons: list[str] = []
    recommendations: list[str] = []

    impressions = float(page.get("gsc_impressions", 0.0))
    clicks = float(page.get("gsc_clicks", 0.0))
    ctr = float(page.get("gsc_ctr", 0.0))
    position = float(page.get("gsc_position", 0.0))

    sessions = float(page.get("ga4_sessions", 0.0))
    conversion_rate = float(page.get("ga4_conversion_rate", 0.0))
    engagement_rate = float(page.get("ga4_engagement_rate", 0.0))

    clicks_delta = page.get("gsc_clicks_delta_pct")
    sessions_delta = page.get("ga4_sessions_delta_pct")

    if impressions >= settings.min_impressions_for_ctr_action and ctr < settings.target_ctr:
        ctr_gap = (settings.target_ctr - ctr) / max(settings.target_ctr, 1e-6)
        ctr_score = min(45.0, ctr_gap * 45.0 + _log_scale(impressions, 6.0))
        score += ctr_score
        categories.append("ctr_optimization")
        reasons.append(
            "High impressions with below-target CTR indicate snippet/title opportunity."
        )
        recommendations.extend(
            [
                "Rewrite title and meta description to better match dominant queries.",
                "Test stronger value proposition in the first 60 title characters.",
            ]
        )

    if sessions >= settings.min_sessions_for_conversion_action and conversion_rate < settings.target_conversion_rate:
        cr_gap = (settings.target_conversion_rate - conversion_rate) / max(
            settings.target_conversion_rate,
            1e-6,
        )
        cr_score = min(45.0, cr_gap * 42.0 + _log_scale(sessions, 6.0))
        score += cr_score
        categories.append("conversion_optimization")
        reasons.append(
            "Strong traffic with weak conversion efficiency suggests on-page UX/content friction."
        )
        recommendations.extend(
            [
                "Strengthen above-the-fold CTA and internal next-step links.",
                "Add trust proof and tighten informational-to-commercial transition sections.",
            ]
        )

    if isinstance(clicks_delta, (int, float)) and clicks_delta <= -0.2:
        drop_score = min(30.0, abs(clicks_delta) * 60.0)
        score += drop_score
        categories.append("content_refresh")
        reasons.append("Organic clicks are declining versus the previous period.")
        recommendations.append(
            "Refresh outdated sections and compare SERP competitors for intent drift."
        )

    if isinstance(sessions_delta, (int, float)) and sessions_delta <= -0.2:
        drop_score = min(30.0, abs(sessions_delta) * 55.0)
        score += drop_score
        categories.append("content_refresh")
        reasons.append("On-site sessions are declining versus the previous period.")
        recommendations.append(
            "Audit UX changes, page speed, and content relevance for recent traffic loss."
        )

    if (
        impressions >= settings.min_impressions_for_ctr_action
        and sessions >= settings.min_sessions_for_conversion_action
        and ctr >= settings.target_ctr
        and conversion_rate >= settings.target_conversion_rate
    ):
        scale_score = min(35.0, _log_scale(clicks + sessions, 7.0) + 10.0)
        score += scale_score
        categories.append("scale_winner")
        reasons.append("Page performs well in both acquisition and conversion.")
        recommendations.extend(
            [
                "Expand topic cluster around this page's highest-performing query themes.",
                "Promote this page via internal links from adjacent intent pages.",
            ]
        )

    if position > 8 and impressions > 0:
        score += min(12.0, (position - 8) * 1.5)
        reasons.append("Average position indicates page may be near page-one threshold.")

    if not categories and score <= 0:
        return ScoreResult(
            score=0.0,
            categories=[],
            reasons=[],
            recommendations=[],
            priority="low",
            expected_impact="low",
            effort="low",
            confidence=0.0,
        )

    unique_categories = sorted(set(categories))
    unique_reasons = list(dict.fromkeys(reasons))
    unique_recommendations = list(dict.fromkeys(recommendations))

    sources = 0
    if impressions > 0:
        sources += 1
    if sessions > 0:
        sources += 1

    volume_factor = 0.0
    if impressions > 0:
        volume_factor += min(0.25, math.log10(impressions + 1) / 10)
    if sessions > 0:
        volume_factor += min(0.25, math.log10(sessions + 1) / 10)

    confidence = min(1.0, 0.35 + 0.2 * sources + volume_factor)

    return ScoreResult(
        score=round(score, 2),
        categories=unique_categories,
        reasons=unique_reasons,
        recommendations=unique_recommendations,
        priority=_priority_from_score(score),
        expected_impact=_expected_impact(score),
        effort=_effort(unique_categories),
        confidence=round(confidence, 2),
    )
