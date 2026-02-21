from __future__ import annotations

from typing import Any, Sequence

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    NumericValue,
    OrderBy,
    RunReportRequest,
)

from seo_analytics_mcp.auth import get_google_credentials


class GA4Connector:
    SCOPES = ("https://www.googleapis.com/auth/analytics.readonly",)

    def __init__(self) -> None:
        credentials = get_google_credentials(self.SCOPES)
        self._client = BetaAnalyticsDataClient(credentials=credentials)

    def _string_match_type(self, op: str) -> Filter.StringFilter.MatchType:
        lookup = {
            "EXACT": Filter.StringFilter.MatchType.EXACT,
            "BEGINS_WITH": Filter.StringFilter.MatchType.BEGINS_WITH,
            "ENDS_WITH": Filter.StringFilter.MatchType.ENDS_WITH,
            "CONTAINS": Filter.StringFilter.MatchType.CONTAINS,
            "FULL_REGEXP": Filter.StringFilter.MatchType.FULL_REGEXP,
            "PARTIAL_REGEXP": Filter.StringFilter.MatchType.PARTIAL_REGEXP,
        }
        try:
            return lookup[op.upper()]
        except KeyError as exc:
            raise ValueError(f"Unsupported string filter op: {op}") from exc

    def _numeric_operation(self, op: str) -> Filter.NumericFilter.Operation:
        lookup = {
            "EQUAL": Filter.NumericFilter.Operation.EQUAL,
            "GREATER_THAN": Filter.NumericFilter.Operation.GREATER_THAN,
            "GREATER_THAN_OR_EQUAL": Filter.NumericFilter.Operation.GREATER_THAN_OR_EQUAL,
            "LESS_THAN": Filter.NumericFilter.Operation.LESS_THAN,
            "LESS_THAN_OR_EQUAL": Filter.NumericFilter.Operation.LESS_THAN_OR_EQUAL,
        }
        try:
            return lookup[op.upper()]
        except KeyError as exc:
            raise ValueError(f"Unsupported numeric op: {op}") from exc

    def _number_value(self, value: float | int) -> NumericValue:
        if isinstance(value, int):
            return NumericValue(int64_value=value)
        return NumericValue(double_value=float(value))

    def _build_filter_expression(self, spec: dict[str, Any]) -> FilterExpression:
        if "and" in spec:
            return FilterExpression(
                and_group=FilterExpressionList(
                    expressions=[
                        self._build_filter_expression(item) for item in spec["and"]
                    ]
                )
            )

        if "or" in spec:
            return FilterExpression(
                or_group=FilterExpressionList(
                    expressions=[
                        self._build_filter_expression(item) for item in spec["or"]
                    ]
                )
            )

        if "not" in spec:
            return FilterExpression(
                not_expression=self._build_filter_expression(spec["not"])
            )

        field = spec["field"]
        op = str(spec.get("op", "EXACT")).upper()

        if op == "IN_LIST":
            in_list = Filter.InListFilter(values=[str(v) for v in spec.get("values", [])])
            return FilterExpression(filter=Filter(field_name=field, in_list_filter=in_list))

        if op.startswith("NUMERIC_"):
            numeric_op = op.replace("NUMERIC_", "", 1)
            if numeric_op == "BETWEEN":
                from_value = self._number_value(spec["from"])
                to_value = self._number_value(spec["to"])
                between = Filter.BetweenFilter(from_value=from_value, to_value=to_value)
                return FilterExpression(
                    filter=Filter(field_name=field, between_filter=between)
                )

            value = self._number_value(spec["value"])
            numeric_filter = Filter.NumericFilter(
                operation=self._numeric_operation(numeric_op),
                value=value,
            )
            return FilterExpression(
                filter=Filter(field_name=field, numeric_filter=numeric_filter)
            )

        string_filter = Filter.StringFilter(
            match_type=self._string_match_type(op),
            value=str(spec["value"]),
            case_sensitive=bool(spec.get("case_sensitive", False)),
        )
        return FilterExpression(
            filter=Filter(field_name=field, string_filter=string_filter)
        )

    def _build_order_bys(self, order_bys: Sequence[dict[str, Any]]) -> list[OrderBy]:
        result: list[OrderBy] = []
        for item in order_bys:
            desc = bool(item.get("desc", False))
            if "metric" in item:
                result.append(
                    OrderBy(
                        metric=OrderBy.MetricOrderBy(metric_name=item["metric"]),
                        desc=desc,
                    )
                )
            elif "dimension" in item:
                result.append(
                    OrderBy(
                        dimension=OrderBy.DimensionOrderBy(dimension_name=item["dimension"]),
                        desc=desc,
                    )
                )
            else:
                raise ValueError(
                    "Order by items require either 'metric' or 'dimension'."
                )
        return result

    def run_report(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
        *,
        dimensions: Sequence[str],
        metrics: Sequence[str],
        limit: int = 10000,
        offset: int = 0,
        keep_empty_rows: bool = False,
        currency_code: str | None = None,
        dimension_filter: dict[str, Any] | None = None,
        metric_filter: dict[str, Any] | None = None,
        order_bys: Sequence[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=name) for name in dimensions],
            metrics=[Metric(name=name) for name in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit,
            offset=offset,
            keep_empty_rows=keep_empty_rows,
        )

        if currency_code:
            request.currency_code = currency_code
        if dimension_filter:
            request.dimension_filter = self._build_filter_expression(dimension_filter)
        if metric_filter:
            request.metric_filter = self._build_filter_expression(metric_filter)
        if order_bys:
            request.order_bys.extend(self._build_order_bys(order_bys))

        response = self._client.run_report(request)

        rows: list[dict[str, Any]] = []
        dim_headers = [h.name for h in response.dimension_headers]
        metric_headers = [h.name for h in response.metric_headers]

        for row in response.rows:
            row_data: dict[str, Any] = {}
            for i, header in enumerate(dim_headers):
                row_data[header] = row.dimension_values[i].value
            for i, header in enumerate(metric_headers):
                value = row.metric_values[i].value
                try:
                    if "." in value:
                        row_data[header] = float(value)
                    else:
                        row_data[header] = int(value)
                except ValueError:
                    row_data[header] = value
            rows.append(row_data)

        return {
            "property_id": property_id,
            "start_date": start_date,
            "end_date": end_date,
            "dimensions": list(dimensions),
            "metrics": list(metrics),
            "rows": rows,
            "row_count": response.row_count,
            "returned_rows": len(rows),
            "limit": limit,
            "offset": offset,
        }

    def run_report_all(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
        *,
        dimensions: Sequence[str],
        metrics: Sequence[str],
        page_size: int = 10000,
        max_rows: int = 100000,
        keep_empty_rows: bool = False,
        currency_code: str | None = None,
        dimension_filter: dict[str, Any] | None = None,
        metric_filter: dict[str, Any] | None = None,
        order_bys: Sequence[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        all_rows: list[dict[str, Any]] = []
        offset = 0
        row_count: int | None = None

        while offset < max_rows:
            response = self.run_report(
                property_id,
                start_date,
                end_date,
                dimensions=dimensions,
                metrics=metrics,
                limit=min(page_size, max_rows - offset),
                offset=offset,
                keep_empty_rows=keep_empty_rows,
                currency_code=currency_code,
                dimension_filter=dimension_filter,
                metric_filter=metric_filter,
                order_bys=order_bys,
            )

            rows = response["rows"]
            row_count = response.get("row_count")
            if not rows:
                break

            all_rows.extend(rows)
            if len(rows) < page_size:
                break
            if row_count is not None and len(all_rows) >= row_count:
                break

            offset += len(rows)

        return {
            "property_id": property_id,
            "start_date": start_date,
            "end_date": end_date,
            "dimensions": list(dimensions),
            "metrics": list(metrics),
            "rows": all_rows,
            "returned_rows": len(all_rows),
            "row_count": row_count if row_count is not None else len(all_rows),
            "page_size": page_size,
            "max_rows": max_rows,
        }
