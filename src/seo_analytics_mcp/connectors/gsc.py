from __future__ import annotations

from typing import Any, Sequence

from googleapiclient.discovery import build

from seo_analytics_mcp.auth import get_google_credentials


class GSCConnector:
    SCOPES = ("https://www.googleapis.com/auth/webmasters.readonly",)

    def __init__(self) -> None:
        credentials = get_google_credentials(self.SCOPES)
        self._service = build(
            "searchconsole",
            "v1",
            credentials=credentials,
            cache_discovery=False,
        )

    def list_sites(self) -> list[dict[str, Any]]:
        response = self._service.sites().list().execute()
        return response.get("siteEntry", [])

    def search_analytics(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
        *,
        dimensions: Sequence[str] | None = None,
        row_limit: int = 25000,
        start_row: int = 0,
        search_type: str = "web",
        data_state: str | None = None,
        aggregation_type: str | None = None,
        dimension_filter_groups: Sequence[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "startDate": start_date,
            "endDate": end_date,
            "rowLimit": row_limit,
            "startRow": start_row,
            "type": search_type,
        }

        if dimensions:
            body["dimensions"] = list(dimensions)
        if data_state:
            body["dataState"] = data_state
        if aggregation_type:
            body["aggregationType"] = aggregation_type
        if dimension_filter_groups:
            body["dimensionFilterGroups"] = list(dimension_filter_groups)

        return self._service.searchanalytics().query(siteUrl=site_url, body=body).execute()

    def search_analytics_all(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
        *,
        dimensions: Sequence[str] | None = None,
        search_type: str = "web",
        data_state: str | None = None,
        aggregation_type: str | None = None,
        dimension_filter_groups: Sequence[dict[str, Any]] | None = None,
        page_size: int = 25000,
        max_rows: int = 100000,
    ) -> dict[str, Any]:
        all_rows: list[dict[str, Any]] = []
        start_row = 0

        while start_row < max_rows:
            response = self.search_analytics(
                site_url,
                start_date,
                end_date,
                dimensions=dimensions,
                row_limit=min(page_size, max_rows - start_row),
                start_row=start_row,
                search_type=search_type,
                data_state=data_state,
                aggregation_type=aggregation_type,
                dimension_filter_groups=dimension_filter_groups,
            )
            rows = response.get("rows", [])
            if not rows:
                break

            all_rows.extend(rows)
            if len(rows) < page_size:
                break

            start_row += len(rows)

        return {
            "rows": all_rows,
            "row_count": len(all_rows),
            "response_aggregation_type": aggregation_type,
            "search_type": search_type,
            "dimensions": list(dimensions or []),
            "start_date": start_date,
            "end_date": end_date,
            "site_url": site_url,
        }
