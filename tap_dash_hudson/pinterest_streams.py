"""Stream type classes for tap-dash-hudson
from the Pinterest backend, docs https://pinterest.dashhudson.com/docs."""
import datetime
from typing import Any, Dict, Optional, Iterable

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dash_hudson.client import DashHudsonStream


class PinterestBackendStream(DashHudsonStream):
    service = 'pinterest'


class PinterestAccountStream(PinterestBackendStream):
    name = "pinterest_account"
    path = "/brands/{brand_id}/account"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("avatar_url", th.StringType),
        th.Property("brand_id", th.NumberType),
        th.Property("created_at", th.DateTimeType),
        th.Property("deleted_at", th.DateTimeType),
        th.Property("ga_view_id", th.StringType),
        th.Property("has_360", th.BooleanType),
        th.Property("import_protected_pins", th.BooleanType),
        th.Property("last_stats_updated_at", th.DateTimeType),
        th.Property("pinterest_account_id", th.StringType),
        th.Property("pinterest_token", th.StringType),
        th.Property("pinterest_username", th.StringType),
        th.Property("total_followers", th.NumberType),
        th.Property("total_profile_reach", th.NumberType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()


class PinterestAccountStatsStream(PinterestBackendStream):
    name = "pinterest_account_stats"
    path = "/brands/{brand_id}/account/stats"
    primary_keys = ["brand_id", "date", "metric_name"]
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("brand_id", th.NumberType),
        th.Property("date", th.DateTimeType),
        th.Property("metric_name", th.StringType),
        th.Property("metric_value", th.NumberType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        start_date = self.get_starting_timestamp(context)
        params: dict = {
            "scale": "DAILY",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": self.config.get("end_date", (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")),
        }
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        response_json = response.json()
        for date, metrics in response_json.items():
            for metric_name, metric_value in metrics.items():
                yield {
                    "date": date,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                }
