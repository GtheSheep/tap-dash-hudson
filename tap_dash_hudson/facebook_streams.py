"""Stream type classes for tap-dash-hudson
from the Facebook backend, docs https://facebook.dashhudson.com/docs."""
import datetime
from typing import Any, Dict, Optional, Iterable

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dash_hudson.client import DashHudsonStream


class FacebookBackendStream(DashHudsonStream):
    service = 'facebook'


class FacebookBusinessesStream(FacebookBackendStream):
    name = "facebook_businesses"
    path = "/brands/{brand_id}/fb_businesses"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("profile_picture_uri", th.StringType),
    ).to_dict()


class FacebookPageMetricsStream(FacebookBackendStream):
    name = "facebook_page_metrics"
    path = "/brands/{brand_id}/page/metrics"
    primary_keys = ["brand_id", "date"]
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("brand_id", th.NumberType),
        th.Property("date", th.DateTimeType),
        th.Property("avg_effectiveness", th.NumberType),
        th.Property("avg_engagement_rate", th.NumberType),
        th.Property("engagements", th.NumberType),
        th.Property("impressions", th.NumberType),
        th.Property("link_clicks", th.NumberType),
        th.Property("new_fans", th.NumberType),
        th.Property("post_count", th.NumberType),
        th.Property("reach", th.NumberType),
        th.Property("total_fans", th.NumberType),
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["brand_id"] = self.config["brand_id"]
        return row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        rows = response.json()['timeseries_metrics']
        for row in rows:
            yield dict(**{"date": row['timestamp']}, **row['metrics'])
