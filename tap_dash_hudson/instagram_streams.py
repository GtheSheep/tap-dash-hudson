"""Stream type classes for tap-dash-hudson
from the Instagram backend, docs https://instagram-backend.dashhudson.com/docs."""
import datetime
from typing import Any, Dict, Optional, Iterable
from urllib.parse import urlparse
from urllib.parse import parse_qs

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dash_hudson.client import DashHudsonStream


class InstagramBackendStream(DashHudsonStream):
    service = 'instagram-backend'


class InstagramDailyBrandUserInsightsStream(InstagramBackendStream):
    name = "instagram_daily_brand_user_insights"
    path = "/brands/{brand_id}/brand_user_insights"
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["brand_id"] = self.config["brand_id"]
        return row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        row = response.json()
        for metric_name in row.keys():
            for label, value in zip(row[metric_name]["labels"], row[metric_name]["values"]):
                yield {
                    "date": label,
                    "metric_name": metric_name,
                    "metric_value": value,
                }


class InstagramDailyFollowersLostInsightsStream(InstagramBackendStream):
    name = "instagram_daily_followers_lost_insights"
    path = "/brands/{brand_id}/followers_lost_insights"
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["brand_id"] = self.config["brand_id"]
        return row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        row = response.json()
        for metric_name in row.keys():
            for label, value in zip(row[metric_name]["labels"], row[metric_name]["values"]):
                yield {
                    "date": label,
                    "metric_name": metric_name,
                    "metric_value": value,
                }


class InstagramDailyFollowersDemographicsStream(InstagramBackendStream):
    name = "instagram_daily_followers_demographics"
    path = "/brands/{brand_id}/followers_demographics"
    primary_keys = ["brand_id", "date", "metric_name", "metric_sub_name"]
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("brand_id", th.NumberType),
        th.Property("date", th.DateTimeType),
        th.Property("metric_name", th.StringType),
        th.Property("metric_sub_name", th.StringType),
        th.Property("metric_value", th.NumberType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        if next_page_token is not None:
            start_date = next_page_token
        else:
            start_date = self.get_starting_timestamp(context)
        params: dict = {
            "date": start_date.strftime("%Y-%m-%d"),
        }
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        start_date = datetime.datetime.strptime(parse_qs(urlparse(response.request.url).query)['date'][0], "%Y-%m-%d")
        next_date = start_date + datetime.timedelta(days=1)
        if next_date < yesterday:
            return next_date
        return None

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["brand_id"] = self.config["brand_id"]
        return row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        row = response.json()
        date = parse_qs(urlparse(response.request.url).query)['date'][0]
        for metric_name in row.keys():
            for category, value in row[metric_name].items():
                if type(value) == dict:
                    for k, v in value.items():
                        yield {
                            "date": date,
                            "metric_name": metric_name,
                            "metric_sub_name": k,
                            "metric_value": v,
                        }
                else:
                    yield {
                        "date": date,
                        "metric_name": category,
                        "metric_sub_name": category,
                        "metric_value": value,
                    }


class InstagramDailyFollowersInsightsStream(InstagramBackendStream):
    name = "instagram_daily_followers_insights"
    path = "/brands/{brand_id}/followers_insights"
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
            "fill_empty": False,
            "scale": "DAILY",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": self.config.get("end_date", (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")),
        }
        return params

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["brand_id"] = self.config["brand_id"]
        return row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        row = response.json()
        for metric_name in row.keys():
            for label, value in zip(row[metric_name]["labels"], row[metric_name]["values"]):
                yield {
                    "date": label,
                    "metric_name": metric_name,
                    "metric_value": value,
                }


class InstagramRelationshipsStream(InstagramBackendStream):
    name = "instagram_relationships"
    path = "/brands/{brand_id}/instagram/relationships"
    primary_keys = ["brand_id", "id"]
    records_jsonpath = "$.data[*]"
    replication_key = None
    schema = th.PropertiesList(
        th.Property("brand_id", th.NumberType),
        th.Property("acceptance_status", th.StringType),
        th.Property("avg_effectiveness", th.NumberType),
        th.Property("avg_emv", th.StringType),
        th.Property("avg_engagement", th.NumberType),
        th.Property("avg_reach", th.NumberType),
        th.Property("avg_total_engagements", th.NumberType),
        th.Property("cover_image", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("email", th.StringType),
        th.Property("has_piq", th.BooleanType),
        th.Property("id", th.NumberType),
        th.Property("invitation_accepted_at", th.DateTimeType),
        th.Property("invitation_revoked_at", th.DateTimeType),
        th.Property("is_fb_connected", th.BooleanType),
        th.Property("last_post_created_at", th.DateTimeType),
        th.Property("notes", th.StringType),
        th.Property("piq_avg_effectiveness", th.NumberType),
        th.Property("piq_avg_engagement", th.NumberType),
        th.Property("piq_avg_reach", th.NumberType),
        th.Property("piq_avg_total_engagements", th.NumberType),
        th.Property("piq_cover_image", th.StringType),
        th.Property("piq_last_post_created_at", th.DateTimeType),
        th.Property("piq_recent_images", th.StringType),
        th.Property("piq_total_emv", th.NumberType),
        th.Property("piq_total_followers_gained", th.NumberType),
        th.Property("piq_total_posts", th.NumberType),
        th.Property("recent_images", th.StringType),
        th.Property("relation_followers", th.NumberType),
        th.Property("relation_instagram_id", th.NumberType),
        th.Property("riq_avg_effectiveness", th.NumberType),
        th.Property("riq_avg_emv", th.StringType),
        th.Property("riq_avg_engagement", th.NumberType),
        th.Property("riq_avg_reach", th.NumberType),
        th.Property("riq_avg_total_engagements", th.NumberType),
        th.Property("riq_cover_image", th.StringType),
        th.Property("riq_last_post_created_at", th.DateTimeType),
        th.Property("riq_recent_images", th.StringType),
        th.Property("riq_story_avg_completion_rate", th.NumberType),
        th.Property("riq_story_avg_exit_rate", th.NumberType),
        th.Property("riq_story_avg_impressions", th.NumberType),
        th.Property("riq_story_avg_reach", th.NumberType),
        th.Property("riq_story_total_posts", th.NumberType),
        th.Property("riq_total_emv", th.NumberType),
        th.Property("riq_total_followers_gained", th.NumberType),
        th.Property("riq_total_posts", th.NumberType),
        th.Property("story_avg_completion_rate", th.NumberType),
        th.Property("story_avg_exit_rate", th.NumberType),
        th.Property("story_avg_impressions", th.NumberType),
        th.Property("story_avg_reach", th.NumberType),
        th.Property("story_total_posts", th.NumberType),
        th.Property("tags", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.NumberType),
                th.Property("color", th.StringType),
                th.Property("name", th.StringType),
            )
        )),
        th.Property("total_emv", th.NumberType),
        th.Property("total_followers_gained", th.NumberType),
        th.Property("total_posts", th.NumberType),
        th.Property("user", th.ObjectType(
            th.Property("avg_effectiveness", th.NumberType),
            th.Property("avg_engagement", th.NumberType),
            th.Property("avg_likes", th.NumberType),
            th.Property("avg_posts_weekly", th.NumberType),
            th.Property("avg_reach", th.NumberType),
            th.Property("avg_total_engagement", th.NumberType),
            th.Property("bio", th.StringType),
            th.Property("bio_url", th.StringType),
            th.Property("followers", th.NumberType),
            th.Property("following", th.NumberType),
            th.Property("handle", th.StringType),
            th.Property("instagram_id", th.NumberType),
            th.Property("is_business", th.NumberType),
        )),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {
            'all_relationships': True,
        }
        if next_page_token is not None:
            params['offset'] = parse_qs(urlparse(next_page_token).query)['offset'][0]
        return params

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["brand_id"] = self.config["brand_id"]
        return row
