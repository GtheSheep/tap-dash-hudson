"""Stream type classes for tap-dash-hudson
from the Twitter backend, docs https://twitter.dashhudson.com/docs."""
import datetime
from typing import Any, Dict, Optional, Iterable

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dash_hudson.client import DashHudsonStream


class TwitterBackendStream(DashHudsonStream):
    service = 'twitter'


class TwitterAccountStream(TwitterBackendStream):
    name = "twitter_account"
    path = "/brands/{brand_id}/account"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("ads_account_id", th.NumberType),
        th.Property("brand_id", th.NumberType),
        th.Property("handle", th.StringType),
        th.Property("last_stats_updated_at", th.DateTimeType),
        th.Property("status", th.NumberType),
        th.Property("total_followers", th.NumberType),
        th.Property("twitter_avatar", th.StringType),
        th.Property("twitter_user_id", th.StringType),
    ).to_dict()


class TwitterMetricsStream(TwitterBackendStream):
    name = "twitter_metrics"
    path = "/brands/{brand_id}/metrics"
    primary_keys = ["brand_id", "date"]
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("brand_id", th.NumberType),
        th.Property("date", th.DateTimeType),
        th.Property("engagement_rate", th.NumberType),
        th.Property("engagement_rate_organic", th.NumberType),
        th.Property("engagement_rate_promoted", th.NumberType),
        th.Property("engagement_rate_total", th.NumberType),
        th.Property("engagements", th.NumberType),
        th.Property("engagements_organic", th.NumberType),
        th.Property("engagements_promoted", th.NumberType),
        th.Property("engagements_total", th.NumberType),
        th.Property("impressions", th.NumberType),
        th.Property("impressions_organic", th.NumberType),
        th.Property("impressions_promoted", th.NumberType),
        th.Property("impressions_total", th.NumberType),
        th.Property("likes", th.NumberType),
        th.Property("likes_organic", th.NumberType),
        th.Property("likes_promoted", th.NumberType),
        th.Property("likes_total", th.NumberType),
        th.Property("new_followers", th.NumberType),
        th.Property("quote_tweets", th.NumberType),
        th.Property("replies", th.NumberType),
        th.Property("replies_organic", th.NumberType),
        th.Property("replies_promoted", th.NumberType),
        th.Property("replies_total", th.NumberType),
        th.Property("retweets", th.NumberType),
        th.Property("retweets_organic", th.NumberType),
        th.Property("retweets_promoted", th.NumberType),
        th.Property("retweets_total", th.NumberType),
        th.Property("total_followers", th.NumberType),
        th.Property("total_retweets", th.NumberType),
        th.Property("tweets_published", th.NumberType),
        th.Property("url_clicks", th.NumberType),
        th.Property("url_clicks_organic", th.NumberType),
        th.Property("url_clicks_promoted", th.NumberType),
        th.Property("url_clicks_total", th.NumberType),
        th.Property("user_profile_clicks", th.NumberType),
        th.Property("user_profile_clicks_organic", th.NumberType),
        th.Property("user_profile_clicks_promoted", th.NumberType),
        th.Property("user_profile_clicks_total", th.NumberType),
        th.Property("video_views", th.NumberType),
        th.Property("video_views_organic", th.NumberType),
        th.Property("video_views_promoted", th.NumberType),
        th.Property("video_views_total", th.NumberType),
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
        rows = response.json()['timeseries_metrics']
        for row in rows:
            yield dict(**{"date": row['timestamp']}, **row['metrics'])
