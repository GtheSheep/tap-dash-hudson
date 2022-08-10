"""DashHudson tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_dash_hudson.facebook_streams import (
    FacebookBusinessesStream,
    FacebookPageMetricsStream,
)
from tap_dash_hudson.instagram_streams import (
    InstagramDailyBrandUserInsightsStream,
    InstagramDailyFollowersLostInsightsStream,
    InstagramDailyFollowersDemographicsStream,
    InstagramDailyFollowersInsightsStream,
    InstagramRelationshipsStream,
)
from tap_dash_hudson.pinterest_streams import (
    PinterestAccountStream,
    PinterestAccountStatsStream,
)
from tap_dash_hudson.twitter_streams import (
    TwitterAccountStream,
    TwitterMetricsStream,
)

FACEBOOK_STREAMS = [
    FacebookBusinessesStream,
    FacebookPageMetricsStream,
]
INSTAGRAM_STREAMS = [
    InstagramDailyBrandUserInsightsStream,
    InstagramDailyFollowersLostInsightsStream,
    InstagramDailyFollowersDemographicsStream,
    InstagramDailyFollowersInsightsStream,
    InstagramRelationshipsStream,
]
PINTEREST_STREAMS = [
    PinterestAccountStream,
    PinterestAccountStatsStream,
]
TWITTER_STREAMS = [
    TwitterAccountStream,
    TwitterMetricsStream,
]

STREAM_TYPES = FACEBOOK_STREAMS + INSTAGRAM_STREAMS + PINTEREST_STREAMS + TWITTER_STREAMS


class TapDashHudson(Tap):
    """DashHudson tap class."""
    name = "tap-dash-hudson"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "brand_id",
            th.NumberType,
            required=True,
            description="Brand IDs to query for"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="Start date to collect metrics from"
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            required=False,
            description="End date to collect metrics for"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
