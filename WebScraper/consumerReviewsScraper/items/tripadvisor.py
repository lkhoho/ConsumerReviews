import scrapy
from .base import ConsumerReview


class TripAdvisorReviewItem(ConsumerReview):
    """ TripAdvisor restaurant review info. """

    def __init__(self):
        super().__init__()
        self["score"] = 0
        self["via_mobile"] = 0
        self["helpful_count"] = 0
        self["location"] = ""
        self["level"] = 0
        self["review_count"] = 0
        self["contribution_count"] = 0
        self["helpful_votes"] = 0

    @staticmethod
    def get_column_headers() -> list:
        return super(TripAdvisorReviewItem, TripAdvisorReviewItem).get_column_headers() + [
            "score", "helpful_count", "location", "level", "review_count", "contribution_count", "helpful_votes"
        ]

    def get_column_values(self) -> list:
        return super().get_column_values() + [
            self["score"], self["helpful_count"], self["location"], self["level"], self["review_count"],
            self["contribution_count"], self["helpful_votes"]
        ]

    score = scrapy.Field()
    via_mobile = scrapy.Field()
    helpful_count = scrapy.Field()
    location = scrapy.Field()
    level = scrapy.Field()
    review_count = scrapy.Field()
    contribution_count = scrapy.Field()
    helpful_votes = scrapy.Field()
