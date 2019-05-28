import scrapy
from .base import ConsumerReview


class EdmundsReviewItem(ConsumerReview):
    """ Edmunds review info. """

    def __init__(self):
        super().__init__()
        self["score"] = 0
        self["vehicle_type"] = ""
        self["helpful_count"] = "0/0"
        self["recommend_count"] = {"up": 0, "down": 0}
        self["best_features"] = []
        self["worst_features"] = []
        self["ratings"] = {
            "safety": 0,
            "performance": 0,
            "comfort": 0,
            "technology": 0,
            "interior": 0,
            "reliability": 0,
            "value": 0
        }

    @staticmethod
    def get_column_headers() -> list:
        return super(EdmundsReviewItem, EdmundsReviewItem).get_column_headers() + [
            "score", "vehicle_type", "helpful_count", "recommend_up_count", "recommend_down_count", "best_features",
            "worst_features", "ratings_safety", "ratings_performance", "ratings_comfort", "ratings_technology",
            "ratings_interior", "ratings_reliability", "ratings_value"
        ]

    def get_column_values(self) -> list:
        return super().get_column_values() + [
            self["score"], self["vehicle_type"], self["helpful_count"], self["recommend_count"]["up"],
            self["recommend_count"]["down"], self["best_features"], self["worst_features"], self["ratings"]["safety"],
            self["ratings"]["performance"], self["ratings"]["comfort"], self["ratings"]["technology"],
            self["ratings"]["interior"], self["ratings"]["reliability"], self["ratings"]["value"]
        ]

    score = scrapy.Field()
    vehicle_type = scrapy.Field()
    helpful_count = scrapy.Field()
    recommend_count = scrapy.Field()
    best_features = scrapy.Field()
    worst_features = scrapy.Field()
    ratings = scrapy.Field()
