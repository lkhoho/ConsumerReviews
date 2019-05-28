import scrapy
from .base import ConsumerReview


class OrbitzReviewItem(ConsumerReview):
    """ Orbitz hotel review info. """

    def __init__(self):
        super().__init__()
        self["score"] = 0
        self["will_recommend"] = 0
        self["recommend_for"] = ""
        self["location"] = ""
        self["remark"] = {
            "pros": "",
            "cons": "",
            "location": ""
        }
        self["response"] = ConsumerReview()

    @staticmethod
    def get_column_headers() -> list:
        return super(OrbitzReviewItem, OrbitzReviewItem).get_column_headers() + [
            "score", "location", "will_recommend", "recommend_for", "remark_pros", "remark_cons", "remark_location",
            "response_title", "response_author", "response_date", "response_content"
        ]

    def get_column_values(self) -> list:
        return super().get_column_values() + [
            self["score"], self["location"], self["will_recommend"], self["recommend_for"], self["remark"]["pros"],
            self["remark"]["cons"], self["remark"]["location"], self["response"]["title"], self["response"]["author"],
            self["response"]["date"], self["response"]["content"]
        ]

    score = scrapy.Field()
    will_recommend = scrapy.Field()
    recommend_for = scrapy.Field()
    location = scrapy.Field()
    remark = scrapy.Field()
    response = scrapy.Field()
