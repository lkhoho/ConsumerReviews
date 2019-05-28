import scrapy
from ..items.tripadvisor import TripAdvisorReviewItem


class TripAdvisorReviewSpider(scrapy.Spider):
    name = "tripadvisor_portillos"

    def start_requests(self):
        urls = [
            "https://www.tripadvisor.com/Restaurant_Review-g35805-d1171368-Reviews-Portillo_s-Chicago_Illinois.html#REVIEWS"
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        selectors = response.xpath("//div[starts-with(@id, 'review_')]")

        for selector in selectors:
            review = TripAdvisorReviewItem()
            l_col = selector.xpath("div/div[@class='col1of2']")  # left column
            r_col = selector.xpath("div/div[@class='col2of2']")  # right column
            review["score"] = int(r_col.xpath("div/div/div[@class='rating reviewItemInline']/span/img/@alt").extract_first()[0])
            review["date"] = r_col.xpath("div/div/div[@class='rating reviewItemInline']/span/@title").extract_first()
            if len(r_col.xpath("div/div/div[@class='rating reviewItemInline']/a[@class='viaMobile']")) > 0:
                review["via_mobile"] = 1
            review["title"] = r_col.xpath("div/div/div[contains(@class, 'quote')]/a/span/text()").extract_first()
            review["content"] = r_col.xpath("div/div/div[@class='entry']/p/text()").extract_first().strip()
            val = r_col.xpath("div/div/div[@class='wrap']/div[contains(@class, 'helpful')]/div/span[@class='numHlp']/span/text()").extract_first()
            review["helpful_count"] = int(val) if val is not None else 0

            member_info = l_col.xpath("div[1]")
            member_badging = l_col.xpath("div[2]")
            review["author"] = member_info.xpath("div[@class='memberOverlayLink']/div[@class='username mo']/span/text()").extract_first().strip()
            review["location"] = member_info.xpath("div[@class='location']/text()").extract_first().strip()

            val = member_badging.xpath("div/div[contains(@class, 'levelBadge')]")
            if len(val) > 0:
                review["level"] = int(val.xpath("@class").re(r"lvl_(\d+)")[0])

            val = member_badging.xpath("div/div[contains(@class, 'reviewerBadge')]/span")
            if len(val) > 0:
                review["review_count"] = int(val.xpath("text()").re(r"(\d+)\s*reviews")[0])

            val = member_badging.xpath("div/div[contains(@class, 'contributionReviewBadge')]")
            if len(val) > 0:
                review["contribution_count"] = int(val.xpath("span/text()").re(r"(\d+)\s*restaurant reviews")[0])

            val = member_badging.xpath("div/span[@class='badgeText']")
            if len(val) > 0:
                review["helpful_votes"] = int(val.xpath("text()").re(r"(\d+)\s*helpful")[0])
            yield review

        next_urls = response.xpath("//div[@class='unified pagination ']/descendant::a/@href").extract()
        for url in next_urls:
            yield scrapy.Request(url="https://www.tripadvisor.com" + url.strip(), callback=self.parse)
