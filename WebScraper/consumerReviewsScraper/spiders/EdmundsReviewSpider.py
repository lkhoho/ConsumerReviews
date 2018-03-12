import re
from urllib import request
from html.parser import HTMLParser
import scrapy
from ..items import EdmundsReviewItem


class EdmundsReviewHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.result = []

    def handle_data(self, data):
        data = re.sub(r"[()\[\]<>,.]", "", data.strip())
        if len(data) > 0:
            self.result.append(data.strip())


class EdmundsReviewSpider(scrapy.Spider):
    name = "edmunds_honda"
    base_url = "https://www.edmunds.com/honda/accord/2017/consumer-reviews"
    num_pages = 62
    reviews = {}

    def start_requests(self):
        urls = [self.base_url + "/pg-" + str(i) for i in range(1, self.num_pages + 1)]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        selectors = response.xpath("//div[@class='individual-review-container']")

        for selector in selectors:
            review = EdmundsReviewItem()
            review['score'] = float(selector.xpath("div[@class='individual-overal-rating']/div/span/@title").extract_first())
            review['title'] = selector.xpath("div[@class='individual-overal-rating']/span/strong/text()").extract_first()
            review_id = selector.xpath("div[@class='hide-comment']/@id").extract_first()

            helpful_text = list(filter(lambda s: len(s) != 0, selector.xpath("div[2]/text()").re(r"\d+\s*of\s*\d+")))
            for text in helpful_text:
                m = re.match(r"(\d+)\s*of\s*(\d+)", text)
                review['helpful_count'] = m.group(1) + "/" + m.group(2)

            # ratings = {}
            # ratings_selectors = selector.xpath("div[@class='individual-review-stars-container']/div[@class='individual-ratings-left']")
            # for rating_selector in ratings_selectors:
            #     spans = rating_selector.xpath("span[@class!='no-display']")
            #     for span in spans:
            #         name = span.xpath("./text()").extract_first().strip().replace("\"", "").lower()
            #         value = int(span.xpath("span/@title").extract_first())
            #         ratings["ratings_" + name] = value
            # for name in EdmundsReviewItem.rating_fields():
            #     if name not in ratings:
            #         ratings[name] = 0
            # for k, v in ratings.items():
            #     review[k] = v

            review['author'] = selector.xpath("div[@class='author-heading']/div[1]/strong/text()").extract_first().strip()
            review['date'] = selector.xpath("div[@class='author-heading']/div[2]/time/text()").extract_first().strip()
            review['vehicle_type'] = selector.xpath("div[@style='float:left']/p/text()").extract_first().strip()
            content = selector.xpath("div[@style='float:left']/following-sibling::div[1]/p/text()").extract_first()
            if content is not None:
                review['content'] = content.strip()

            url = self.base_url + "/review-" + review_id
            detail_str = request.urlopen(url).read().decode("utf-8")

            # get best features
            best_parser = EdmundsReviewHTMLParser()
            start = detail_str.find("<h4>Best Features</h4>")
            start = detail_str.find("<ul class=\"crr-features-list\"", start)
            end = detail_str.find("</ul>", start) + len("</ul>")
            best_parser.feed(detail_str[start:end])
            review["best_features"] = best_parser.result

            # get worst features
            worst_parser = EdmundsReviewHTMLParser()
            start = detail_str.find("<h4>Worst Features</h4>", start)
            start = detail_str.find("<ul class=\"crr-features-list\"", start)
            end = detail_str.find("</ul>", start) + len("</ul>")
            worst_parser.feed(detail_str[start:end])
            review["worst_features"] = worst_parser.result

            # get recommend up count
            recommend_up_parser = EdmundsReviewHTMLParser()
            start = detail_str.find("<span class=\"link\" id=\"crr_rate_up\">", start)
            end = detail_str.find("<span class=\"link\" id=\"crr_rate_down\">", start)
            recommend_up_parser.feed(detail_str[start:end])
            review["recommend_count"]["up"] = int(recommend_up_parser.result[0]) if len(recommend_up_parser.result) > 0 else 0

            # get recommend down count
            recommend_down_parser = EdmundsReviewHTMLParser()
            start = detail_str.find("<span class=\"link\" id=\"crr_rate_down\">", start)
            end = detail_str.find("</div>", start)
            recommend_down_parser.feed(detail_str[start:end])
            review["recommend_count"]["down"] = int(recommend_down_parser.result[0]) if len(recommend_down_parser.result) > 0 else 0
            yield review
