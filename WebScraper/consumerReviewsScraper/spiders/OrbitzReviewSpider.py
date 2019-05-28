import re
import scrapy
from ..items.orbitz import OrbitzReviewItem


class OrbitzReviewSpider(scrapy.Spider):
    name = "orbitz_stratosphere"
    url_format = "https://www.orbitz.com/Las-Vegas-Hotels-Stratosphere-Hotel-Casino-Resort-Hotel.h41081-p{}.Hotel-Reviews?rm1=a2&"
    num_reviews_per_page = 10
    num_reviews = 5403
    num_pages = (num_reviews + num_reviews_per_page - 1) // 10
    is_pos_neg_separated = True

    def start_requests(self):
        urls = []
        for i in range(1, self.num_pages + 1):
            urls.append(self.url_format.format(str(i)))

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        selectors = response.xpath("//section[@id='reviews']/article")

        for selector in selectors:
            review = OrbitzReviewItem()
            review["score"] = int(selector.xpath("div[@class='summary']/span/span/text()").extract_first())
            val = selector.xpath("div[@class='summary']/blockquote/text()").re(r"for .+")
            if len(val) > 0:
                review["recommend_for"] = val[0].replace("for ", "")
                review["will_recommend"] = 1
            val = selector.xpath("div[@class='summary']/blockquote/div/text()").extract_first().strip().replace("\xa0", " ")
            m = re.match(r"by\s*([\w\s]+) from\s*([\w\s,]+)", val)
            if m is not None:
                review["author"] = m.group(1)
                review["location"] = m.group(2)
            else:
                review["author"] = re.match(r"by\s*([\w\s]+)", val).group(1)
            val = selector.xpath("div[@class='details']/h3/text()").extract_first()
            if val is not None:
                review["title"] = val.replace("\r\n", "").replace("\"", "'")
            review["date"] = selector.xpath("div[@class='details']/span/text()").extract_first().strip().replace("Posted ", "")
            val = selector.xpath("div[@class='details']/div[@class='review-text']/text()").extract_first()
            if val is not None:
                review["content"] = val.strip().replace("\r\n", "")
            remark_selectors = selector.xpath("div[@class='details']/div[@class='remark']")
            for remark_selector in remark_selectors:
                k = remark_selector.xpath("strong/text()").extract_first().lower().replace(":", "")
                v = "".join(remark_selector.xpath("text()").extract()).strip().replace("\r\n", " ")
                review["remark"][k] = v
            response_selector = selector.xpath("div[@class='details']/div[@class='management-response']")
            if len(response_selector) > 0:
                response_selector = response_selector[0]
                review["response"]["title"] = response_selector.xpath("div[@class='title']/text()").extract_first().strip()
                review["response"]["content"] = response_selector.xpath("div[@class='text']/text()").extract_first().strip()
                val = response_selector.xpath("div[@class='date-posted']/text()").extract_first().strip()
                m = re.match(r"(.+)\sby\s*(.+)", val)
                if m is not None:
                    review["response"]["date"] = m.group(1)
                    review["response"]["author"] = m.group(2)
                else:
                    review["response"]["author"] = re.match(r"by\s*(.+)", val).group(1).strip()
            yield review
