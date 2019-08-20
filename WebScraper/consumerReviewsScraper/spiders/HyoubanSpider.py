import re
import math
import pytz
from datetime import datetime
from fake_useragent import UserAgent
from scrapy.http import Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items.hyouban import HyoubanReview


class HyoubanReviewSpider(CrawlSpider):
    """ Spider to scrape reviews at hyouban.com """

    name = 'hyouban_review_spider'

    num_reviews_per_page = 10

    custom_settings = {
        'LOG_FILE': '{}.log'.format(name),

        # a list of proxies
        'ROTATING_PROXY_LIST': [
            '168.181.121.195:38494',
            '80.90.88.147:49786',
            '1.20.97.122:44279',
            '192.99.207.23:40067',
            '3188.168.75.254:56899',
            '87.103.196.39:57064',
            '206.189.159.68:3128',
            '27.111.18.210:56820',
            '41.190.95.20:55405',
            '103.254.126.182:34324',
            '202.57.37.197:46107',
            '46.160.237.93:51711',
            '157.230.149.189:80',
            '46.63.104.139:8080',
            '186.42.163.234:44252',
            '124.41.211.132:61461',
            '95.66.142.18:60719',
            '138.219.71.74:52688',
            '110.74.196.235:46140',
            '181.211.38.58:53281',
            '138.219.228.21:8080',
            '134.209.106.81:8080',
            '165.22.247.226:8080',
            '185.105.168.37:8080',
            '81.200.14.150:40413',
            '93.171.27.99:53651',
            '35.181.47.163:3128',
            '81.95.1.92:8181',
            '178.128.222.81:3128',
            '51.141.83.47:3128',
            '162.243.160.213:8080',
            '45.7.230.201:8080',
            '170.239.84.239:3128',
            '178.128.126.150:3128',
            '167.99.94.1:3128',
            '167.99.175.21:3128',
            '104.248.76.171:8080',
            '185.183.185.175:8080',
            '167.99.67.9:3128',
            '157.230.149.189:80',
            '198.199.122.218:8080',
            '99.79.100.105:8080',
            '138.197.67.160:3128',
            '31.14.133.86:8080',
            '167.99.165.114:3128',
            '67.158.54.103:8080',
            '206.81.11.75:80',
            '91.211.245.12:80',
            '68.183.39.251:8080',
            '157.230.140.12:8080',
            '3.19.17.101:8080',
            '203.107.135.127:80',
            '167.99.148.235:3128',
            '77.95.96.160:80',
            '159.203.169.239:3128',
            '203.107.135.123:80',
            '142.93.202.126:8080',
            '134.209.231.163:8080',
            '157.230.57.151:8080',
            '5.202.148.24:80',
            '34.73.124.56:80',
            '185.13.37.46:80',
            '167.114.34.23:8080',
            '165.22.159.107:8080',
            '134.209.161.229:3128',
            '23.248.162.48:8080',
            '35.183.224.95:8080',
            '47.74.8.220:3128',
            '157.230.227.116:8080',
            '123.176.96.49:3128',
            '50.239.245.110:80',
            '134.119.188.154:8080',
            '219.92.37.174:80',
            '118.174.232.239:39258',
            '139.255.64.10:36354',
            '118.174.232.54:31793',
            '96.9.69.133:50693',
            '119.93.234.41:41731',
            '196.37.143.202:8080',
            '94.253.12.163:40930',
        ],

        # cap of backoff (1 day)
        'ROTATING_PROXY_BACKOFF_CAP': 3600 * 24,

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'consumerReviewsScraper.middlewares.RandomUserAgentMiddleware': 400,
            # 'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            # 'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
    }

    start_urls = [
        'https://en-hyouban.com/company/10013092883/kuchikomi'
    ]

    # fake user agent
    user_agent = UserAgent()

    # rules = (
    #     Rule(LinkExtractor(allow=r'http://my.xcar.com.cn/space.php\?uid=\d+'), callback='parse_user'),
    #     Rule(LinkExtractor(allow=r'forumdisplay.php\?fid=\d+&page=\d+',
    #                        process_value=lambda v: 'http://www.xcar.com.cn/bbs/' + v), callback='parse_forum'),
    # )

    def parse_start_url(self, response):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_company)


    def response_is_ban(self, request, response):
        return b'Forbidden' in response.body


    def exception_is_ban(self, request, exception):
        return None


    def parse_company(self, response):
        self.logger.info('解析公司页 ' + response.url)
        # check mini icon-logo. If it is None, retry the request
        # if response.xpath("//i[@class='icon-logo']").extract_first() is None:
        #     yield Request(url=response.url, cookies=, dont_filter=True)
        company_id = int(re.findall(r'https://en-hyouban.com/company/(\d+)/kuchikomi*', response.url)[0])
        company_name = response.xpath("//div[@class='topArea']/h1/span[@id='companyName']/text()").get()
        num_reviews = int(response.xpath("//div[@class='num ']/span[1]/text()").get())
        num_pages = math.ceil(num_reviews / self.num_reviews_per_page)
        self.logger.info('Total reviews: {}; Per page: {}; Pages: {}.'.format(
            num_reviews, self.num_reviews_per_page, num_pages))
        
        categories = {}
        for cat in response.xpath("//div[@class='categoryArea']//li[contains(@class, 'tabBase--kuchikomi')]"):
            cat_id = int(cat.attrib['class'].strip()[-2:])
            cat_name = cat.xpath("a/text()").get().strip()
            categories[cat_id] = cat_name
        self.logger.info('Categories: {}'.format(categories))

        for i in range(1, num_pages + 1):
            url = response.url + '?pagenum=' + str(i)
            yield Request(url=url, callback=self.parse_reviews, 
                          meta={'company_id': company_id, 'company_name': company_name, 'categories': categories},
                          headers={'User-Agent': self.user_agent.random},
                          cookies={
                              'AWSELB': 'C7C1B5D5128129DD7F40AF3751D975041B8F85700FCE0FEBDDF8E443D19E2D38589DF45D5E46504537D861FB5B4C47ADE763D1ED1371B88994973B84C656B57A3DEB87DB7F',
                              'IDE': 'AHWqTUk32_5O2QujNkwzrzeyju96i0_PkYmF6I2cUtcb5GyeiG-uxzEn3P3qPJtO',
                              'IID': '9ffc16e4151d401aaa3c02162a77bfdd',
                              'PHPSESSID': '0au9019mau7fh777bqu93klrq7',
                              'TAGKNIGHT_CONTROL_CLUSTER': 123,
                              'U': '514ce3496003c4766c0b4dba87b2260a',
                              '__cfduid': 'dc0b87f6c438e84e94a904b222ca3d6761566287077',
                              '__hd_ss': 1566287075737,
                              '_kyp': 'QEkQBZnKWX5L0qGDeBbxslObdUzo2YJb2urzgwGoGnKW2Vv4MMpglva6vODYbQQt6lLCaaboScIS_.en-hyouban.com',
                              '_kys': 'QEkRF4/66fiJ4A_.en-hyouban.com',
                              '_kzuid': 'd65e71aae22940fae06315209187e4b5',
                              'c': 'a0748fb76f.207',
                              'csrf_cookie_name': 'f24796d57ff0e15e7f3fd135a4746e9e',
                              'cto_lwid': '386fbe82-a9c3-419f-bec6-e8beb86793c6',
                              'uid': '86c02ecb-b666-44dc-966a-22aac61673c1',
                          })


    def parse_reviews(self, response):
        self.logger.info('解析评论页 ' + response.url)

        for div in response.xpath("//div[contains(@class, 'kuchikomi--')]"):
            div = div.xpath("div[@class='box']")
            subject_div = div.xpath("div[@class='subject']")
            comment_div = div.xpath("div[@class='comment']")
            
            review = HyoubanReview()
            review['attitude'] = subject_div.xpath("div[@class='label']/text()").get()
            review['company_id'] = response.meta['company_id']
            review['company'] = response.meta['company_name']
            review['review_id'] = int(re.findall(r'/company/\d+/kuchikomi/(\d+).*', 
                                      comment_div.xpath("div[1]/a[1]/@href").get())[0])
            category_id = int(re.findall(r'title title--(\d+).*', comment_div.xpath("div[1]/@class").get())[0])
            review['category'] = response.meta['categories'][category_id]
            review['content'] = ''.join(comment_div.xpath("text()").getall()).strip()
            review['status'] = comment_div.xpath("div[@class='status']/a/text()").get()
            publish_date_str_match = re.findall(r'口コミ投稿日：(.*).*', 
                                                ''.join(comment_div.xpath("div[@class='status']/text()").getall()).strip())
            publish_date_str = publish_date_str_match[0] if len(publish_date_str_match) > 0 else 'n/a'
            if publish_date_str != 'n/a':
                publish_date = datetime.strptime(publish_date_str.strip(), '%Y年%m月%d日')
            else:
                publish_date = ''
            # review['publish_date'] = pytz.timezone('Asia/Shanghai').localize(publish_date).astimezone(pytz.utc)
            review['publish_date'] = publish_date
            review['num_helpful'] = int(comment_div.xpath("span/span/span/text()").get())
            review['created_datetime'] = datetime.utcnow()
            yield review
