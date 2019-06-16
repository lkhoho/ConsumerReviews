import re
import pytz
from datetime import datetime
from fake_useragent import UserAgent
from scrapy.http import Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items.xcar import *


class XcarForumSpider(CrawlSpider):
    """ Spider to scrape forums at xcar.com.cn """

    name = 'xcar_forum_spider'

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
        'http://www.xcar.com.cn/bbs/forumdisplay.php?fid=738&page=1',
        'http://www.xcar.com.cn/bbs/forumdisplay.php?fid=741&page=1'
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
            yield Request(url=url, callback=self.parse_forum)

    def response_is_ban(self, request, response):
        return b'Forbidden' in response.body

    def exception_is_ban(self, request, exception):
        return None

    def parse_forum(self, response):
        self.logger.info('解析论坛页 ' + response.url)
        # check mini icon-logo. If it is None, retry the request
        # if response.xpath("//i[@class='icon-logo']").extract_first() is None:
        #     yield Request(url=response.url, cookies=, dont_filter=True)
        forum = XcarForum()
        forum['fid'] = int(re.findall(r'http://www.xcar.com.cn/bbs/forumdisplay.php\?fid=(\d+).*', response.url)[0])
        forum['name'] = response.xpath("//div[@class='forum-tit']/h1/text()").get().strip()
        manager_urls = response.xpath("//ul[@class='userinfo']/li[1]/a/@href").getall()
        forum['managers'] = [int(re.findall(r'.*uid=(\d+).*', url)[0]) for url in manager_urls]
        forum['created_datetime'] = datetime.utcnow()
        yield forum

        for url in manager_urls:
            yield Request(url=url, callback=self.parse_user,
                          meta={
                              'manage': forum['fid']},
                          headers={
                              'User-Agent': self.user_agent.random,
                              'Host': 'my.xcar.com.cn',
                              'Referer': response.url},
                          cookies={
                              '_discuz_uid': 17502059,
                              'bbs_auth': 'VxvjtVV4%2BEBYZi9w75NEsF45437ZvB%2FrPnXfu5bfVSsNizKksOtXpobRKkWjalhECjU',
                              'bbs_cookietime': 31536000,
                              'bbs_visitedfid': '741D738D39',
                              '_fuv': 5603160404768,
                              '_fwck_www': '7b015af75e43be59766bb70bb0b9f7fe',
                              '_appuv_www': 'a9a3380ae18b10bfc7685cd81f612bf6',
                              '_fwck_my': '5ccd62d46ca3cf8378abc412a985f3b5',
                              '_appuv_my': '610527e88f1d625fe42138e789c893b9'})

        for thread in response.xpath("//dl[@class='list_dl']"):
            url = thread.xpath("dt/p[@class='thenomal']/a[@class='titlink']/@href").get()
            num_replies, num_views = tuple(thread.xpath("dd[@class='cli_dd']/span/text()").getall())
            yield Request(url='http://www.xcar.com.cn' + url, callback=self.parse_thread,
                          meta={
                              'num_replies': int(num_replies),
                              'num_views': int(num_views)},
                          headers={
                              'User-Agent': self.user_agent.random,
                              'Host': 'www.xcar.com.cn',
                              'Referer': response.url})

        # pagination
        next_page_url = response.xpath("//a[@class='page_down']/@href").get()
        if next_page_url is not None:
            yield Request(url='http://www.xcar.com.cn/bbs/' + next_page_url,
                          callback=self.parse_forum,
                          headers={
                              'User-Agent': self.user_agent.random,
                              'Host': 'www.xcar.com.cn',
                              'Referer': response.url,
                              'Connection': 'keep-alive'})

    def parse_user(self, response):
        self.logger.info('解析成员页 ' + response.url)
        user = XcarUser()
        user['coin'] = response.meta.get('coin', None)
        user['register_date'] = response.meta.get('register_date', None)
        user['manage'] = response.meta.get('manage', None)
        user['uid'] = int(re.findall(r'http://my.xcar.com.cn/space.php\?uid=(\d+).*', response.url)[0])
        user['name'] = response.xpath("//span[@class='name']/text()").get().strip()
        user['gender'] = None
        for title in response.xpath("//div[@class='icon_bed']/a/@title").getall():
            title = title.strip()
            if title != '男' and title != '女':
                continue
            user['gender'] = title
            break
        user['avatar_url'] = response.xpath("//img[@id='pic_user']/@src").get()
        loc_rank = list(filter(lambda s: len(s.strip()) > 0,
                               response.xpath("//div[@class='pf_address clearfix']/text()").getall()))
        if len(loc_rank) >= 3:
            user['location'] = ','.join([loc_rank[0].strip(), loc_rank[1].strip()])
            user['rank'] = loc_rank[-1].strip()
        elif len(loc_rank) >= 1:
            user['location'] = ''
            user['rank'] = loc_rank[-1].strip()
        else:
            user['location'] = ''
            user['rank'] = None
        numbers = response.xpath("//ul[@class='user_atten clearfix user_atten_m']/li/a/strong/text()").getall()
        user['num_follows'] = int(numbers[0].strip())
        user['num_fans'] = int(numbers[1].strip())
        user['num_posts'] = int(numbers[2].strip())
        user['created_datetime'] = datetime.utcnow()
        yield user

    def parse_thread(self, response):
        self.logger.info('解析帖子页 ' + response.url)
        thread = XcarThread()
        thread['tid'] = int(re.findall(r'http://www.xcar.com.cn/bbs/viewthread.php\?tid=(\d+)', response.url)[0])
        title = ''.join(response.xpath("//h2[@class='title']/text()").getall()).strip()
        thread['title'] = re.findall(r'[\s>]*(.*)', title)[0]
        thread['forum'] = int(response.xpath("//h2[@class='title']/a/@href").re_first(r'.*fid=(\d+)'))
        thread['is_elite'] = True \
            if response.xpath("//div[@class='maintop']/div[@class='seal']/span[@class='jh']") is not None else False
        thread['num_views'] = response.meta.get('num_views', 0)
        thread['num_replies'] = response.meta.get('num_replies', 0)
        thread['created_datetime'] = datetime.utcnow()
        yield thread

        for _post in response.xpath("//div[@class='main item']"):
            # process user
            user_info = _post.xpath("descendant::div[@class='userside']")
            user_url = user_info.xpath("div[@class='user_info']/p[@class='name']/a/@href").get()
            user_coin = self._translate_coin(user_info.xpath("descendant::li[@class='akb']/em/text()").get().strip())
            register_dt_str = user_info.xpath("descendant::p[@class='ursr_info']/span/text()").get()
            register_dt = datetime.strptime(register_dt_str.strip(), '%Y-%m-%d').date()
            yield Request(url=user_url, callback=self.parse_user,
                          meta={
                              'coin': user_coin,
                              'register_date': register_dt},
                          headers={
                              'User-Agent': self.user_agent.random,
                              'Host': 'my.xcar.com.cn',
                              'Referer': response.url},
                          cookies={
                              '_discuz_uid': 17502059,
                              'bbs_auth': 'VxvjtVV4%2BEBYZi9w75NEsF45437ZvB%2FrPnXfu5bfVSsNizKksOtXpobRKkWjalhECjU',
                              'bbs_cookietime': 31536000,
                              'bbs_visitedfid': '741D738D39',
                              '_fuv': 5603160404768,
                              '_fwck_www': '7b015af75e43be59766bb70bb0b9f7fe',
                              '_appuv_www': 'a9a3380ae18b10bfc7685cd81f612bf6',
                              '_fwck_my': '5ccd62d46ca3cf8378abc412a985f3b5',
                              '_appuv_my': '610527e88f1d625fe42138e789c893b9'})

            # process post
            post = XcarPost()
            post['pid'] = int(_post.xpath("a/@name").re_first(r'pid(\d+)'))
            self.logger.info('解析评论 pid={}, tid={}'.format(post['pid'], thread['tid']))
            publish_dt_str = _post.xpath("descendant::div[@class='mainboxTop']/p/text()")\
                .re_first(r'.*发表于\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s*')
            publish_dt = datetime.strptime(publish_dt_str.strip(), '%Y-%m-%d %H:%M')
            post['publish_datetime'] = pytz.timezone('Asia/Shanghai').localize(publish_dt).astimezone(pytz.utc)
            post['content'] = ' '.join(_post.xpath("descendant::div[@class='t_msgfont1']/descendant::*/text()")
                                       .getall()).strip()
            post['created_datetime'] = datetime.utcnow()
            post['is_flag'] = True if _post.xpath("descendant::span[@class='t_title1']") is not None else False
            post['author'] = int(re.findall(r'.*uid=(\d+)', user_url)[0])
            post['thread'] = thread['tid']
            yield post

        # pagination
        next_page_url = response.xpath("//a[@class='page_down']/@href").get()
        if next_page_url is not None:
            yield Request(url='http://www.xcar.com.cn/bbs/' + next_page_url,
                          callback=self.parse_thread,
                          meta={
                              'num_views': thread['num_views'],
                              'num_replies': thread['num_replies']
                          },
                          headers={
                              'User-Agent': self.user_agent.random,
                              'Host': 'www.xcar.com.cn',
                              'Referer': response.url,
                              'Connection': 'keep-alive'})

    def _translate_coin(self, coin: str) -> int:
        coin = coin.lower()
        if 'w' in coin:
            value = re.findall(r'([\d.]+)w.*', coin)[0]
            return int(float(value) * 10000)
        else:
            return int(coin)
