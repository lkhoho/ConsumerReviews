import re
import pytz
import json
from datetime import datetime, date, timedelta
from fake_useragent import UserAgent
from scrapy.http import Request
from scrapy.spiders import CrawlSpider
from ..items.dianping import *


class DianpingCommunitySpider(CrawlSpider):
    """
    只爬取大众点评网的社区部落、帖子、评论、会员、徽章、加分的Spider.
    """
    name = 'dp_community_spider'

    custom_settings = {
        'LOG_FILE': '{}.log'.format(name),

        # a list of proxies
        'ROTATING_PROXY_LIST': [
            '3.209.90.67:8080',
            '165.227.37.166:8080',
            '50.239.245.108:80',
            '46.218.155.194:3128',
            '35.236.207.97:8080',
            '128.199.169.60:80',
            '212.119.229.18:33852',
            '75.151.213.85:8080',
            '82.207.41.135:47395',
            '37.152.163.44:80',
            '41.223.155.118:53281',
            '185.189.199.75:23500',
            '77.232.153.248:60950',
            '190.85.136.34:48480',
            '103.206.168.177:53281',
            '213.33.155.80:44387',
            '217.182.51.228:8080',
            '193.242.178.50:52376',
            '89.186.1.215:53281',
            '1.20.100.8:47974',
            '94.180.108.102:54022',
            '118.175.93.182:39210',
            '103.116.37.28:44760',
            '89.102.2.149:8080',
            '35.245.136.201:3128',
            '117.6.161.118:47544',
            '91.205.218.33:80',
            '128.0.179.234:41258',
            '95.67.38.193:53281',
            '41.221.107.110:37557',
            '165.227.107.218:3128',
            '203.107.135.122:80',
            '203.107.135.125:80',
            '3.88.120.107:8080',
            '159.203.20.110:8080',
            '38.142.218.58:3128',
            '178.128.153.253:3128',
            '104.248.123.136:8080',
            '157.230.149.54:80',
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

        # cap of backoff (2 days)
        'ROTATING_PROXY_BACKOFF_CAP': 3600 * 48,

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'consumerReviewsScraper.middlewares.RandomUserAgentMiddleware': 400,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },

        'DATETIME_FORMAT': '%Y-%m-%d %H:%M:%S.%f',
        'START_CITY': '北京',
        'CITY_IDS': {'上海': 1, '北京': 2, '成都': 8, '重庆': 9}
    }

    allowed_domains = ['dianping.com']

    start_urls = [
        # 'http://s.dianping.com/bjmeishi?utm_source=forum_pc_index&pageno=1'
        'http://s.dianping.com/bjmeishi?pageno=1'
    ]

    # fake user agent
    user_agent = UserAgent()

    member_url_prefix = 'http://www.dianping.com/member/'

    # rules = (
    #     # Rule(LinkExtractor(allow=r'http://www.dianping.com/member/\d+.*'), callback='parse_member'),
    #     Rule(LinkExtractor(allow=r'http://s.dianping.com/topic/\d+\?utm_source=forum_pc_tribe'),
    #          callback='parse_topic'),
    #     Rule(LinkExtractor(allow=r'http://s.dianping.com/topic/\d+\?pageno=\d+'), callback='parse_review'),
    #     # Rule(LinkExtractor(allow=r'http://s.dianping.com/bjmeishi\?pageno=\d+'), callback='parse_community'),
    # )

    def parse_start_url(self, response):
        for url in self.start_urls:
            yield Request(url, cookies={'cy': self.settings['CITY_IDS'][self.settings['START_CITY']]},
                          callback=self.parse_community)

    def response_is_ban(self, request, response):
        return 'verify.meituan.com' in response.url or b'Forbidden' in response.body

    def exception_is_ban(self, request, exception):
        return None

    def parse_community(self, response):
        self.logger.info('解析部落页 ' + response.url)
        # check mini icon-logo. If it is None, retry the request
        # if response.xpath("//i[@class='icon-logo']").extract_first() is None:
        #     yield Request(url=response.url, cookies=, dont_filter=True)
        community = DPCommunity()
        city = response.xpath("//a[@class='city J-city']/span[2]/text()").extract_first()
        community['city'] = city.strip() if city is not None else None
        group_info = response.xpath("//div[@class='group-info']")
        name = group_info.xpath("div[@class='group-l1']/h1/text()").extract_first()
        community['name'] = name.strip() if name is not None else None
        created_datetime = group_info.xpath("div[@class='group-more-info']/span[1]/text()").re_first(r'[\d:\-\s]+')
        if created_datetime is not None:
            utc_dt = self._convert_timezone(datetime.strptime(created_datetime.strip(), '%Y-%m-%d %H:%M:%S'))
            community['created_datetime'] = utc_dt.strftime(self.settings.get('DATETIME_FORMAT'))
        else:
            community['created_datetime'] = None
        num_topics = group_info.xpath("div[@class='group-more-info']/span[2]/text()").re_first(r'[\d]+')
        community['num_topics'] = int(num_topics.strip()) if num_topics is not None else None
        num_members = group_info.xpath("div[@class='group-more-info']/span[3]/text()").re_first(r'[\d]+')
        community['num_members'] = int(num_members.strip()) if num_members is not None else None
        group_more_info = response.xpath("//div[@class='group-more-infos']")
        about = group_more_info.xpath("div[@class='group-intro']/text()").extract_first()
        community['about'] = about.strip() if about is not None else None
        manager_urls = group_more_info.xpath("div[@class='group-manager']/a/@href").extract()
        manager_urls = [s.strip() for s in manager_urls] if manager_urls is not None else None
        manager_id_pattern = re.compile(r'http://www.dianping.com/member/(\d+)\?.*')
        community['managers'] = [re.findall(manager_id_pattern, url)[0] for url in manager_urls]
        community['scraped_datetime'] = datetime.utcnow().strftime(self.settings.get('DATETIME_FORMAT'))
        yield community

        # managers
        for url in manager_urls:
            yield Request(url=url, cookies={'_lxsdk_cuid': '16a0e631ecec8-0f8406114d973-36697e04-140000-16a0e631ecec8',
                                            '_lxsdk': '16a0e631ecec8-0f8406114d973-36697e04-140000-16a0e631ecec8',
                                            '_hc.v': 'b504f640-0652-3473-b2a0-a09eb97af9c5.1555019538',
                                            'cy': self.settings['CITY_IDS'][self.settings['START_CITY']],
                                            '_lx_utm': 'utm_source%3Dforum_pc_tribe',
                                            '_lxsdk_s': '16a2c53249a-f3f-a9c-479%7C%7C29'},
                          callback=self.parse_member)

        # topic URLs
        topic_urls = response.xpath("//ul[@class='note-lists']/li/a/@href").extract()
        for url in topic_urls:
            yield Request(url=url, callback=self.parse_topic)

        # pagination
        page_list = [1]
        max_page = max(page_list + [int(page_str) for page_str in
                                    response.xpath("//ul[@class='page-list']/li[@class='item']/a/@href").re(r'\d+')])
        for i in range(1, max_page + 1):
            pos = response.url.find('?')
            if pos != -1:
                url = response.url[:pos] + '?pageno=%d' % i
            else:
                url = response.url + '?pageno=%d' % i
            yield Request(url=url, callback=self.parse_community)

    def parse_member(self, response):
        self.logger.info('解析成员页 ' + response.url)
        member = DPMember()
        id_pattern = re.compile(r'http://www.dianping.com/member/(\d+).*')
        member['id'] = int(re.findall(id_pattern, response.url)[0])
        pic_url_pattern = re.compile('(.*jpg|.*jpeg|.*png).*')
        pic_url = response.xpath("//div[@class='pic']/a/img/@src").extract_first()
        member['avatar_url'] = re.findall(pic_url_pattern, pic_url.strip())[0] if pic_url is not None else None
        name = response.xpath("//h2[@class='name']/text()").extract_first()
        member['name'] = name.strip() if name is not None else None
        is_vip = response.xpath("//div[@class='vip']/a/i/@class").extract_first()
        member['is_vip'] = 1 if is_vip is not None else 0
        gender = response.xpath("//div[@class='user-info col-exp']/span[2]/i/@class").extract_first()
        member['gender'] = gender.strip() if gender is not None else None
        location = response.xpath("//div[@class='user-info col-exp']/span[2]/text()").extract_first()
        member['location'] = location.strip() if location is not None else None
        tags = response.xpath("//div[@class='user-info col-exp']/span[3]/em/text()").extract()  # 用户标签
        member['tags'] = ','.join(tags) if tags is not None else None
        num_follows = response.xpath("//div[@class='user_atten']/ul/li[1]/a/strong/text()").extract_first()  # 关注数
        member['num_follows'] = int(num_follows.strip()) if num_follows is not None else None
        num_fans = response.xpath("//div[@class='user_atten']/ul/li[2]/a/strong/text()").extract_first()  # 粉丝数
        member['num_fans'] = int(num_fans.strip()) if num_fans is not None else None
        num_interactive = response.xpath("//div[@class='user_atten']/ul/li[3]/strong/text()").extract_first()  # 互动数
        member['num_interactive'] = int(num_interactive.strip()) if num_interactive is not None else None
        experience = response.xpath("//div[@class='user-time']/p[1]/span[2]/text()").extract_first()  # 贡献值
        member['experience'] = int(experience.strip()) if experience is not None else None
        rank = response.xpath("//div[@class='user-time']/p[2]/text()").extract_first()  # 社区等级
        member['rank'] = rank.strip() if rank is not None else None
        member['badges'] = []
        register_date = response.xpath("//div[@class='user-time']/p[3]/text()").extract_first()  # 注册时间
        if register_date is not None:
            arr = register_date.split('-')
            year, month, day = int(arr[0]), int(arr[1]), int(arr[2])
            member['register_date'] = date(year, month, day).strftime(self.settings.get('DATETIME_FORMAT'))
        else:
            member['register_date'] = None
        about = response.xpath("//div[@class='user-about']/text()").extract()  # 简介
        member['about'] = ''.join(about).strip() if about is not None else None
        user_msg_nodes = response.xpath("//div[@class='user-message Hide']/ul/li")
        member['body_type'] = None
        member['relationship'] = None
        member['birthday'] = None
        member['zodiac'] = None
        for node in user_msg_nodes:
            text = node.xpath("em/text()").extract_first()
            value = node.xpath("text()").extract_first()
            if '体型' in text:
                member['body_type'] = value.strip() if value is not None else None
            elif '恋爱状况' in text:
                member['relationship'] = value.strip() if value is not None else None
            elif '生日' in text:
                if value is not None:
                    if '-' not in value:
                        member['birthday'] = value
                    else:
                        arr = value.split('-')
                        year, month, day = int(arr[0]), int(arr[1]), int(arr[2])
                        member['birthday'] = date(year, month, day).strftime(self.settings.get('DATETIME_FORMAT'))
                else:
                    member['birthday'] = None
            elif '星座' in text:
                member['zodiac'] = value.strip() if value is not None else None
        member['scraped_datetime'] = datetime.utcnow().strftime(self.settings.get('DATETIME_FORMAT'))
        yield member

    def parse_topic(self, response):
        self.logger.info('解析帖子页 ' + response.url)
        topic = DPTopic()
        id_pattern = re.compile(r'http://s.dianping.com/topic/(\d+).*')
        topic['id'] = int(re.findall(id_pattern, response.url)[0])
        title = response.xpath("//h1[@class='note-title']/text()").extract_first()
        topic['title'] = title.strip() if title is not None else None
        author_id = response.xpath("//div[@class='user-r']/div[@class='user-info']/a/@data-uid").extract_first()
        topic['author_id'] = int(author_id.strip()) if author_id is not None else None
        yield Request(url=self.member_url_prefix + str(topic['author_id']),
                      cookies={'_lxsdk_cuid': '16a0e631ecec8-0f8406114d973-36697e04-140000-16a0e631ecec8',
                               '_lxsdk': '16a0e631ecec8-0f8406114d973-36697e04-140000-16a0e631ecec8',
                               '_hc.v': 'b504f640-0652-3473-b2a0-a09eb97af9c5.1555019538',
                               'cy': self.settings['CITY_IDS'][self.settings['START_CITY']],
                               '_lx_utm': 'utm_source%3Dforum_pc_tribe',
                               '_lxsdk_s': '16a2c53249a-f3f-a9c-479%7C%7C29'},
                      callback=self.parse_member)
        publish_datetime = response\
            .xpath("//div[@class='user-r']/div[@class='publish-time']/text()")\
            .re(r'[\d\-\s:]+')[0]
        if publish_datetime is not None:
            utc_dt = self._convert_timezone(datetime.strptime(publish_datetime.strip(), '%Y-%m-%d %H:%M:%S'))
            topic['publish_datetime'] = utc_dt.strftime(self.settings.get('DATETIME_FORMAT'))
        else:
            topic['publish_datetime'] = None
        num_views = response.xpath("//div[@class='user-r']/div[3]/span[@class='view-num']/text()").re(r'\d+')[0]
        topic['num_views'] = int(num_views.strip()) if num_views is not None else None
        num_likes = response.xpath("//div[@class='user-r']/div[3]//a[@class='ac-like ']/span/text()").extract_first()
        topic['num_likes'] = int(num_likes.strip()) if num_likes is not None else None
        num_replies = response.xpath("//div[@class='user-r']/div[3]//a[@class='ac-reply']/text()").re(r'\d+')[0]
        topic['num_replies'] = int(num_replies.strip()) if num_replies is not None else None
        content = response.xpath("//div[@class='note-area']/child::*[@class!='shop']/descendant::text()").extract()
        if content is not None:
            content = '\n'.join(content).strip()
            content = '\n'.join([s.strip() for s in re.split(r'[\n\xa0]', content) if len(s) > 1])
            topic['content'] = content
        else:
            topic['content'] = None
        edit_datetime = response.xpath("//div[@class='edit-info']/text()").re_first(r'[\d\-\s:]+')
        if edit_datetime is not None:
            utc_dt = self._convert_timezone(datetime.strptime(edit_datetime.strip(), '%Y-%m-%d %H:%M:%S'))
            topic['edit_datetime'] = utc_dt.strftime(self.settings.get('DATETIME_FORMAT'))
        else:
            topic['edit_datetime'] = None
        ops_url = 'http://s.dianping.com/ajax/json/group/note/oploglist?noteId=' + str(topic['id'])
        res = requests.get(ops_url, headers={
            'User-Agent': self.user_agent.random,
            'Host': 's.dianping.com',
            'Origin': 'http://s.dianping.com',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://s.dianping.com/topic/%d?utm_source=forum_pc_tribe' % topic['id']})
        res = json.loads(res.text)
        code = res['code']
        if code == 200:
            ops = res['msg']['data']
            op = ops[0] if len(ops) > 0 else None
            m = re.match(r'.*/member/(?P<admin_id>\d+).*于(?P<mark_dt>[\d\s\-:]+)设为精华帖', op)
            topic['mark_elite_by'] = m.group('admin_id')
            topic['mark_elite_datetime'] = self._convert_timezone(
                datetime.strptime(m.group('mark_dt').strip(), '%Y-%m-%d %H:%M:%S'))
        else:
            self.logger.warn('获取帖子{}精华请求失败！返回code是{}'.format(topic['id'], code))
            topic['mark_elite_by'] = None
            topic['mark_elite_datetime'] = None
        topic['scraped_datetime'] = datetime.utcnow().strftime(self.settings.get('DATETIME_FORMAT'))
        yield topic

        bonus = DPBonus()  # 帖子加分
        scores_url = 'http://s.dianping.com/ajax/json/group/note/scorelist?noteId=' + str(topic['id'])
        res = requests.get(scores_url, headers={
            'User-Agent': self.user_agent.random,
            'Host': 's.dianping.com',
            'Origin': 'http://s.dianping.com',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://s.dianping.com/topic/%d?utm_source=forum_pc_tribe' % topic['id']})
        res = json.loads(res.text)
        code = res['code']
        if code == 200:
            member_id_pattern = re.compile(r'/member/(\d+)')
            for score in res['msg']['data']:
                bonus['topic_id'] = topic['id']
                bonus['member_id'] = re.findall(member_id_pattern, score['userlink'])[0]
                bonus['points'] = int(score['score'])
                bonus['reason'] = score['comment'].strip() if score['comment'] is not None else None
                bonus['scraped_datetime'] = datetime.utcnow().strftime(self.settings.get('DATETIME_FORMAT'))
                yield bonus
        else:
            self.logger.warn('获取帖子{}加分请求失败！返回code是{}'.format(topic['id'], code))

        # pagination for reviews
        page_list = [1]
        max_page = max(page_list + [int(page_str) for page_str in
                                    response.xpath("//ul[@class='page-list']/li[@class='item']/a/text()").extract()])
        for i in range(1, max_page + 1):
            pos = response.url.find('?')
            if pos != -1:
                url = response.url[:pos] + '?pageno=%d' % i
            else:
                url = response.url + '?pageno=%d' % i
            yield Request(url=url, callback=self.parse_review)

    def parse_review(self, response):
        self.logger.info('解析评论页 ' + response.url)
        id_pattern = re.compile(r'http://s.dianping.com/topic/(\d+).*')
        topic_id = int(re.findall(id_pattern, response.url)[0])

        # parse review replies
        review_reply_url = 'http://s.dianping.com/ajax/lolNote'
        payload = {'followNoteIds': response.xpath("//ul[@class='reply-list']/li/@data-replyid").extract()}
        res = requests.post(review_reply_url, data=payload, headers={
            'User-Agent': self.user_agent.random,
            'Host': 's.dianping.com',
            'Origin': 'http://s.dianping.com',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://s.dianping.com/topic/%d?utm_source=forum_pc_tribe' % topic_id})
        res = json.loads(res.text)
        code = res['code']
        if code == 200:
            for review_id, replies in res['data'].items():
                for reply in replies:
                    review = DPReview()
                    review['publish_datetime'] = self\
                        ._convert_timezone(datetime.strptime(reply['addTimeStr'].strip(), '%Y-%m-%d %H:%M'))\
                        .strftime(self.settings.get('DATETIME_FORMAT'))
                    review['reply_to'] = int(review_id.strip())
                    review['topic_id'] = topic_id
                    review['num_likes'] = None
                    review['id'] = reply['lolNoteId']
                    review['author_id'] = reply['userId']
                    review['content'] = reply['noteBody'].strip()
                    review['scraped_datetime'] = datetime.utcnow().strftime(self.settings.get('DATETIME_FORMAT'))
                    yield review
        else:
            self.logger.warn('获取帖子{}回复请求失败！返回code是{}'.format(topic_id, code))

        # parse reviews
        replies = response.xpath("//ul[@class='reply-list']/li")
        for reply in replies:
            review = DPReview()
            review['topic_id'] = topic_id
            review_id = reply.xpath("@data-replyid").extract_first()
            review['id'] = int(review_id.strip()) if review_id is not None else None
            author_id = reply.xpath("div[@class='item-r']/div[@class='l1 user-info']/a/@data-uid").extract_first()
            review['author_id'] = int(author_id.strip()) if author_id is not None else None
            yield Request(url=self.member_url_prefix + str(review['author_id']),
                          cookies={'_lxsdk_cuid': '16a0e631ecec8-0f8406114d973-36697e04-140000-16a0e631ecec8',
                                   '_lxsdk': '16a0e631ecec8-0f8406114d973-36697e04-140000-16a0e631ecec8',
                                   '_hc.v': 'b504f640-0652-3473-b2a0-a09eb97af9c5.1555019538',
                                   'cy': self.settings['CITY_IDS'][self.settings['START_CITY']],
                                   '_lx_utm': 'utm_source%3Dforum_pc_tribe',
                                   '_lxsdk_s': '16a2c53249a-f3f-a9c-479%7C%7C29'},
                          callback=self.parse_member)
            reply_time = reply\
                .xpath("div[@class='item-r']/div[@class='l1 user-info']/span[@class='reply-time']/text()")\
                .extract_first()
            m = re.match(r'.*?(\d+-\d+-\d+\s*\d+:\d+:\d+)|.*?([今昨前天]+).*(\d+:\d+:\d+)', reply_time)
            if m is not None:
                reply_time = m.group(0)
                try:
                    if '今天' in reply_time or '昨天' in reply_time or '前天' in reply_time:
                        naive_date_str = m.group(2).strip()  # e.g. '今天'
                        naive_time_str = m.group(3).strip()  # e.g. '12:23:45'
                        if naive_date_str == '今天':
                            corrected_date = date.today()
                        elif naive_date_str == '昨天':
                            corrected_date = date.today() - timedelta(days=1)
                        else:
                            corrected_date = date.today() - timedelta(days=2)
                        reply_time = '%d-%d-%d %s' % (corrected_date.year, corrected_date.month, corrected_date.day,
                                                      naive_time_str)
                    else:
                        reply_time = m.group(1)
                    utc_dt = self._convert_timezone(datetime.strptime(reply_time.strip(), '%Y-%m-%d %H:%M:%S'))
                    review['publish_datetime'] = utc_dt.strftime(self.settings.get('DATETIME_FORMAT'))
                except ValueError:
                    self.logger.warn('爬取评论时间失败，使用默认值None. 获得的reply_time=' + reply_time)
                    review['publish_datetime'] = None
            else:
                review['publish_datetime'] = None
            content = reply.xpath("div[@class='item-r']/div[@class='l2']/div/text()").extract_first()
            review['content'] = content.strip() if content is not None else None
            num_likes = reply.xpath("div[@class='item-r']/div[@class='l3']/a[2]/text()").extract_first()
            review['num_likes'] = int(num_likes) if num_likes is not None else None
            review['reply_to'] = None
            review['scraped_datetime'] = datetime.utcnow().strftime(self.settings.get('DATETIME_FORMAT'))
            yield review

    def _convert_timezone(self, naive_dt: datetime, from_tz=pytz.timezone('Asia/Shanghai'), to_tz=pytz.utc) -> datetime:
        from_dt = from_tz.localize(naive_dt)
        return from_dt.astimezone(to_tz)
