import re
from datetime import datetime
from typing import Optional, Union
from scrapy.selector import SelectorList
from scrapy.http import Request
from scrapy.spiders import Spider
from scrapy.utils.request import request_fingerprint
from WebScraper.consumerReviewsScraper.items.steam import SteamUserProfileItem


class SteamSpider(Spider):
    name = 'steam_spider'

    custom_settings = {
        'LOG_FILE': '{}.log'.format(name),
        # 'LOG_LEVEL': 'DEBUG',

        # Auto-throttling
        # 'CONCURRENT_REQUESTS': 10,
        # 'DOWNLOAD_DELAY': 1,

        # Retry
        'CRAWLER_PAUSE_SECONDS': 60,
        'RETRY_HTTP_CODES': [429],

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'consumerReviewsScraper.middlewares.RandomUserAgentMiddleware': 400,

            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'consumerReviewsScraper.middlewares.TooManyRequestsRetryMiddleware': 543,
        },

        'SPIDER_MIDDLEWARES': {
            'consumerReviewsScraper.middlewares.UpdateURLStatusMiddleware': 10,
        },
    }

    start_urls = ['https://steamcommunity.com/id/rushingtotheend',
                  'https://steamcommunity.com/profiles/76561197969749884',
                  'https://steamcommunity.com/id/CheezusCrisp',
                  'https://steamcommunity.com/id/DashWithIt']

    def start_requests(self):
        for url in self.start_urls:
            req = Request(url=url, callback=self.parse, dont_filter=True)
            req.meta['fp'] = request_fingerprint(req)
            yield req

    def parse(self, response):
        self.logger.info('解析用户页 ' + response.url)

        # get friends link
        req = Request(url=response.url + '/friends', callback=self.parse_friends, dont_filter=True)
        req.meta['fp'] = request_fingerprint(req)
        yield req

        # basic info
        match = re.search(r'http.*/id/(.+)', response.url)
        if match is not None:
            user_id = match[1]
        else:
            user_id = None

        match = re.search(r'http.*/profiles/(\d+)', response.url)
        if match is not None:
            profile_id = match[1]
        else:
            profile_id = None
        name = self._extract_str(response.xpath("//span[@class='actual_persona_name']/text()"))
        location = response.xpath("//div[@class='header_real_name ellipsis']/text()").getall()[2].strip()

        # skill info
        level = self._extract_int(response.xpath("//div[@class='persona_name persona_level']//span[@class='friendPlayerLevelNum']/text()"))
        num_badges = self._extract_int(response.xpath("//div[@class='profile_badges']//span[@class='profile_count_link_total']/text()"))

        num_games = None
        num_screenshots = None
        num_workshop_items = None
        num_artworks = None
        item_links = response.xpath("//div[@class='profile_item_links']//a")
        for a in item_links:
            category = a.xpath("span[@class='count_link_label']/text()").get().strip()
            if category.lower() == 'games':
                num_games = self._extract_int(a.xpath("span[@class='profile_count_link_total']/text()"))
            if category.lower() == 'screenshots':
                num_screenshots = self._extract_int(a.xpath("span[@class='profile_count_link_total']/text()"))
            if category.lower() == 'workshop items':
                num_workshop_items = self._extract_int(a.xpath("span[@class='profile_count_link_total']/text()"))
            if category.lower() == 'artwork':
                num_artworks = self._extract_int(a.xpath("span[@class='profile_count_link_total']/text()"))

        num_joined_groups = self._extract_int(response.xpath("//div[@class='profile_group_links profile_count_link_preview_ctn responsive_groupfriends_element']//span[@class='profile_count_link_total']/text()"))
        num_friends = self._extract_int(response.xpath("//div[@class='profile_friend_links profile_count_link_preview_ctn responsive_groupfriends_element']//span[@class='profile_count_link_total']/text()"))

        # achievement showcase
        showcase = response.xpath("//div[@class='achievement_showcase']/div[@class='showcase_content_bg showcase_stats_row']")
        if len(showcase) > 0:
            num_achievements = self._extract_int(showcase.xpath("div[1]/div[@class='value']/text()"))
            num_perfect_games = self._extract_int(showcase.xpath("div[2]/div[@class='value']/text()"))
            avg_game_completion_rate = self._extract_float(showcase.xpath("div[3]/div[@class='value']/text()"))
        else:
            num_achievements = None
            num_perfect_games = None
            avg_game_completion_rate = None

        # workshop showcase
        showcase = response.xpath("//div[@class='myworkshop_showcase']/div[@class='showcase_stats_row showcase_content_bg']")
        if len(showcase) > 0:
            num_submissions = self._extract_int(showcase.xpath("a/div[@class='value']/text()"))
            num_followers = self._extract_int(showcase.xpath("div[1]/div[@class='value']/text()"))
        else:
            num_submissions = None
            num_followers = None

        # badge collector
        showcase = response.xpath("//div[@class='badge_showcase']/div[@class='showcase_content_bg showcase_stats_row']")
        if len(showcase) > 0:
            num_badges_earned = self._extract_int(showcase.xpath("a[1]/div[@class='value']/text()"))
            num_game_cards = self._extract_int(showcase.xpath("a[2]/div[@class='value']/text()"))
        else:
            num_badges_earned = None
            num_game_cards = None

        # game collector
        showcase = response.xpath("//div[@class='gamecollector_showcase']/div[@class='showcase_content_bg showcase_stats_row']")
        if len(showcase) > 0:
            num_games_owned = self._extract_int(showcase.xpath("a[1]/div[@class='value']/text()"))
            num_dlc_owned = self._extract_int(showcase.xpath("a[2]/div[@class='value']/text()"))
            num_reviews = self._extract_int(showcase.xpath("a[3]/div[@class='value']/text()"))
            num_wish_listed = self._extract_int(showcase.xpath("a[4]/div[@class='value']/text()"))
        else:
            num_games_owned = None
            num_dlc_owned = None
            num_reviews = None
            num_wish_listed = None

        # items for trade
        showcase = response.xpath("//div[@class='trade_showcase']//div[@class='showcase_stats_row showcase_stats_trading']")
        if len(showcase) > 0:
            num_items_owned = self._extract_int(showcase.xpath("a/div[@class='value']/text()"))
            num_trades_made = self._extract_int(showcase.xpath("div[1]/div[@class='value']/text()"))
            num_market_trx = self._extract_int(showcase.xpath("div[2]/div[@class='value']/text()"))
        else:
            num_items_owned = None
            num_trades_made = None
            num_market_trx = None

        # recent activity
        activity = response.xpath("//div[@class='recentgame_quicklinks recentgame_recentplaytime']")
        if len(activity) > 0:
            match = re.search(r'([\d.,]+) hours past 2 weeks', activity.xpath("h2/text()").get().strip())
            if match is not None:
                num_hours_past_2_weeks = self._extract_float(match[1])
            else:
                num_hours_past_2_weeks = None
        else:
            num_hours_past_2_weeks = None

        # comments
        comment = response.xpath("//div[@class='commentthread_header']/div[@class='commentthread_paging has_view_all_link']")
        if len(comment) > 0:
            num_comments_by_others = self._extract_int(comment.xpath("a[@class='commentthread_allcommentslink']/span/text()"))
        else:
            num_comments_by_others = None

        profile = SteamUserProfileItem(
            user_id=user_id,
            profile_id=profile_id,
            name=name,
            location=location,
            level=level,
            num_badges=num_badges,
            num_games=num_games,
            num_screenshots=num_screenshots,
            num_workshop_items=num_workshop_items,
            num_artworks=num_artworks,
            num_joined_groups=num_joined_groups,
            num_friends=num_friends,
            num_achievements=num_achievements,
            num_perfect_games=num_perfect_games,
            avg_game_completion_rate=avg_game_completion_rate,
            num_submissions=num_submissions,
            num_followers=num_followers,
            num_badges_earned=num_badges_earned,
            num_game_cards=num_game_cards,
            num_games_owned=num_games_owned,
            num_dlc_owned=num_dlc_owned,
            num_reviews=num_reviews,
            num_wish_listed=num_wish_listed,
            num_items_owned=num_items_owned,
            num_trades_made=num_trades_made,
            num_market_trx=num_market_trx,
            num_hours_past_2_weeks=num_hours_past_2_weeks,
            num_comments_by_others=num_comments_by_others,
            created_datetime=datetime.utcnow(),
        )
        yield profile

    def parse_friends(self, response):
        self.logger.info('解析朋友页 ' + response.url)
        


    def _extract_int(self, selector) -> Optional[int]:
        if selector is None:
            return None
        else:
            return int(selector.get().strip().replace(',', '').replace(',', ''))

    def _extract_str(self, selector) -> Optional[str]:
        if selector is None:
            return None
        else:
            return selector.get().strip()

    def _extract_float(self, selector: Union[SelectorList, str]) -> Optional[float]:
        if selector is None:
            return None
        else:
            if isinstance(selector, SelectorList):
                value = selector.get().strip()
            else:
                value = selector
            if '%' in value:
                return float(value.replace(',', '').replace('%', '')) / 100.0
            else:
                return float(value.replace(',', ''))
