import scrapy


class SteamUserProfileItem(scrapy.Item):
    """ User profile on steamcommunity.com """

    # basic info
    user_id = scrapy.Field()
    profile_id = scrapy.Field()
    name = scrapy.Field()
    location = scrapy.Field()

    # skill info
    level = scrapy.Field()
    num_badges = scrapy.Field()
    num_games = scrapy.Field()
    num_screenshots = scrapy.Field()
    num_workshop_items = scrapy.Field()
    num_artworks = scrapy.Field()
    num_joined_groups = scrapy.Field()
    num_friends = scrapy.Field()

    # achievement showcase
    num_achievements = scrapy.Field()
    num_perfect_games = scrapy.Field()
    avg_game_completion_rate = scrapy.Field()

    # workshop showcase
    num_submissions = scrapy.Field()
    num_followers = scrapy.Field()

    # badge collector
    num_badges_earned = scrapy.Field()
    num_game_cards = scrapy.Field()

    # game collector
    num_games_owned = scrapy.Field()
    num_dlc_owned = scrapy.Field()
    num_reviews = scrapy.Field()
    num_wish_listed = scrapy.Field()

    # items up for trade
    num_items_owned = scrapy.Field()
    num_trades_made = scrapy.Field()
    num_market_trx = scrapy.Field()

    # recent activity
    num_hours_past_2_weeks = scrapy.Field()

    # comments
    num_comments_by_others = scrapy.Field()

    created_datetime = scrapy.Field()
