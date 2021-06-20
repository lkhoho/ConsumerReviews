from sqlalchemy import Index, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, BigInteger, Float, String, DateTime

# base class for all database tables
DeclarativeBase = declarative_base()


class SteamUserProfile(DeclarativeBase):
    """
    User profiles on steamcommunity.com
    """

    __tablename__ = 'steam_userprofile'

    pid = Column(BigInteger, primary_key=True, autoincrement=True)

    # basic info
    user_id = Column(String(255))
    profile_id = Column(String(255))
    name = Column(String(255))
    location = Column(String(512))  # city, state, country

    # skill info
    level = Column(Integer)
    num_badges = Column(Integer)
    num_games = Column(Integer)
    num_screenshots = Column(Integer)
    num_workshop_items = Column(Integer)
    num_artworks = Column(Integer)
    num_joined_groups = Column(Integer)
    num_friends = Column(Integer)

    # achievement showcase
    num_achievements = Column(Integer)
    num_perfect_games = Column(Integer)
    avg_game_completion_rate = Column(Float)

    # workshop showcase
    num_submissions = Column(Integer)
    num_followers = Column(Integer)

    # badge collector
    num_badges_earned = Column(Integer)
    num_game_cards = Column(Integer)

    # game collector
    num_games_owned = Column(Integer)
    num_dlc_owned = Column(Integer)
    num_reviews = Column(Integer)
    num_wish_listed = Column(Integer)

    # items up for trade
    num_items_owned = Column(Integer)
    num_trades_made = Column(Integer)
    num_market_trx = Column(Integer)

    # recent activity
    num_hours_past_2_weeks = Column(Float)

    # comments
    num_comments_by_others = Column(Integer)

    created_datetime = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('uidx__steam_profile__pid', 'pid', unique=True),
    )

    def __repr__(self):
        return '<SteamProfile(pid={}, user_id={}, profile_id={}, name={})>'\
            .format(self.pid, self.user_id, self.profile_id, self.name)
