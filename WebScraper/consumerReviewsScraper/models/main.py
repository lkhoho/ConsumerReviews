from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from scrapy.utils.project import get_project_settings
from WebScraper.consumerReviewsScraper.models.bizrate import BizrateStore, BizrateReview
from WebScraper.consumerReviewsScraper.models.expedia import ExpediaHotel, ExpediaReview
from WebScraper.consumerReviewsScraper.settings import DB_CONN_STR


# DB_CONN_STR = '{driver}://{user}:{password}@{host}:{port}/{schema}?charset=utf8mb4'.format(
#     driver='mysql', user='root', password='welcome', host='kesnas.local', port=3306, schema='consumer_reviews')

# base class for all database tables
DeclarativeBase = declarative_base()


def db_connect():
    """
    Connect to database using settings from settings.py.

    :return: Sqlalchemy engine instance.
    """

    return create_engine(DB_CONN_STR)


def create_all_tables(engine):
    """
    Take metadata and create database tables.
    """

    DeclarativeBase.metadata.create_all(engine)


def create_bizrate_tables(engine):
    """
    Create Bizrate-related tables.
    """

    BizrateStore.metadata.create_all(engine)
    BizrateReview.metadata.create_all(engine)


def create_expedia_tables(engine):
    """
    Create Expedia-related tables.
    """

    ExpediaHotel.metadata.create_all(engine)
    ExpediaReview.metadata.create_all(engine)
