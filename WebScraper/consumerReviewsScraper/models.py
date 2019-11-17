from sqlalchemy import create_engine, Table, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, BigInteger, Boolean, Float, String, Text, Date, DateTime
# from .settings import DB_CONN_STR


DB_CONN_STR = '{driver}:///{name}'.format(driver='sqlite', name='C:\\Users\\lkhoho\\OneDrive\\consumer_reviews.db')

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


class BizrateStore(DeclarativeBase):
    """
    Store on bizrate.com
    """

    __tablename__ = 'bizrate_store'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    description = Column(Text)
    website = Column(String)
    is_certified = Column(Boolean, default=False)
    award_year = Column(Integer)
    award_tier = Column(String)
    award_won_years = Column(Integer)
    created_datetime = Column(DateTime, nullable=False)
    
    # overall ratings
    overall_satisfaction = Column(Float)
    shop_again = Column(Float)
    to_recommend = Column(Float)
    
    # site and shopping experience ratings
    exp_ease_to_find = Column(Float)
    exp_site_design = Column(Float)
    exp_satisfaction_checkout = Column(Float)
    exp_product_selection = Column(Float)
    exp_clarity_product_info = Column(Float)
    exp_charges_clearly = Column(Float)
    exp_price_relative_other = Column(Float)
    exp_shipping_charges = Column(Float)
    exp_variety_shipping = Column(Float)
    
    # after purchase ratings
    after_deliver_ontime = Column(Float)
    after_order_tracking = Column(Float)
    after_product_met_expectations = Column(Float)
    after_customer_support = Column(Float)
    after_product_availability = Column(Float)
    after_returns_process = Column(Float)


    def __repr__(self):
        return '<BizrateStore(store_id={}, name={})>'.format(self.store_id, self.name)


class BizrateReview(DeclarativeBase):
    """
    Review of stores on bizrate.com
    """

    __tablename__ = 'bizrate_review'

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(BigInteger, nullable=False)
    store_id = Column(Integer, nullable=False)
    author = Column(String)
    before_purchase_publish_date = Column(Date)
    before_purchase_content = Column(Text)
    after_purchase_publish_date = Column(Date)
    after_purchase_content = Column(Text)
    overall_satisfaction = Column(Integer)
    shop_again = Column(Integer)
    to_recommend = Column(Integer)
    created_datetime = Column(DateTime, nullable=False)
    
    # site and shopping experience ratings
    exp_ease_to_find = Column(Integer)
    exp_site_design = Column(Integer)
    exp_satisfaction_checkout = Column(Integer)
    exp_product_selection = Column(Integer)
    exp_clarity_product_info = Column(Integer)
    exp_charges_clearly = Column(Integer)
    exp_price_relative_other = Column(Integer)
    exp_shipping_charges = Column(Integer)
    exp_variety_shipping = Column(Integer)
    
    # after purchase ratings
    after_deliver_ontime = Column(Integer)
    after_order_tracking = Column(Integer)
    after_product_met_expectations = Column(Integer)
    after_customer_support = Column(Integer)
    after_product_availability = Column(Integer)
    after_returns_process = Column(Integer)

    def __repr__(self):
        return '<BizrateReview(review_id={}, store_id={})>'.format(self.review_id, self.store_id)
