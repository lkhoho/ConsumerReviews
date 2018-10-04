import datetime
from pony.orm import *
from utils.cipher import CipherUtils
from definitions import get_full_path
from configs import DatabaseConfig


default_db_config_file = get_full_path('dev', 'conf', 'local_dev.yaml')
db_config = DatabaseConfig.from_file(default_db_config_file)
db = Database()


class BizrateReview(db.Entity):
    _table_ = (db_config.schema, 'BIZRATE_REVIEW')

    pid = PrimaryKey(int, column='PID', size=64, auto=True)
    create_time = Required(datetime.datetime, column='CREATE_TIME')
    source = Required(str, column='SOURCE')
    store = Required(str, column='STORE', index='IX__BIZRATE_REVIEW_STORE')
    shop_again = Optional(int, column='SHOP_AGAIN')
    to_recommend = Optional(int, column='TO_RECOMMEND')
    satisfaction = Optional(int, column='SATISFACTION')
    # pre-purchase
    before_author = Optional(str, column='BEFORE_AUTHOR')
    before_date = Optional(datetime.date, column='BEFORE_DATE')
    before_review = Optional(str, column='BEFORE_REVIEW')
    # after-purchase
    after_author = Optional(str, column='AFTER_AUTHOR')
    after_date = Optional(datetime.date, column='AFTER_DATE')
    after_review = Optional(str, column='AFTER_REVIEW')
    # relationships
    site_experience_ratings = Optional('BizrateSiteExperienceRatings')
    after_purchase_ratings = Optional('BizrateAfterPurchaseRatings')

    def get_overall_scores(self) -> dict:
        return {
            'pid': self.pid,
            'show_again': self.shop_again,
            'to_recommend': self.to_recommend,
            'satisfaction': self.satisfaction
        }

    def get_pre_purchase_review(self) -> dict:
        return {
            'pid': self.pid,
            'author': self.before_author,
            'date': self.before_date,
            'review': self.before_review
        }

    def get_after_purchase_review(self) -> dict:
        return {
            'pid': self.pid,
            'author': self.after_author,
            'date': self.after_date,
            'review': self.after_review
        }


class BizrateAfterPurchaseRatings(db.Entity):
    _table_ = (db_config.schema, 'BIZRATE_AFTER_PURCHASE_RATINGS')

    pid = PrimaryKey(int, column='PID', size=64, auto=True)
    create_time = Required(datetime.datetime, column='CREATE_TIME')
    # review_pid = Column('', BIGINT, ForeignKey('BIZRATE_REVIEW.PID', name='FK__AFTER_PURCHASE__REVIEW',
    #                                                         onupdate='RESTRICT', ondelete='CASCADE'))
    product_availability = Optional(int, column='PRODUCT_AVAILABILITY')
    ontime_delivery = Optional(int, column='ONTIME_DELIVERY')
    product_met_expectations = Optional(int, column='PRODUCT_MET_EXPECTATIONS')
    order_tracking = Optional(int, column='ORDER_TRACKING')
    return_process = Optional(int, column='RETURN_PROCESS')
    customer_support = Optional(int, column='CUSTOMER_SUPPORT')
    # relationship
    review = Required(BizrateReview, column='FK_REVIEW_PID', fk_name='FK__AFTER_PURCHASE__REVIEW', cascade_delete=True)

    def get_ratings(self) -> dict:
        return {
            'product_availability': self.product_availability,
            'ontime_delivery': self.ontime_delivery,
            'product_met_expectations': self.product_met_expectations,
            'order_tracking': self.order_tracking,
            'return_process': self.return_process,
            'customer_support': self.customer_support
        }


class BizrateSiteExperienceRatings(db.Entity):
    _table_ = (db_config.schema, 'BIZRATE_SITE_EXPERIENCE_RATINGS')

    pid = PrimaryKey(int, size=64, auto=True, column='PID')
    create_time = Required(datetime.datetime, column='CREATE_TIME')
    # review_pid = Column('FK_REVIEW_PID', BIGINT, ForeignKey('BIZRATE_REVIEW.PID', name='FK__SITE_EXPERIENCE__REVIEW',
    #                                                         onupdate='RESTRICT', ondelete='CASCADE'))
    shipping_options = Optional(int, column='SHIPPING_OPTIONS')
    shipping_charges = Optional(int, column='SHIPPING_CHARGES')
    ease_of_finding = Optional(int, column='EASE_OF_FINDING')
    price_relative_to_other_retailers = Optional(int, column='PRICE_RELATIVE_TO_OTHER')
    clarity_product_info = Optional(int, column='CLARITY_PRODUCT_INFO')
    charges_stated_clearly = Optional(int, column='CHARGES_STATED_CLEARLY')
    site_design = Optional(int, column='SITE_DESIGN')
    product_selection = Optional(int, column='PRODUCT_SELECTION')
    checkout_satisfaction = Optional(int, column='CHECKOUT_SATISFACTION')
    # relationship
    review = Required(BizrateReview, column='FK_REVIEW_PID', fk_name='FK__AFTER_PURCHASE__REVIEW', cascade_delete=True)

    def get_ratings(self) -> dict:
        return {
            'shipping_options': self.shipping_options,
            'shipping_charges': self.shipping_charges,
            'ease_of_finding': self.ease_of_finding,
            'price_relative_to_other_retailers': self.price_relative_to_other_retailers,
            'clarity_product_info': self.clarity_product_info,
            'charges_stated_clearly': self.charges_stated_clearly,
            'site_design': self.site_design,
            'product_selection': self.product_selection,
            'checkout_satisfaction': self.checkout_satisfaction
        }


def update_db_tables(database: Database, create_tables=False):
    password = db_config.password if not db_config.encrypted else CipherUtils.decrypt(db_config.password)
    database.bind(provider=db_config.database, host=db_config.host,
                  user=db_config.user, passwd=password, db=db_config.schema)
    database.generate_mapping(create_tables)
