from sqlalchemy import create_engine, Column, INTEGER, BIGINT, VARCHAR, DATE, DATETIME, JSON, Index, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

BaseEntity = declarative_base()


class BizrateReview(BaseEntity):
    __tablename__ = 'BIZRATE_REVIEW'

    pid = Column('PID', BIGINT, autoincrement=True, primary_key=True)
    create_time = Column('CREATE_TIME', DATETIME)
    source = Column('SOURCE', VARCHAR(128))
    store = Column('STORE', VARCHAR(128))
    tags = Column('TAGS', JSON)
    shop_again = Column('SHOP_AGAIN', INTEGER)
    to_recommend = Column('TO_RECOMMEND', INTEGER)
    satisfaction = Column('SATISFACTION', INTEGER)
    # pre-purchase
    pre_title = Column('PRE_TITLE', VARCHAR(256))
    pre_author = Column('PRE_AUTHOR', VARCHAR(256))
    pre_date = Column('PRE_DATE', DATE)
    pre_content = Column('PRE_CONTENT', VARCHAR(5000))
    # after-purchase
    after_title = Column('AFTER_TITLE', VARCHAR(256))
    after_author = Column('AFTER_AUTHOR', VARCHAR(256))
    after_date = Column('AFTER_DATE', DATE)
    after_content = Column('AFTER_CONTENT', VARCHAR(5000))
    site_experience_ratings = relationship('BizrateSiteExperienceRatings', back_populates='review')
    after_purchase_ratings = relationship('BizrateAfterPurchaseRatings', back_populates='review')


Index('IX__BIZRATE_REVIEW_STORE', BizrateReview.store)


class BizrateAfterPurchaseRatings(BaseEntity):
    __tablename__ = 'BIZRATE_AFTER_PURCHASE_RATINGS'

    pid = Column('PID', BIGINT, autoincrement=True, primary_key=True)
    create_time = Column('CREATE_TIME', DATETIME)
    review_pid = Column('FK_REVIEW_PID', BIGINT, ForeignKey('BIZRATE_REVIEW.PID', name='FK__AFTER_PURCHASE__REVIEW',
                                                            onupdate='RESTRICT', ondelete='CASCADE'))
    product_availability = Column('PRODUCT_AVAILABILITY', INTEGER)
    ontime_delivery = Column('ONTIME_DELIVERY', INTEGER)
    product_met_expectations = Column('PRODUCT_MET_EXPECTATIONS', INTEGER)
    order_tracking = Column('ORDER_TRACKING', INTEGER)
    return_process = Column('RETURN_PROCESS', INTEGER)
    customer_support = Column('CUSTOMER_SUPPORT', INTEGER)
    review = relationship('BizrateReview', back_populates='after_purchase_ratings')


class BizrateSiteExperienceRatings(BaseEntity):
    __tablename__ = 'BIZRATE_SITE_EXPERIENCE_RATINGS'

    pid = Column('PID', BIGINT, autoincrement=True, primary_key=True)
    create_time = Column('CREATE_TIME', DATETIME)
    review_pid = Column('FK_REVIEW_PID', BIGINT, ForeignKey('BIZRATE_REVIEW.PID', name='FK__SITE_EXPERIENCE__REVIEW',
                                                            onupdate='RESTRICT', ondelete='CASCADE'))
    shipping_options = Column('SHIPPING_OPTIONS', INTEGER)
    shipping_charges = Column('SHIPPING_CHARGES', INTEGER)
    ease_of_finding = Column('EASE_OF_FINDING', INTEGER)
    price_relative_to_other_retailers = Column('PRICE_RELATIVE_TO_OTHER', INTEGER)
    clarity_product_info = Column('CLARITY_PRODUCT_INFO', INTEGER)
    charges_stated_clearly = Column('CHARGES_STATED_CLEARLY', INTEGER)
    site_design = Column('SITE_DESIGN', INTEGER)
    product_selection = Column('PRODUCT_SELECTION', INTEGER)
    checkout_satisfaction = Column('CHECKOUT_SATISFACTION', INTEGER)
    review = relationship('BizrateReview', back_populates='site_experience_ratings')


# engine = create_engine('mysql+pymysql://root:welcome@localhost:3306/CONSUMER_REVIEWS?charset=utf8mb4',
#                        echo=True)
# BizrateReview.metadata.create_all(engine)
#
# import datetime
# review = BizrateReview(create_time=datetime.datetime.now(), source='abc.com')
# print(review.after_purchase_ratings)
