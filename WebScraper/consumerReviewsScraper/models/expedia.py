from sqlalchemy import Table, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, BigInteger, Boolean, Float, String, Text, Date, DateTime


# base class for all database tables
DeclarativeBase = declarative_base()


class ExpediaHotel(DeclarativeBase):
    """
    Hotel on expedia.com
    """

    __tablename__ = 'expedia_hotel'
    
    pid = Column(Integer, primary_key=True, autoincrement=True)
    hotel_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    nearby_info = Column(Text)
    around_info = Column(Text)
    introduction = Column(Text)
    created_datetime = Column(DateTime, nullable=False)

    def __repr__(self):
        return '<ExpediaHotel(hotel_id={}, name={})>'.format(self.hotel_id, self.name)


class ExpediaReview(DeclarativeBase):
    """
    Review of hotels on expedia.com
    """

    __tablename__ = 'expedia_review'

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(BigInteger, nullable=False)
    author = Column(String)
    publish_datetime = Column(DateTime, nullable=False)
    content = Column(Text)
    created_datetime = Column(DateTime, nullable=False)
    overall_rating = Column(Integer)
    num_helpful = Column(Integer)
    stay_duration = Column(String)
    response_id = Column(String)
    response_author = Column(String)
    response_publish_datetime = Column(DateTime)
    response_content = Column(Text)
    superlative = Column(String)
    title = Column(String)
    locale = Column(String)
    location = Column(String)
    remarks_positive = Column(String)
    remarks_negative = Column(String)
    remarks_location = Column(String)
    response_display_locale = Column(String)
    hotel_id = Column(Integer)

    def __repr__(self):
        return '<ExpediaReview(review_id={}, hotel_id={})>'.format(self.review_id, self.hotel_id)
