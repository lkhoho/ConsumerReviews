from sqlalchemy import Index, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, BigInteger, Boolean, Float, String, Text, Date, DateTime


# base class for all database tables
DeclarativeBase = declarative_base()


class ExpediaHotel(DeclarativeBase):
    """
    Hotel on expedia.com
    """

    __tablename__ = 'expedia_hotel'
    
    pid = Column(BigInteger, primary_key=True, autoincrement=True)
    hotel_id = Column(BigInteger, nullable=False, index=True, unique=True)
    name = Column(String(255), nullable=False)
    nearby_info = Column(Text)
    around_info = Column(Text)
    introduction = Column(Text)
    created_datetime = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('uidx__expedia_hotel__hotel_id', 'hotel_id', unique=True),
    )

    def __repr__(self):
        return '<ExpediaHotel(hotel_id={}, name={})>'.format(self.hotel_id, self.name)


class ExpediaReview(DeclarativeBase):
    """
    Review of hotels on expedia.com
    """

    __tablename__ = 'expedia_review'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    review_id = Column(String(255), nullable=False, index=True, unique=True)
    author = Column(String(255))
    publish_datetime = Column(DateTime, nullable=False)
    content = Column(Text)
    created_datetime = Column(DateTime, nullable=False)
    overall_rating = Column(Integer)
    num_helpful = Column(Integer)
    stay_duration = Column(String(255))
    response_id = Column(String(255))
    response_author = Column(String(255))
    response_publish_datetime = Column(DateTime)
    response_content = Column(Text)
    superlative = Column(String(255))
    title = Column(String(255))
    locale = Column(String(32))
    location = Column(Text)
    remarks_positive = Column(Text)
    remarks_negative = Column(Text)
    remarks_location = Column(Text)
    response_display_locale = Column(String(32))
    hotel_id = Column(BigInteger)

    __table_args__ = (
        Index('uidx__expedia_review__review_id', 'review_id', unique=True),
    )

    def __repr__(self):
        return '<ExpediaReview(review_id={}, hotel_id={})>'.format(self.review_id, self.hotel_id)
