from sqlalchemy import Index, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, Float, String, DateTime


# base class for all database tables
DeclarativeBase = declarative_base()


class University(DeclarativeBase):
    """
    Universities on topuniversities.com
    """

    __tablename__ = 'university'
    
    pid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    country = Column(String(255))
    region = Column(String(255))
    created_datetime = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('idx__university__country', 'country'),
        Index('idx__university__name', 'name')
    )

    def __repr__(self):
        return '<University(name={}, country={})>'.format(self.name, self.country)


class UniversityRanking(DeclarativeBase):
    """
    Ranking of universities on topuniversities.com
    """

    __tablename__ = 'university_ranking'

    pid = Column(Integer, primary_key=True, autoincrement=True)
    subject = Column(String(255), nullable=False)
    year = Column(Integer)
    ranking = Column(String(255), nullable=False)
    score = Column(Float)
    university_id = Column(Integer, nullable=False)
    created_datetime = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('idx__university_ranking__year', 'year'),
        Index('idx__university_ranking__subject', 'subject')
    )

    def __repr__(self):
        return '<UniversityRanking(subject={}, year={}, ranking={}, university_id={})>'.format(
            self.subject, self.year, self.ranking, self.university_id)
