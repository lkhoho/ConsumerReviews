from sqlalchemy import Index, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import BigInteger, String, DateTime


# base class for all database tables
DeclarativeBase = declarative_base()


class URLStatus(DeclarativeBase):
    """
    Spider crawling status. This is persistent among jobs.
    """

    __tablename__ = 'url_status'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fingerprint = Column(String(512), nullable=False, index=True, unique=True)
    domain = Column(String(512))
    status = Column(String(32))
    last_update_datetime = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('uidx__url_status__fingerprint', 'fingerprint', unique=True),
    )

    def __repr__(self):
        return '<URLStatus(id={}, fingerprint={}, domain={})>'.format(self.id, self.fingerprint, self.domain)
