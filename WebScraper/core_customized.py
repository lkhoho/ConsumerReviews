import logging
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import referer_str, request_fingerprint
from urllib.parse import urlparse
from WebScraper.consumerReviewsScraper.models.main import db_connect
from WebScraper.consumerReviewsScraper.models.url_status import URLStatus

SUCCESS = 'succ'
FAILED = 'fail'
PENDING = 'pend'


class RFPDupeFilterWithStatus(BaseDupeFilter):
    """Request Fingerprint duplicates filter based on fingerprint and status. URLs will be persisted
            in database by default.
       Pending request will be updated once its response is received, either success, or failed. Failed request
            will be retried at the next spider run. When spider is running, fingerprint and status
            will be maintained in memory. At the close of spider, fingerprint and status will be persisted.
    """

    def __init__(self, debug=False):
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        self.engine = db_connect()
        self.session = sessionmaker(bind=self.engine)()
        self.logger.info('Connected to database %s' % self.engine.engine.url.database)

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(debug)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        url_status = self.session.query(URLStatus).filter_by(fingerprint=fp).one_or_none()
        if url_status is not None and url_status.status == SUCCESS:
            return True
        else:
            return False

    def request_fingerprint(self, request):
        return request_fingerprint(request)

    def close(self, reason):
        self.session.close_all()
        self.logger.info('Disconnected to database %s ' % self.engine.engine.url.database)

    def log(self, request, spider):
        if self.debug:
            msg = "Filtered duplicate request: %(request)s (referer: %(referer)s)"
            args = {'request': request, 'referer': referer_str(request)}
            self.logger.debug(msg, args, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)

    def get_domain(self, request):
        parsed_uri = urlparse(request.url)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return domain
