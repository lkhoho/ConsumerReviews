import os
import logging
from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.job import job_dir
from scrapy.utils.request import referer_str, request_fingerprint


class RFPDupeFilterWithStatus(BaseDupeFilter):
    """Request Fingerprint duplicates filter based on fingerprint and status. Persisted records will be in
            (status, fingerprint) format. Status: S - success. F - failed. P - pending.
       Pending request will be updated once its response is received, either success, or failed. Failed request
            will be retried at the next spider run. When spider is running, fingerprint and status
            will be maintained in memory. At the close of spider, fingerprint and status will be persisted.
    """

    def __init__(self, path=None, debug=False):
        self.file = None
        self.fingerprints = set()
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if path:
            self.file = open(os.path.join(path, 'requests.seen'), 'w')
            self.file.seek(0)
            self.fingerprints.update((s, x.rstrip()) for s, x in self.file)

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(job_dir(settings), debug)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        if ('S', fp) in self.fingerprints:
            return True
        self.fingerprints.add(('P', fp))

    def request_fingerprint(self, request):
        return request_fingerprint(request)

    def close(self, reason):
        if self.file:
            for status, fp in self.fingerprints:
                self.file.write('({}, {})\n'.format(status, fp))
            self.file.close()
            self.logger.info('{} requests status are persisted.'.format(len(self.fingerprints)))

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
