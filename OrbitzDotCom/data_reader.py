import simplejson as json
from common import config
from common.exception import UnsupportedFileFormatError

log = config.getLogger(__name__)


def readReviewData(filename: str) -> tuple:
    """
    Read raw review data in JSON format.
    :param filename:
    :return: tuple of (reviews, posneg).
    """

    fileExtension = filename.split('.')[-1]

    if fileExtension != 'json':
        raise UnsupportedFileFormatError('Error: file of type {} is not supported.'.format(fileExtension))

    log.info('Reading reviews from {}'.format(filename))
    with open(filename) as fp:
        reviews = json.load(fp, encoding='utf-8')
        reviews = [x['content'].strip() for x in reviews]
        posneg = [1 if x['will_recommend'] == 1 else 0 for x in reviews]

    assert len(reviews) == len(posneg), 'Error: # of reviews should equal to # of classes.'
    log.info('Read {} reviews from file {}.'.format(len(reviews), filename))

    return reviews, posneg
