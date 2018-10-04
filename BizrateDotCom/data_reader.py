import pandas as pd
from common import config
from common.exception import UnsupportedFileFormatError

log = config.get_logger(__name__)


def readReviewData(filename: str) -> tuple:
    """
    Read raw review data in CSV format.
    :param filename:
    :return: tuple of (reviews, posneg).
    """

    fileExtension = filename.split('.')[-1]

    if fileExtension != 'csv':
        raise UnsupportedFileFormatError('Error: file of type {} is not supported.'.format(fileExtension))

    log.info('Reading reviews from {}'.format(filename))
    df = pd.read_csv(filename, encoding='utf-8', na_filter=False)
    scores = df[config.bizrateDotComLabelConfig['scoreName']]
    reviews = [x.strip() for x in df['content'].tolist()]
    posneg = [1 if score >= config.bizrateDotComLabelConfig['threshold'] else 0 for score in scores]

    assert len(reviews) == len(posneg), 'Error: # of reviews should equal to # of classes.'
    log.info('Read {} reviews from file {}.'.format(len(reviews), filename))

    return reviews, posneg
