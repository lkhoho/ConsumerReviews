import nltk
from common import config

log = config.getLogger(__name__)


def readStopwords(filename) -> set:
    log.info('Reading stopwords from {}.'.format(filename))
    fp = open(filename)
    result = {word.strip().lower for word in fp}
    result |= set(nltk.corpus.stopwords.words('english'))
    log.info('After merging with NLTK, stopwords set now has {} words.'.format(len(result)))
    return result
