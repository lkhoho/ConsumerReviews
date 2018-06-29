import re
import enum
import nltk
from nltk.corpus import sentiwordnet as swn
from utils.logging import get_logger


POS = {
    'JJ': 'a', 'JJR': 'a', 'JJS': 'a', 'NN': 'n', 'NNP': 'n', 'NNS': 'n', 'NNPS': 'n', 'RB': 'r',
    'RBR': 'r', 'RBS': 'r', 'VB': 'v', 'VBD': 'v', 'VBG': 'v', 'VBN': 'v', 'VBP': 'v'
}


@enum.unique
class LemmatizationType(enum.Enum):
    """
    Lemmatizer types.
    """

    PARAGRAPH = 0
    VOCABULARY = 1


class Lemmatizer(object):
    """
    Lemmatize a list of texts by given lemmatization type (paragraph or vocabulary).
    """

    log = get_logger('Lemmatizer')

    def __init__(self, tagger, part_of_speech=POS, stopwords=None, lemm_type=LemmatizationType.VOCABULARY):
        super().__init__()
        self.tagger = tagger
        self.pos = part_of_speech
        if stopwords is not None:
            self.stopwords = stopwords
        else:
            self.stopwords = Lemmatizer.read_stopwords()
        self.type = lemm_type
        self.lem = nltk.WordNetLemmatizer()

    def preprocess(self, texts: list):
        if self.type is LemmatizationType.PARAGRAPH:
            return self._lem_by_para(texts)
        elif self.type is LemmatizationType.VOCABULARY:
            return self._lem_by_voc(texts)

    def _lem_by_para(self, texts, sentence_sep='###', invalid_pos='x', verbose=1000):
        result = []
        for text_id, text in enumerate(texts):
            if text_id % verbose == 0:
                self.log.info('Lemmatize text {}/{} by paragraph.'.format(text_id, len(texts)))
            text = text.strip().lower()
            sentences = nltk.sent_tokenize(text)
            words = []
            for sent in sentences:
                words += [x for x in sent.split() if x not in self.stopwords and len(x) > 1]
                words.append(sentence_sep)
            text = [x for x in words if x not in self.stopwords and len(x) > 1]
            tagged = [x for x in self.tagger.tag(text) if len(x[0]) > 1]
            words = []
            for t in tagged:
                raw_words = re.findall(r'[a-zA-Z]+|#{3}', t[0])
                pos = self.pos.get(t[1], invalid_pos)
                if len(raw_words) > 0 and (raw_words[0] not in self.stopwords) and pos != invalid_pos:
                    word = raw_words[0]
                    word = self.lem.lemmatize(word, pos=pos)
                    words.append(word + '_' + pos)
                elif len(raw_words) > 0 and raw_words[0] == sentence_sep:
                    words.append(sentence_sep)
            result.append(' '.join(words))
        return result

    def _lem_by_voc(self, texts, sentence_sep='###', invalid_pos='x'):
        result = []
        vocabulary = set()
        texts = [text.strip().lower() for text in texts]
        for text in texts:
            for sent in nltk.sent_tokenize(text):
                tokens = [x for x in re.findall(r'[a-zA-Z]+', sent) if len(x) > 1]
                tokens = set(filter(lambda x: x not in self.stopwords, tokens))
                vocabulary |= tokens
        tagged_voc = {word: pos for (word, pos) in self.tagger.tag(list(vocabulary))}
        for text in texts:
            words = []
            for sent in nltk.sent_tokenize(text):
                tokens = [x for x in re.findall(r'[a-zA-Z]+', sent) if len(x) > 1]
                tokens = list(filter(lambda x: x not in self.stopwords, tokens))
                tokens.append(sentence_sep)
                for word in tokens:
                    pos = self.pos.get(tagged_voc.get(word), invalid_pos)
                    if word in tagged_voc and pos != invalid_pos:
                        word = self.lem.lemmatize(word, pos=pos)
                        words.append(word + '_' + pos)
                    elif word == sentence_sep:
                        words.append(sentence_sep)
            result.append(' '.join(words))
        return result

    @staticmethod
    def read_stopwords(file='../resource/stopwords.txt'):
        result = set(nltk.corpus.stopwords.words('english'))
        with open(file) as fp:
            result |= {word.strip().lower for word in fp}
        return result


class SentimentScoreCalculator(object):
    """
    Calculate sentiment score of words in a given set from sentiwordnet.
    """

    log = get_logger('SentimentScoreCalculator')

    def __init__(self):
        super().__init__()

    def preprocess(self, texts, batch_size=100, sep='###'):
        texts = [text.strip().lower() for text in texts]
        words = set()
        for text in texts:
            words |= set([x for x in text.split() if len(x) > 1 and x != sep])
        result = {}
        no_score_words = []
        num_words = len(words)
        num_chunks = (num_words + batch_size - 1) // batch_size
        word_list = list(words)
        for i in range(num_chunks):
            start = i * batch_size
            end = (i + 1) * batch_size if (i + 1) * batch_size < num_words else num_words
            sublist = word_list[start:end]
            for w in sublist:
                word = w[:-2]
                part = w[-1]
                word_str = '{}.{}.01'.format(word, part)
                try:
                    res = swn.senti_synset(word_str)
                    result[w] = (res.pos_score(), res.neg_score(), res.obj_score())
                except:
                    no_score_words.append(w)
        return result, no_score_words


class WordCleaner(object):
    """
    Remove undesired words (including words starting with '###') in lemmatized review contents.
    """

    log = get_logger('WordCleaner')

    def __init__(self):
        super().__init__()

    def preprocess(self, texts, sep='###'):
        result = []
        for text in texts:
            text = ' '.join([w for w in text.split() if not w.startswith(sep)])
            result.append(text)
        return result
