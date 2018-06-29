import re
import enum
import nltk
from nltk.corpus import sentiwordnet as swn
from common import config
from utils.logging import get_logger





class Lemmatizer(object):
    """
    Provide lemmatization for given corpus. Supports two lemmatizers: 1) by sentence; 2) by vocabulary.
    """

    log = get_logger('Lemmatizer')

    def __init__(self, tagger, lemm_type, part_of_speech: dict, data: list, stopwords: set):
        """
        Create a lemmatizer instance.
        :param tagger: a part-of-speech (POS) tagger.
        :param lemm_type: lemmatization type: PARAGRAPH, or VOCABULARY
        :param part_of_speech: POS mapping, e.g. {'NN' -> 'n', 'JJ' -> 'a'}
        :param stopwords: a set of stopwords.
        """

        self.tagger = tagger
        self.type = lemm_type
        self.pos = part_of_speech
        self.stopwords = stopwords
        self.data = data
        self.lemmatized_by_paragraph = []
        self.lemmatized_by_vocabulary = []
        self.scored_by_paragraph = {}
        self.scored_by_vocabulary = {}
        self.noScoreWordsByParagraph = []
        self.noScoreWordsByVocabulary = []
        self.cleanedByParagraph = []
        self.cleanedByVocabulary = []

    def computeSentimentScores(self, batch_size=50):
        """
        Getting sentiment scores of words from sentiwordnet.
        :param batch_size: number of words processed in single batch.
        :return list of review contents with each word has sentiment scores. e.g 'like': (v, 0.8, 0.0, 0.0)
        """

        if self.type is LemmatizerType.VOCABULARY:
            lemContents = self.lemmatized_by_vocabulary
            voc = set()
            Lemmatizer.log.info('Start getting scores of {} words by {}.'.format(
                len(lemContents), self.type.name.lower()))
            for content in lemContents:
                voc |= set([w for w in content.split() if len(w) > 1 and w != '###'])
            self.scored_by_vocabulary, self.noScoreWordsByVocabulary = self._scoreWordSet(voc, batch_size)
        else:
            lemContents = self.lemmatized_by_paragraph
            voc = set()
            Lemmatizer.log.info('Start getting scores of {} words by {}.'.format(
                len(lemContents), self.type.name.lower()))
            for content in lemContents:
                voc |= set([w for w in content.split() if len(w) > 1 and w != '###'])
            self.scored_by_paragraph, self.noScoreWordsByParagraph = self._scoreWordSet(voc, batch_size)

    def lemmatize(self):
        """
        Lemmatize review contents (review-by-review, or by vocabulary).
        """

        lem = nltk.WordNetLemmatizer()
        invalidPOS = 'x'  # invalid part-of-speech

        if self.type == LemmatizerType.VOCABULARY:
            Lemmatizer.log.info('Start lemmatizing data based on vocabulary.')
            self._lemmatizeByVocabulary(lem, invalidPOS)
            Lemmatizer.log.info('Finished lemmatizing data based on vocabulary.')
        elif self.type == LemmatizerType.PARAGRAPH:
            Lemmatizer.log.info('Start lemmatizing data based on paragraph.')
            self._lemmatizeByParagraph(lem, invalidPOS)
            Lemmatizer.log.info('Finished lemmatizing data based on paragraph.')

    def removeUndesiredWords(self):
        """
        Removing undesired words (including words starting with '###') in lemmatized review contents.
        """

        Lemmatizer.log.info('Removing words starting with "###"')
        for content in self.lemmatized_by_vocabulary:
            content = ' '.join([w for w in content.split() if not w.startswith('###')])
            self.cleanedByVocabulary.append(content)
        for content in self.lemmatized_by_paragraph:
            content = ' '.join([w for w in content.split() if not w.startswith('###')])
            self.cleanedByParagraph.append(content)

    def _lemmatizeByVocabulary(self, lem: nltk.WordNetLemmatizer, invalidPOS: str):
        """
        Lemmatize all review contents (by a vocabulary) based on part-of-speech tagging.
        :param lem: a lemmatizer.
        :param invalidPOS: symbol of invalid POS.
        :return: Lemmatized review contents.
        """

        # build vocabulary
        vocabulary = set()
        for paragraph in self.data:
            for sentence in nltk.sent_tokenize(paragraph.strip().lower()):
                tokens = [t for t in re.findall(r'[a-zA-Z]+', sentence) if len(t) > 1]
                tokens = [w for w in tokens if w not in self.stopwords]
                vocabulary |= set(tokens)
        Lemmatizer.log.info('Vocabulary of size {} is built.'.format(len(vocabulary)))

        # tag vocabulary
        taggedVocabulary = {word: pos for (word, pos) in self.tagger.tag(list(vocabulary))}
        Lemmatizer.log.info('Tagged vocabulary has {} word-pos pairs.'.format(len(taggedVocabulary)))

        # lemmatize data
        for paragraph in self.data:
            words = []
            for sentence in nltk.sent_tokenize(paragraph.strip().lower()):
                tokens = [t for t in re.findall(r'[a-zA-Z]+', sentence) if len(t) > 1]
                tokens = [w for w in tokens if w not in self.stopwords]
                tokens.append('###')
                for word in tokens:
                    pos = self.pos.get(taggedVocabulary.get(word), invalidPOS)
                    if word in taggedVocabulary and pos != invalidPOS:
                        word = lem.lemmatize(word, pos=pos)
                        words.append(word + '_' + pos)
                    elif word == '###':
                        words.append('###')
            self.lemmatized_by_vocabulary.append(' '.join(words))

    def _lemmatizeByParagraph(self, lem: nltk.WordNetLemmatizer, invalidPOS: str):
        """
        Lemmatize all review contents (review-by-review) based on part-of-speech tagging.
        :param lem: a lemmatizer.
        :param invalidPOS: symbol of invalid POS.
        :return: Lemmatized review contents.
        """

        for paragraphId, paragraph in enumerate(self.data):
            if paragraphId % 1000 == 0:
                Lemmatizer.log.info('Lemmatize paragraph {} of {}.'.format(paragraphId, len(self.data)))

            text = paragraph.strip().lower()
            sentences = nltk.sent_tokenize(text)
            review_words = []
            for sentence in sentences:
                review_words += [w for w in sentence.split() if w not in self.stopwords and len(w) > 1]
                review_words.append('###')
            text = [w for w in review_words if w not in self.stopwords and len(w) > 1]
            tagged = [t for t in self.tagger.tag(text) if len(t[0]) > 1]
            words = []
            for t in tagged:
                raw_words = re.findall(r'[a-zA-Z]+|#{3}', t[0])
                pos = self.pos.get(t[1], invalidPOS)
                if len(raw_words) > 0 and (raw_words[0] not in self.stopwords) and pos != invalidPOS:
                    word = raw_words[0]
                    word = lem.lemmatize(word, pos=pos)
                    words.append(word + '_' + pos)
                elif len(raw_words) > 0 and raw_words[0] == '###':
                    words.append('###')
            self.lemmatized_by_paragraph.append(' '.join(words))

    def _scoreWordSet(self, words: set, batchSize=50) -> tuple:
        """
        Get sentiment score for word in a set.
        :param words: a set of words.
        :param batchSize: number of words processed in single batch.
        :return Tuple of (word-to-scores. e.g 'like': (v, 0.8, 0.0, 0.0), words-no-scores)
        """

        result, noScoreWords = {}, []
        nWords = len(words)
        nChunks = (nWords + batchSize - 1) // batchSize
        wordsList = list(words)

        for i in range(nChunks):
            start = i * batchSize
            end = (i + 1) * batchSize if (i + 1) * batchSize < nWords else nWords
            sublist = wordsList[start:end]
            for w in sublist:
                word = w[:-2]
                part = w[-1]
                wordStr = "{}.{}.01".format(word, part)
                Lemmatizer.log.debug('wordStr: ' + wordStr)
                try:
                    res = swn.senti_synset(wordStr)
                    result[w] = (res.pos_score(), res.neg_score(), res.obj_score())
                except:
                    noScoreWords.append(w)
        Lemmatizer.log.info('Finished. Found scores of {} words. Scores of {} words are not found.'.format(
            len(result), len(noScoreWords)))
        return result, noScoreWords
