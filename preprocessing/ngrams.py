from collections import Counter, defaultdict
from sklearn.feature_extraction.text import CountVectorizer
from spacy.tokens import Span
from textblob import TextBlob
from typing import List, Set, Tuple, Generator
import numpy as np
import os
import pandas as pd
import spacy


# Spacy document parser
parser = spacy.load('en_core_web_sm')

# list of POS tag belonging to nouns
noun_tag = set(['NN', 'NNP', 'NNS'])

# list of dependencies not likely to be words found in features
com_dep = set(['det', 'aux', 'cc', 'punct', 'mark', '', 'neg', 'nummod',
               'prt', 'auxpass', 'case', 'expl', 'preconj', 'intj',
               'predet', 'meta', 'quantmod', 'agent'])

# list of POS tags not likely to be words found in features
com_tag = set(['IN', 'PRP', 'PRP$', 'DT', 'HYPH', 'TO', ',', '.', 'CC',
               'SP', 'CD', 'MD', 'WDT', 'RP', 'WRB', '-LRB-', '-RRB-',
               ':', 'WP', 'POS', '``', "''", 'SYM', 'EX', 'PDT', 'UH',
               'NFP', 'XX'])


class SentCustomProperties(object):
    '''
    Adds properties to spacy sentences
    '''

    def __init__(self, review_id: int, rating: float, sent_idx: int, sent: Span):
        '''
        INPUT: int, int, int, spacy sentence (spacy.tokens.span.Span)
        OUTPUT: None

        Attribures:
            review_idd (int): ID of review where sentence originated
            review_rating (int): customer review rating
            sent (spacy.tokens.span.Span): spacy Span object
            sent_idx (int): index of sentence within review corpus
            start_idx (int): index of first token in sentence within review
            n_words (int): num of words in sentences
        '''
        self.review_id = review_id
        self.review_rating = rating
        self.sent = sent
        self.sent_idx = sent_idx
        self.start_idx = sent[0].i
        self.n_words = len(sent)


class ReviewSents(object):
    '''
    Takes a list of unicode reviews and stores the sentences
    (with additional properties) in the returned object
    '''

    def __init__(self, data: pd.DataFrame, id_field: str, text_field: str, rating_field: str):
        '''
        INPUT: pd.DataFrame, str, str, str
        OUTPUT: None

        Attribures:
            n_reviews (int): total number of reviews for product
            n_sent (int): total number of sentences in all reviews for product
            ratings (list): list of customer review ratings for product
            reviews (list): list of customer review text for product
            sentences (list): list of SentCustomProperties objects
        '''
        self.data = data
        self.id_field = id_field
        self.text_field = text_field
        self.rating_field = rating_field
        self.n_reviews = data.shape[0]
        self.n_sent, self.sentences = self._parse_sentences()

    def _parse_sentences(self) -> Tuple[int, List[SentCustomProperties]]:
        """
        Uses spacy to parse and split the sentences. 
        Returns number of sentences, and list of spacy objects.
        """

        n_sent = 0
        sentences = []

        for _, row in self.data.iterrows():
            review_id = row[self.id_field]
            text = row[self.text_field].lower()
            rating = row[self.rating_field]
            try:
                review = parser(text)
            except AssertionError:
                print('parser for review #{} failed'.format(review_id))
                continue

            for sent in review.sents:
                if sent.string:
                    sentences.append(SentCustomProperties(review_id, rating, n_sent, sent))
                    n_sent += 1

        return n_sent, sentences


class Unigramer(object):
    '''
    Class for extracting Unigrams.
    '''

    def __init__(self):
        '''
        Attribures:
            cnt_dict (dict): {word -> word_freq in all reviews}
            aspect_dict (dict): {aspect -> aspect_freq in all reviews}
            dep_dict (dict): {word -> list(dependency types that corresponds with the word token)}
            rev_dict (dict): {word -> set(review IDs containg this word)}
            sent_dict (dict): {word -> list(sentence indices containing this word)}
            word_pos_dict (dict): {word -> list(token index of word within spacy sentences)}
            unigrams (set): set of unigrams obtained with candidate_unigrams function
            n_reviews (int): total number of reviews
            non_aspects (set): a set of words considered as non-aspects
        '''
        self.cnt_dict = defaultdict(int)
        self.aspect_dict = defaultdict(int)
        self.dep_dict = defaultdict(list)
        self.rev_dict = defaultdict(set)
        self.sent_dict = defaultdict(list)
        self.word_pos_dict = defaultdict(list)
        self.unigrams = None
        self.n_reviews = None
        self.non_aspects = set()

        with open(os.sep.join(['resources', 'nonaspects.txt'])) as fp:
            for line in fp:
                self.non_aspects.add(line.strip())

    def _iter_nouns(self, sent: SentCustomProperties):
        '''
        INPUT: SentCustomProperties
        OUTPUT: str

        Iterates through each token of spacy sentence and collects
        lemmas of all nouns into a set.
        '''

        wordset = set()
        acomp_dict = defaultdict(list)

        for token in sent.sent:
            self.cnt_dict[token.lemma_] += 1
            self.dep_dict[token.head.lemma_].append((token, token.dep_))
            if token.dep_ == 'acomp':
                acomp_dict[token.head] = token
            root = parser.vocab[token.lemma].prob

            # filter to only consider nouns, valid aspects, and uncommon words
            if token.tag_ in noun_tag and (root < -7.5 and token.lemma_ not in self.non_aspects):
                wordset.add(token.lemma_)
                self.aspect_dict[token.lemma_] += 1  # treat nouns as aspects
                self.rev_dict[token.lemma_].add(sent.review_id)

                if sent.sent_idx not in self.sent_dict[token.lemma_]:
                    i = token.i - sent.start_idx
                    self.word_pos_dict[token.lemma_].append(i)
                    self.sent_dict[token.lemma_].append(sent.sent_idx)

        for head_word, token in acomp_dict.items():
            for child in filter(lambda x: x.tag_ in noun_tag, head_word.children):
                self.dep_dict[child.lemma_].append((token, token.dep_))

        return ' '.join(wordset)

    def candidate_unigrams(self, corpus: ReviewSents, min_pct=0.001, a_pct=0.09) -> Set[str]:
        '''
        INPUT: ReviewSents, float, float
        OUTPUT: set

        Args:
            min_pct: percentage of sentences unigram must appear in
            a_pct: minimum percentage where word element has a corresponding 'amod' or 'acomp' dependency

        Obtains a set of candidate unigrams.
        Each candidate unigram must be a noun.
        '''

        count_X = []
        self.n_reviews = corpus.n_reviews

        for sent in corpus.sentences:
            count_X.append(self._iter_nouns(sent))

        cnt_vec = CountVectorizer()
        freq = cnt_vec.fit_transform(count_X)

        total_count = freq.toarray().sum(axis=0)

        # filter for aspect appearing in min_pct of sentences
        features = np.array(cnt_vec.get_feature_names())
        unigrams = set(features[total_count >= min_pct * corpus.n_sent])

        # filter for percentage of time aspect is modified by amod
        for word in unigrams.copy():
            arr = np.array([x[1] for x in self.dep_dict[word]])
            arr = ((arr == 'amod') | (arr == 'acomp'))

            if np.mean(arr) < a_pct:
                unigrams.remove(word)

        self.unigrams = unigrams

        return unigrams

    def update_review_count(self, bigramer, trigramer=None):
        '''
        IMPUT: Bigramer, Trigramer
        OUTPUT: none

        Updates Unigramer rev_dict so that reviews aren't double counted for
        unigram words appearing in bigrams and trigrams.
        '''
        update_queue = self.unigrams & bigramer.bigram_words

        for unigram in update_queue:
            for bigram in bigramer.bigrams:
                if unigram in bigram:
                    self.rev_dict[unigram] -= bigramer.rev_dict[bigram]

            if not trigramer:
                continue

            for trigram in trigramer.trigrams:
                if unigram in trigram:
                    self.rev_dict[unigram] -= trigramer.rev_dict[trigram]


class Bigramer(object):
    '''
    Class for extracting Bigrams.
    '''

    def __init__(self, unigramer: Unigramer):
        '''
        INPUT: unigramer
        OUTPUT: None

        Attribures:
            avg_dist (dict): {word -> word_freq in all reviews}
            distances (dict): {bigram -> list(absolute word spacing difference betweeen words of bigram)}
            ordering (dict): {bigram -> list(which word in bigram appears first in sentence)}
            pmi (dict): {bigram -> float describing Pointwise Mutual Information between words in bigram}
            rev_dict (dict): {word -> set(review IDs containing this word)}
            sent_dict (dict): {word -> list(sentence indices containing word)}
            word_pos_dict (dict): {word -> list(token indices of word within spacy sentence)}
            bigrams (set): set of bigrams obtained with candidate_bigrams function
            bigram_words (set): set of words used in bigrams
            unigramer (Unigramer): Unigramer object for product
        '''
        self.avg_dist = defaultdict(float)
        self.distances = defaultdict(list)
        self.ordering = defaultdict(list)
        self.pmi = defaultdict(float)
        self.rev_dict = defaultdict(set)
        self.sent_dict = defaultdict(list)
        self.word_pos_dict = defaultdict(list)
        self.bigrams = set()
        self.bigram_words = set()
        self.unigramer = unigramer

    def _reverse_key(self, key, new_key):
        '''
        INPUT: str, str
        OUTPUT: None

        Args:
            key: bigram with words seperated by space
            new_key: bigram with words reversed and seperated by space

        Reverses the word order for the key in the class dictionaries
        '''
        self.avg_dist[new_key] = self.avg_dist.pop(key)
        self.distances[new_key] = self.distances.pop(key)
        self.ordering[new_key] = self.ordering.pop(key)[::-1]
        self.pmi[new_key] = self.pmi.pop(key)
        self.rev_dict[new_key] = self.rev_dict.pop(key)
        self.sent_dict[new_key] = self.sent_dict.pop(key)
        self.word_pos_dict[new_key] = self.word_pos_dict.pop(key)

    def _get_compactness_feat(self, corpus: ReviewSents):
        '''
        INPUT: ReviewSents
        OUTPUT: generator(tuples(str))

        Returns generator of tuples (in alphabetical order) consisting of:
            at least one noun
            a second word within +/- 3 words of noun
        Excludes dependencies and tags not likely to be a feature word
        '''

        for sent in corpus.sentences:
            output = set()

            for i, token in enumerate(sent.sent):
                # one word in bigram must be noun
                if token.tag_ in noun_tag and token.lemma_ not in nonaspects:
                    arr = sent.sent[max(0, i - 3):min(i + 4, sent.n_words)]
                    arr = np.array(arr)
                    arr = arr[arr != token]

                    for item in arr:
                        root = parser.vocab[item.lemma].prob
                        # filter out unlikely features
                        if root < -7.5 and (item.dep_ not in com_dep and
                                            item.tag_ not in com_tag and
                                            item.lemma_ not in nonaspects):
                            bigrm = ' '.join(sorted([item.lemma_, token.lemma_]))
                            dist = item.i - token.i
                            word_sort = item.lemma_ < token.lemma_

                            if not self.ordering[bigrm]:
                                self.ordering[bigrm] = [0, 0]

                            self.distances[bigrm].append(abs(dist))
                            self.rev_dict[bigrm].add(sent.review_id)
                            self.ordering[bigrm][word_sort == (dist > 0)] += 1

                            if sent.sent_idx not in self.sent_dict[bigrm]:
                                i = token.i - sent.start_idx
                                self.word_pos_dict[bigrm].append(i)
                                self.sent_dict[bigrm].append(sent.sent_idx)

                            output.add(bigrm)

            if output:
                for element in output:
                    yield element

    def candidate_bigrams(self, corpus: ReviewSents, min_pct=0.005, pmi_pct=0.0003, max_avg_dist=2) -> Set[str]:
        '''
        INPUT: ReviewSents, float, float, float
        OUTPUT: set(str)

        Args:
            min_pct: percentage of sentences bigram must appear in
            pmi_pct: minimum PMI value for bigram to be considered valid
            max_avg_dist: maximum average word spacing distance for bigram to
                          be considered valud

        Outputs set of bigram strings with words in bigram seperated by space
        '''
        bigrams, bigram_words = self.bigrams, self.bigram_words
        cnt_dict = self.unigramer.cnt_dict

        feats = Counter(self._get_compactness_feat(corpus))

        for (key, val) in feats.items():
            order = sorted(key.split(' '),
                           reverse=self.ordering[key][1] > self.ordering[key][0])
            new_key = ' '.join(order)

            pmi = val / (cnt_dict[order[0]] * cnt_dict[order[1]])
            avg_dist = round(np.mean(self.distances[key]), 2)

            if pmi >= pmi_pct and (avg_dist < max_avg_dist and val >= max(2, min_pct * corpus.n_sent)):
                self.avg_dist[key] = avg_dist
                self.pmi[key] = pmi

                bigrams.add(new_key)
                bigram_words.update(set(order))

                if key != new_key:
                    self._reverse_key(key, new_key)

        return bigrams

    def pop_bigrams(self, trigramer):
        '''
        IMPUT: Trigramer
        OUTPUT: none

        Remove bigrams if the words appear in a trigram
        '''
        bigrams = sorted(list(self.bigrams))
        split_bigrams = [bigram.split(" ") for bigram in bigrams]

        for trigram in trigramer.trigrams:
            trigram = trigram.split(" ")
            for bigram, split in zip(bigrams, split_bigrams):
                if split[0] in trigram and split[1] in trigram:
                    self.bigrams -= set([bigram])


class Trigramer(object):
    '''
    Class for extracting trigrams.
    '''

    def __init__(self, bigramer: Bigramer):
        '''
        INPUT: Bigramer
        OUTPUT: None

        Attribures:
            bigramer (Bigramer): Bigramer object for product
            rev_dict (dict): {word -> set(review IDs containing this word)}
            sent_dict (dict): {word -> list(sentence indices containing this word)}
            word_pos_dict (dict): {word -> list(token index of word within spacy sentence)}
            trigrams (set): set of trigrams obtained with candidate_trigrams function
        '''
        self.bigramer = bigramer
        self.rev_dict = defaultdict(set)
        self.sent_dict = defaultdict(list)
        self.word_pos_dict = defaultdict(list)
        self.trigrams = set()

    def _find_idx(self, corpus, bigram1, bigram2, trigram):
        '''
        INPUT: ReviewSents, str, str, str
        OUTPUT: None

        Args:
            bigram1: first two words of trigram
            bigram2: last two words of trigram
            trigram: three word aspect

        Checks the rev_dict, sent_dict, and word_pos_dict of bigrams in trigram
        to build the same dictionaries for the trigram.
        '''
        bgrm_sdict = self.bigramer.sent_dict
        tgrm_sdict = self.sent_dict
        bgrm_wdict = self.bigramer.word_pos_dict
        tgrm_wdict = self.word_pos_dict

        match_idx = np.in1d(bgrm_sdict[bigram1], bgrm_sdict[bigram2])
        match_reviews = set()

        tgrm_sdict[trigram] = np.array(bgrm_sdict[bigram1])[match_idx].tolist()
        tgrm_wdict[trigram] = np.array(bgrm_wdict[bigram1])[match_idx].tolist()

        for si in tgrm_sdict[trigram]:
            match_reviews.add(corpus.sentences[si].review_id)

        self.rev_dict[trigram] = match_reviews

    def candidate_trigrams(self, corpus: ReviewSents, review_pct=0.10) -> set:
        '''
        INPUT: ReviewSents, float
        OUTPUT: set(str)

        Args:
            review_pct: percentage of reviews two bigrams linked in a trigram
                        must appear together in (relative to the bigram
                        appearing in fewer reviews)

        Outputs set of trigram strings with words in trigram seperated by space
        '''
        bigrams, trigrams = self.bigramer.bigrams, self.trigrams
        bgrm_rdict = self.bigramer.rev_dict

        split_bigrams = [bigram.split(' ') for bigram in bigrams]

        for bigram1, split1 in zip(bigrams, split_bigrams):
            for bigram2, split2 in zip(bigrams, split_bigrams):
                # check if the bigrams connect to form a trigram
                if split1[1] != split2[0]:
                    continue

                bg1_cnt = len(bgrm_rdict[bigram1])
                bg2_cnt = len(bgrm_rdict[bigram2])
                bg12_min_cnt = min(bg1_cnt, bg2_cnt)
                bg_common_cnt = len(bgrm_rdict[bigram1] & bgrm_rdict[bigram2])

                if bg_common_cnt / bg12_min_cnt < review_pct:
                    continue

                trigram = ' '.join([split1[0], split1[1], split2[1]])
                trigrams.add(trigram)
                self._find_idx(corpus, bigram1, bigram2, trigram)

        return trigrams
