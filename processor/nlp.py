import re
import simplejson
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from common import config


log = config.getLogger(__name__)


def computeGiniIndexScore(filename):
    """
    Compute Gini index score for every feature in TFIDF matrix.
    :param filename: TFIDF filename.
    """

    log.info('Compute GINI index score ...')
    datasetName = re.match('(.*)_nouns_tfidf.csv', filename).group(1)
    df = pd.read_csv(config.processedDataPath + filename)
    shape = df.shape
    log.info('TFIDF file {} has {} reviews and {} features.'.format(filename, shape[0], shape[1] - 1))
    labelName = 'posneg'
    labelPositive = 1
    labelNegative = 0

    result = [('feature', 'gini', 'posneg', 'num_pos', 'num_neg', 'total')]
    for column in df:
        if column == labelName:
            continue
        idx_app = df.index[df[column] > 0]  # reviews' Id that a particular word appears
        idx_napp = df.index[~df.index.isin(idx_app)]  # reviews' Id that a particular word doesn't appear
        assert shape[0] == (len(idx_app) + len(idx_napp)), 'appear_index + not_appear_index != num_reviews'

        num_pos_app, num_neg_app = 0, 0  # number of positive/negative reviews that a particular word appears
        num_pos_napp, num_neg_napp = 0, 0  # number of positive/negative reviews that a particular word doesn't appear

        for label in (df.loc[idx_app, labelName] == labelPositive):
            if label:
                num_pos_app += 1
            else:
                num_neg_app += 1
        for label in (df.loc[idx_napp, labelName] == labelNegative):
            if label:
                num_pos_napp += 1
            else:
                num_neg_napp += 1
        assert num_pos_app + num_neg_app == len(idx_app), 'appear_pos + appear_neg != num_appear'
        assert num_pos_napp + num_neg_napp == len(idx_napp), 'not_appear_pos + not_appear_neg != num_not_appear'

        # gini index of appeared case
        if num_pos_app == 0 or num_neg_app == 0:
            gini_app = 1.0
        else:
            gini_app = (num_pos_app / len(idx_app)) ** 2 + (num_neg_app / len(idx_app)) ** 2

        # gini index of not appeared case
        if num_pos_napp == 0 or num_neg_napp == 0:
            gini_napp = 1.0
        else:
            gini_napp = (num_pos_napp / len(idx_napp)) ** 2 + (num_neg_napp / len(idx_napp)) ** 2

        posneg_app = 1 if num_pos_app > num_neg_app else (0 if num_neg_app > num_pos_app else 2)
        posneg_napp = 1 if num_pos_napp > num_neg_napp else (0 if num_neg_napp > num_pos_napp else 2)
        result.append((column + '_1', gini_app, posneg_app, num_pos_app, num_neg_app, len(idx_app)))
        result.append((column + '_0', gini_napp, posneg_napp, num_pos_napp, num_neg_napp, len(idx_napp)))

    filename = config.processedDataPath + datasetName + '_gini.csv'
    with open(filename, 'w') as fp:
        for res in result:
            fp.write(','.join(str(x) for x in res))
            fp.write('\n')
    log.info('Done! Gini index score is computed and saved in ' + filename)


def computeSentimentScoresForAllWords(datasetName, cleanedContents, taggedWords, posneg, stopwords):
    """
    Compute sentiment scores for all words.
    """

    # 1) construct TFIDF matrix
    log.info('Constructing TFIDF matrix ... ')
    vectorizer = TfidfVectorizer(stop_words=list(stopwords))
    tfidf = vectorizer.fit_transform(cleanedContents)
    df = pd.DataFrame(tfidf.toarray(), columns=vectorizer.get_feature_names())

    # append posneg column
    pn = pd.Series(posneg)
    df['posneg'] = pn.values

    # save DataFrame
    filename = config.processedDataPath + datasetName + 'raw_tfidf.csv'
    df.to_csv(filename, index=False)
    log.info('Done! TFIDF matrix of shape {} is constructed and saved in {}.'.format(df.shape, filename))

    # 2) multiply sentiment scores of words
    log.info('Multiplying sentiment scores to TFIDF matrix ... ')
    for feature in df:
        if feature in taggedWords:
            score = max(taggedWords[feature])
            if score == taggedWords[feature][0]:
                df[feature] *= score
            elif score == taggedWords[feature][1]:
                df[feature] *= -score
            else:
                df[feature] *= 0.0

    filename = config.processedDataPath + datasetName + 'raw_mul.csv'
    df.to_csv(filename, index=False)
    log.info('Done! TFIDF matrix of shape {} is constructed and saved in {}.'.format(df.shape, filename))

    # 3) construct sentiment scores for words
    log.info('Constructing pure sentiment scores ... ')
    pure_scores = []
    for content in cleanedContents:
        words = content.split()
        word_scores = {}
        for feature in df:
            if feature == 'posneg':
                continue

            if feature in taggedWords:
                score = max(taggedWords[feature])
                if score == taggedWords[feature][0]:
                    word_scores[feature] = score
                elif score == taggedWords[feature][1]:
                    word_scores[feature] = -score
                else:
                    word_scores[feature] = 0.0
            else:
                word_scores[feature] = 0.0
        pure_scores.append(word_scores)
    df = pd.DataFrame(pure_scores)
    df['posneg'] = pn.values
    filename = config.processedDataPath + datasetName + 'raw_sc.csv'
    df.to_csv(filename, index=False)
    log.info('Done! TFIDF matrix of shape {} is constructed and saved in {}.'.format(df.shape, filename))


def computeSentimentScoresForNouns(datasetName, cleanedContents, taggedWords, posneg, stopwords):
    """
    Compute sentiment scores for nouns only.
    """

    # 1) get scores of nouns
    log.info('Extracting nouns from review contents ... ')
    noun_scores = _getNounScores(cleanedContents, taggedWords)
    filename = config.processedDataPath + datasetName + 'sentiment_scores_nouns.json'
    with open(filename, 'w') as fp:
        simplejson.dump(noun_scores, fp)
    assert len(noun_scores) == len(cleanedContents)
    log.info('Done! {} reviews are processed. Sentiment scores of nouns are saved in {}.'.format(
        len(cleanedContents), filename))

    # 1) construct TFIDF matrix
    log.info('Constructing TFIDF matrix ... ')
    # tfidf = TfidfVectorizer(tokenizer=tokenize_paragraph, stop_words=list(stopwords))
    #     noun_scores_tfidf = tfidf.fit_transform([",".join(list(n.keys())) for n in noun_scores])
    vectorizer = TfidfVectorizer(stop_words=list(stopwords))
    tfidf = vectorizer.fit_transform(cleanedContents)
    df = pd.DataFrame(tfidf.toarray(), columns=vectorizer.get_feature_names())
    log.info('Done! TFIDF matrix of shape {} is constructed.'.format(df.shape))

    # drop columns not in noun_scores
    log.info('Dropping columns not in noun scores ... ')
    df_words = [x[0] for x in list(df.iteritems())]
    log.debug('DataFrame has %d words.' % len(df_words))
    # print(df_words)

    noun_set = set([x for d in noun_scores for x in list(d.keys())])
    log.debug('Noun_set has %d nouns.' % len(noun_set))
    # print(noun_set)

    not_in = [x for x in df_words if x not in noun_set]
    df = df.drop(not_in, axis=1)
    log.info('Done! {} words are dropped.'.format(len(not_in)))

    # add posneg column
    pn = pd.Series(posneg)
    df['posneg'] = pn.values

    log.info('Exporting DataFrame to CSV file ... ')
    filename = config.processedDataPath + datasetName + 'nouns_tfidf.csv'
    df.to_csv(filename, index=False)
    log.info('Done! TFIDF matrix of shape {} is constructed and saved in {}.'.format(df.shape, filename))

    log.info('Multiplying sentiment score to TFIDF value ... ')
    assert len(noun_scores) == df.shape[0]
    for i in range(df.shape[0]):
        for noun, score in noun_scores[i].items():
            df.loc[i, noun] *= score
    filename = config.processedDataPath + datasetName + 'nouns_mul.csv'
    df.to_csv(filename, index=False)
    log.info('Done! TFIDF matrix of shape {} is multiplied and saved in {}.'.format(df.shape, filename))

    # construct sentiment scores for nouns
    log.info('Constructing pure sentiment scores for nouns ... ')
    pure_scores = []

    for i in range(len(cleanedContents)):
        content = cleanedContents[i]
        words = content.split()
        ns = {}
        for feature in df:
            if feature == 'posneg':
                continue

            if feature in list(noun_scores[i].keys()):
                ns[feature] = noun_scores[i][feature]
            else:
                ns[feature] = 0.0
        pure_scores.append(ns)
    df = pd.DataFrame(pure_scores)
    df['posneg'] = pn.values
    filename = config.processedDataPath + datasetName + 'nouns_sc.csv'
    df.to_csv(filename, index=False)
    log.info('Done! TFIDF matrix of shape {} is constructed and saved in {}.'.format(df.shape, filename))


def computeSentimentScoresAfterTransformingObjectiveWords(datasetName, cleanedContents, lemContents, taggedWords,
                                                          posneg, stopwords):
    """
    Compute sentiment scores based on transformed objective words.
    """

    log.info('Constructing TFIDF matrix ... ')
    # tfidf = TfidfVectorizer(tokenizer=tokenize_paragraph, stop_words=list(stopwords))
    vectorizer = TfidfVectorizer(stop_words=list(stopwords))
    tfidf = vectorizer.fit_transform(cleanedContents)
    df = pd.DataFrame(tfidf.toarray(), columns=vectorizer.get_feature_names())

    # add posneg column
    pn = pd.Series(posneg)
    df['posneg'] = pn.values

    filename = config.processedDataPath + datasetName + 'all_tfidf.csv'
    df.to_csv(filename, index=False)
    #     feature_names = tfidf.get_feature_names()
    #     feature_names_dict = {}
    #     for index, feature in enumerate(feature_names):
    #         feature_names_dict[feature] = index
    log.info('Done! TFIDF matrix of shape {} is constructed and saved in {}.'.format(df.shape, filename))

    log.info('Determining sentiment orientation and value for every sentence ... ')
    thresholdS = 0.1
    negative_modifier = {'no', 'not', 'but', 'however'}
    sentence_sent = []
    for content in lemContents:
        for sent in [s.strip() for s in content.split('###') if len(s) > 0]:
            tokens = sent.split()
            sentiS = 0.0
            posS, negS = 0.0, 0.0
            num_words, has_neg = 0, False
            for token in tokens:
                try:
                    posS += taggedWords[token][0]
                    negS += taggedWords[token][1]
                    num_words += 1
                except KeyError:
                    pass

                if token in negative_modifier:
                    has_neg = True
            #             print("pos: {}, neg: {}, n: {}".format(posS, negS, num_words))

            if num_words > 0 and abs(posS - negS) / num_words >= thresholdS:
                sentiS = posS if posS > negS else (negS * -1.0 if negS > posS else 0.0)
            else:
                sentiS = 0.0

            # negation
            if has_neg:
                sentiS *= -1

            sentence_sent.append(sentiS)
    log.info('Done! {} sentences are processed.'.format(len(sentence_sent)))

    log.info('Determining revised sentiment score for objective words ... ')
    thresholdW = 0.5
    obj_words = {}

    for content in lemContents:
        for index, sent in enumerate([s.strip() for s in content.split('###') if len(s) > 0]):
            tokens = sent.split()
            for token in tokens:
                try:
                    # only revise objective words
                    if max(taggedWords[token]) == taggedWords[token][2]:
                        ps, ns, os = obj_words.get(token, (0, 0, 0))
                        if sentence_sent[index] > 0.0:
                            ps += 1
                        elif sentence_sent[index] < 0.0:
                            ns += 1
                        else:
                            os += 1
                        obj_words[token] = (ps, ns, os)
                except KeyError:
                    pass

    for word in obj_words:
        ps, ns, os = obj_words[word]
        s = sum(obj_words[word])
        obj_words[word] = 0.0

        if max([ps, ns]) / s > thresholdW:
            pr_pos = ps / s
            pr_neg = ns / s
            obj_words[word] = pr_pos if pr_pos > pr_neg else (-1 * pr_neg if pr_neg > pr_pos else 0.0)
    for word in obj_words:
        df[word] *= obj_words[word]
    filename = config.processedDataPath + datasetName + 'all_mul.csv'
    df.to_csv(filename, index=False)
    log.info('Done! TFIDF matrix of shape {} is constructed and saved in {}.'.format(df.shape, filename))

    # construct sentiment scores
    log.info('Constructing pure sentiment scores ... ')
    pure_scores = []
    for _ in cleanedContents:
        word_scores = {}
        for feature in df:
            if feature == 'posneg':
                continue

            if feature in taggedWords:
                score = max(taggedWords[feature])
                if score == taggedWords[feature][0]:
                    word_scores[feature] = score
                elif score == taggedWords[feature][1]:
                    word_scores[feature] = -score
                else:
                    word_scores[feature] = obj_words.get(feature, 0.0)
            else:
                word_scores[feature] = 0.0
        pure_scores.append(word_scores)

    df = pd.DataFrame(pure_scores)
    df['posneg'] = pn.values
    filename = config.processedDataPath + datasetName + 'all_sc.csv'
    df.to_csv(filename, index=False)
    log.info('Done! TFIDF matrix of shape {} is constructed and saved in {}.'.format(df.shape, filename))


def _getNounScores(contents: list, taggedWords: list) -> list:
    """
    Get averaged scores for nouns in reviews.
    :param contents: list of reviews.
    :param taggedWords: a list of {word: (POS, pos_score, neg_score, obj_score)} mapping.
    :return: a list of dictionary mapping nouns to their averaged scores.
    """

    result = []
    for content in contents:
        tokens = content.split()
        nounScore = _averageNounScores(tokens, taggedWords)
        result.append(nounScore)
    return result


def _averageNounScores(tokens: list, taggedWords: list) -> dict:
    """
    Get averaged scores of nouns in tokens.
    :param tokens: a list of words.
    :param taggedWords: a list of {word_POS: (pos_score, neg_score, obj_score)} mapping.
    :return {noun: averaged_score mapping.
    """

    nouns = []
    total_score = 0.0
    counts = 0
    nouns_count = {}
    nouns_score = {}

    # compute scores of nouns
    for token in [t for t in tokens if t in taggedWords]:
        word_prop = taggedWords[token]  # word properties: [pos_score, neg_score, obj_score]
        pos = token[-1]  # tokens are in format: book_n, eat_v, ...
        score = 0.0
        if pos != 'n':  # only score of non-noun words are needed
            try:
                max_score = max(word_prop)
                if max_score == word_prop[0]:
                    score = word_prop[0]
                    counts += 1
                elif max_score == word_prop[1]:
                    score = -word_prop[1]
                    counts += 1
                total_score += score
            except:
                pass
        else:  # for nouns, add it to result list
            nouns.append([token, 0])
    avg_score = (total_score / counts) if counts > 0 else 0.0
    for noun in nouns:
        noun[1] = avg_score

    # take average of noun scores
    for noun, score in nouns:
        nouns_count[noun] = nouns_count.get(noun, 0) + 1
        nouns_score[noun] = nouns_score.get(noun, 0.0) + score
    for noun, score in nouns_score.items():
        nouns_score[noun] = score / nouns_count[noun]

    return nouns_score
