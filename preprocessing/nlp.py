import os
import re
import logging
import glob
import simplejson as json
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from preprocessing.lemmatization import LemmatizationMode


def process_all(stopwords: set, mode: LemmatizationMode, store: str, working_dir: str, include_index: bool, **context):
    """
    Compute sentiment scores for all words.
    """

    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date

    input_files = context['task_instance'].xcom_pull(task_ids='over_sampling__' + store)['output_files']
    logging.info('Input files=' + str(input_files))

    score_file = context['task_instance'].xcom_pull(
        task_ids='score__{}_{}'.format(store, str(mode)))['output_files']['scored']
    with open(working_dir + os.path.sep + score_file) as fp:
        word2score = json.load(fp)
    logging.info('Score file=' + score_file)

    cleaned_file = context['task_instance'].xcom_pull(
        task_ids='clean__{}_{}'.format(store, str(mode)))['output_files'][0]
    with open(working_dir + os.path.sep + cleaned_file) as fp:
        reviews = json.load(fp)
    logging.info('Cleaned file=' + cleaned_file)

    # wd = os.getcwd()
    # os.chdir(working_dir)
    # word2score_file = store + '__scored__' + str(mode) + '.json'
    # with open(word2score_file) as fp:
    #     word2score = json.load(fp)
    # review_file = store + '__cleaned__' + str(mode) + '.json'
    # with open(review_file) as fp:
    #     reviews = json.load(fp)
    # rating_files = glob.glob(store + '__split__*.csv')
    # regex = r'.+__split__(?P<score>\w+)__(?P<range>[\w\d\-.]+)\.csv'
    regex = r'.+__(?P<label>[\w_]+)\.csv'
    output_files = []
    for file in input_files:
        # score_name = re.match(regex, file).group('score')
        # range_kv = re.match(regex, file).group('range')
        label = re.match(regex, file).group('label')
        df = pd.read_csv(working_dir + os.path.sep + file)
        for pid, text in [(x['pid'], x['text']) for x in reviews]:
            df.loc[df['PID'] == pid, 'BEFORE_REVIEW'] = text
        file_name_pattern = os.path.sep.join(['nlp', '{}_{}_{}_'.format(store, str(mode), label)])
        if not os.path.exists(os.path.sep.join([working_dir, 'nlp'])):
            os.makedirs(os.path.sep.join([working_dir, 'nlp']))
        # + str(mode) + '_' + score_name + '_' + range_kv + '.csv'
        results = _helper_all_words(cleaned_texts=df['BEFORE_REVIEW'].tolist(), labels=df[label].tolist(),
                                    label_name=label, word2score=word2score, stopwords=stopwords,
                                    file_name_pattern=working_dir + os.path.sep + file_name_pattern + '{}.csv',
                                    include_index=include_index)
        output_files += results
    return {
        'input_files': input_files,
        'output_files': [os.path.sep.join(['nlp', filename]) for filename in output_files],
        'nlp': 'process_all'
    }
    # os.chdir(wd)


def process_nouns(stopwords: set, mode: LemmatizationMode, store: str, working_dir: str, include_index: bool, **context):
    """
    Compute sentiment scores for nouns only.
    """

    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date

    input_files = context['task_instance'].xcom_pull(task_ids='over_sampling__' + store)['output_files']
    logging.info('Input files=' + str(input_files))

    score_file = context['task_instance'].xcom_pull(
        task_ids='score__{}_{}'.format(store, str(mode)))['output_files']['scored']
    with open(working_dir + os.path.sep + score_file) as fp:
        word2score = json.load(fp)
    logging.info('Score file=' + score_file)

    cleaned_file = context['task_instance'].xcom_pull(
        task_ids='clean__{}_{}'.format(store, str(mode)))['output_files'][0]
    with open(working_dir + os.path.sep + cleaned_file) as fp:
        reviews = json.load(fp)
    logging.info('Cleaned file=' + cleaned_file)

    # wd = os.getcwd()
    # os.chdir(working_dir)
    # word2score_file = store + '__scored__' + str(mode) + '.json'
    # with open(word2score_file) as fp:
    #     word2score = json.load(fp)
    # review_file = store + '__cleaned__' + str(mode) + '.json'
    # with open(review_file) as fp:
    #     reviews = json.load(fp)
    # rating_files = glob.glob(store + '__split__*.csv')
    # regex = r'.+__split__(?P<score>\w+)__(?P<range>[\w\d\-.]+)\.csv'
    regex = r'.+__(?P<label>[\w_]+)\.csv'
    output_files = []
    for file in input_files:
        # score_name = re.match(regex, file).group('score')
        # range_kv = re.match(regex, file).group('range')
        label = re.match(regex, file).group('label')
        df = pd.read_csv(working_dir + os.path.sep + file)
        for pid, text in [(x['pid'], x['text']) for x in reviews]:
            df.loc[df['PID'] == pid, 'BEFORE_REVIEW'] = text
        file_name_pattern = os.path.sep.join(['nlp', '{}_{}_{}_'.format(store, str(mode), label)])
        if not os.path.exists(os.path.sep.join([working_dir, 'nlp'])):
            os.makedirs(os.path.sep.join([working_dir, 'nlp']))
            # + str(mode) + '_' + score_name + '_' + range_kv + '.csv'
        results = _helper_nouns(cleaned_texts=df['BEFORE_REVIEW'].tolist(), labels=df[label].tolist(),
                                label_name=label, word2score=word2score, stopwords=stopwords,
                                file_name_pattern=working_dir + os.path.sep + file_name_pattern + '{}.csv',
                                include_index=include_index)
        output_files += results
    return {
        'input_files': input_files,
        'output_files': [os.path.sep.join(['nlp', filename]) for filename in output_files],
        'nlp': 'process_nouns'
    }
    # os.chdir(wd)


def transform_objective(stopwords: set, mode: LemmatizationMode, sep: str, store: str, working_dir: str,
                        include_index: bool, **context):
    """
    Compute sentiment scores based on transformed objective words.
    """

    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date

    input_files = context['task_instance'].xcom_pull(task_ids='over_sampling__' + store)['output_files']
    logging.info('Input files=' + str(input_files))

    score_file = context['task_instance'].xcom_pull(
        task_ids='score__{}_{}'.format(store, str(mode)))['output_files']['scored']
    with open(working_dir + os.path.sep + score_file) as fp:
        word2score = json.load(fp)
    logging.info('Score file=' + score_file)

    cleaned_file = context['task_instance'].xcom_pull(
        task_ids='clean__{}_{}'.format(store, str(mode)))['output_files'][0]
    with open(working_dir + os.path.sep + cleaned_file) as fp:
        reviews = json.load(fp)
    logging.info('Cleaned file=' + cleaned_file)

    lemmatized_file = context['task_instance'].xcom_pull(
        task_ids='lemmatize__{}_{}'.format(store, str(mode)))['output_files'][0]
    with open(working_dir + os.path.sep + lemmatized_file) as fp:
        lemmatized = json.load(fp)
    logging.info('Lemmatized file=' + lemmatized_file)

    # wd = os.getcwd()
    # os.chdir(working_dir)
    # word2score_file = store + '__scored__' + str(mode) + '.json'
    # with open(word2score_file) as fp:
    #     word2score = json.load(fp)
    # cleaned_file = store + '__cleaned__' + str(mode) + '.json'
    # with open(cleaned_file) as fp:
    #     cleaned_reviews = json.load(fp)
    # lemmatized_file = store + '__lemmatized__' + str(mode) + '.json'
    # with open(lemmatized_file) as fp:
    #     lemmatized_reviews = json.load(fp)
    # rating_files = glob.glob(store + '__split__*.csv')
    # regex = r'.+__split__(?P<score>\w+)__(?P<range>[\w\d\-.]+)\.csv'
    regex = r'.+__(?P<label>[\w_]+)\.csv'
    output_files = []
    for file in input_files:
        # score_name = re.match(regex, file).group('score')
        # range_kv = re.match(regex, file).group('range')
        label = re.match(regex, file).group('label')
        df = pd.read_csv(working_dir + os.path.sep + file)
        for pid, text in [(x['pid'], x['text']) for x in reviews]:
            df.loc[df['PID'] == pid, 'BEFORE_REVIEW'] = text
        for pid, text in [(x['pid'], x['text']) for x in lemmatized]:
            df.loc[df['PID'] == pid, 'LEMMATIZED_REVIEW'] = text
        file_name_pattern = os.path.sep.join(['nlp', '{}_{}_{}_'.format(store, str(mode), label)])
        if not os.path.exists(os.path.sep.join([working_dir, 'nlp'])):
            os.makedirs(os.path.sep.join([working_dir, 'nlp']))
            # + str(mode) + '_' + score_name + '_' + range_kv + '.csv'
        results = _helper_transform_objective(cleaned_texts=df['BEFORE_REVIEW'].tolist(),
                                              lemmatized_texts=df['LEMMATIZED_REVIEW'].tolist(),
                                              labels=df[label].tolist(), label_name=label,
                                              word2score=word2score, stopwords=stopwords, sep=sep,
                                              file_name_pattern=working_dir + os.path.sep + file_name_pattern + '{}.csv',
                                              include_index=include_index)
        output_files += results
    return {
        'input_files': input_files,
        'output_files': [os.path.sep.join(['nlp', filename]) for filename in output_files],
        'nlp': 'transform_objective'
    }
    # os.chdir(wd)


def _helper_all_words(cleaned_texts: list, labels: list, label_name: str, word2score: dict,
                      stopwords: set, file_name_pattern: str, include_index: bool):
    output_files = []

    # 1) construct TFIDF matrix
    vectorizer = TfidfVectorizer(stop_words=list(stopwords))
    tfidf = vectorizer.fit_transform(cleaned_texts)
    df = pd.DataFrame(tfidf.toarray(), columns=vectorizer.get_feature_names())
    labels = pd.Series(labels)
    df[label_name] = labels
    filename = file_name_pattern.format('raw_tfidf')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])

    # 2) multiply sentiment scores of words
    for feature in df:
        if feature in word2score:
            score = max(word2score[feature])
            if score == word2score[feature][0]:
                df[feature] *= score
            elif score == word2score[feature][1]:
                df[feature] *= -score
            else:
                df[feature] *= 0.0
    filename = file_name_pattern.format('raw_mul')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])

    # 3) construct sentiment scores for words
    pure_scores = []
    for _ in cleaned_texts:
        word_scores = {}
        for feature in df:
            if feature == label_name:
                continue
            if feature in word2score:
                score = max(word2score[feature])
                if score == word2score[feature][0]:
                    word_scores[feature] = score
                elif score == word2score[feature][1]:
                    word_scores[feature] = -score
                else:
                    word_scores[feature] = 0.0
            else:
                word_scores[feature] = 0.0
        pure_scores.append(word_scores)
    df = pd.DataFrame(pure_scores)
    df[label_name] = labels
    filename = file_name_pattern.format('raw_sc')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])
    return output_files


def _helper_nouns(cleaned_texts: list, labels: list, label_name: str, word2score: dict, stopwords: set,
                  file_name_pattern: str, include_index: bool):
    output_files = []

    # 1) get scores of nouns
    noun_scores = _get_noun_scores(cleaned_texts, word2score)
    # filename = working_dir + os.path.sep + store + '__sentiment_scores_nouns__' + mode + '.json'
    # with open(filename, 'w') as fp:
    #     json.dump(noun_scores, fp)
    # assert len(noun_scores) == len(cleaned_texts)

    # 2) construct TFIDF matrix
    # tfidf = TfidfVectorizer(tokenizer=tokenize_paragraph, stop_words=list(stopwords))
    # noun_scores_tfidf = tfidf.fit_transform([",".join(list(n.keys())) for n in noun_scores])
    vectorizer = TfidfVectorizer(stop_words=list(stopwords))
    tfidf = vectorizer.fit_transform(cleaned_texts)
    df = pd.DataFrame(tfidf.toarray(), columns=vectorizer.get_feature_names())
    # drop columns not in noun_scores
    df_words = [x[0] for x in list(df.iteritems())]
    noun_set = set([x for d in noun_scores for x in list(d.keys())])
    not_in = [x for x in df_words if x not in noun_set]
    df = df.drop(not_in, axis=1)
    logging.info('{} non-noun words are dropped.'.format(len(not_in)))
    labels = pd.Series(labels)
    df[label_name] = labels
    filename = file_name_pattern.format('nouns_tfidf')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])

    # 3) multiply sentiment scores of words
    assert len(noun_scores) == df.shape[0]
    for i in range(df.shape[0]):
        for noun, score in noun_scores[i].items():
            df.loc[i, noun] *= score
    filename = file_name_pattern.format('nouns_mul')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])

    # 4) construct sentiment scores for nouns
    pure_scores = []
    for i in range(len(cleaned_texts)):
        ns = {}
        for feature in df:
            if feature == label_name:
                continue
            if feature in list(noun_scores[i].keys()):
                ns[feature] = noun_scores[i][feature]
            else:
                ns[feature] = 0.0
        pure_scores.append(ns)
    df = pd.DataFrame(pure_scores)
    df[label_name] = labels
    filename = file_name_pattern.format('nouns_sc')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])
    return output_files


def _helper_transform_objective(cleaned_texts: list, lemmatized_texts: list, labels: list, label_name: str,
                                word2score: dict, stopwords: set, sep: str, file_name_pattern: str,
                                include_index: bool):
    output_files = []
    # 1) construct TFIDF matrix
    # tfidf = TfidfVectorizer(tokenizer=tokenize_paragraph, stop_words=list(stopwords))
    vectorizer = TfidfVectorizer(stop_words=list(stopwords))
    tfidf = vectorizer.fit_transform(cleaned_texts)
    df = pd.DataFrame(tfidf.toarray(), columns=vectorizer.get_feature_names())
    labels = pd.Series(labels)
    df[label_name] = labels

    filename = file_name_pattern.format('obj_tfidf')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])
    #     feature_names = tfidf.get_feature_names()
    #     feature_names_dict = {}
    #     for index, feature in enumerate(feature_names):
    #         feature_names_dict[feature] = index

    # 2) determine sentiment orientation and value for every sentence
    thresholdS = 0.1
    negative_modifier = {'no', 'not', 'but', 'however'}
    sentence_sent = []
    for text in lemmatized_texts:
        for sent in [s.strip() for s in text.split(sep) if len(s) > 0]:
            tokens = sent.split()
            posS, negS = 0.0, 0.0
            num_words, has_neg = 0, False
            for token in tokens:
                posS += word2score.get(token, (0.0, 0.0))[0]
                negS += word2score.get(token, (0.0, 0.0))[1]
                num_words += 1
                if token in negative_modifier:
                    has_neg = True
            if num_words > 0 and abs(posS - negS) / num_words >= thresholdS:
                sentiS = posS if posS > negS else (negS * -1.0 if negS > posS else 0.0)
            else:
                sentiS = 0.0
            if has_neg:
                sentiS *= -1
            sentence_sent.append(sentiS)
    logging.info('{} sentences are processed.'.format(len(sentence_sent)))

    # 3) determine modified sentiment score for objective words
    thresholdW = 0.5
    obj_words = {}
    for text in lemmatized_texts:
        for index, sent in enumerate([s.strip() for s in text.split(sep) if len(s) > 0]):
            tokens = sent.split()
            for token in tokens:
                try:
                    # modify only objective words
                    if max(word2score[token]) == word2score[token][2]:
                        _ps, _ns, _os = obj_words.get(token, (0, 0, 0))
                        if sentence_sent[index] > 0.0:
                            _ps += 1
                        elif sentence_sent[index] < 0.0:
                            _ns += 1
                        else:
                            _os += 1
                        obj_words[token] = (_ps, _ns, _os)
                except KeyError:
                    pass
    for word in obj_words:
        _ps, _ns, _os = obj_words[word]
        s = sum(obj_words[word])
        obj_words[word] = 0.0

        if max([_ps, _ns]) / s > thresholdW:
            pr_pos = _ps / s
            pr_neg = _ns / s
            obj_words[word] = pr_pos if pr_pos > pr_neg else (-1 * pr_neg if pr_neg > pr_pos else 0.0)
    for word in obj_words:
        df[word] *= obj_words[word]

    filename = file_name_pattern.format('obj_mul')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])

    # 4) construct sentiment scores
    pure_scores = []
    for _ in cleaned_texts:
        word_scores = {}
        for feature in df:
            if feature == label_name:
                continue
            if feature in word2score:
                score = max(word2score[feature])
                if score == word2score[feature][0]:
                    word_scores[feature] = score
                elif score == word2score[feature][1]:
                    word_scores[feature] = -score
                else:
                    word_scores[feature] = obj_words.get(feature, 0.0)
            else:
                word_scores[feature] = 0.0
        pure_scores.append(word_scores)
    df = pd.DataFrame(pure_scores)
    df[label_name] = labels
    filename = file_name_pattern.format('obj_sc')
    df.to_csv(filename, index=include_index)
    output_files.append(filename[filename.rindex(os.path.sep) + 1:])
    return output_files


def _get_noun_scores(cleaned_texts: list, word2score: dict) -> list:
    """
    Get averaged scores for nouns in reviews.
    :param cleaned_texts: list of reviews.
    :param word2score: word to (pos_score, neg_score, obj_score) mapping.
    :return: a list of dict which maps nouns to their averaged scores.
    """

    result = []
    for text in cleaned_texts:
        tokens = text.split()
        nounScore = _average_noun_scores(tokens, word2score)
        result.append(nounScore)
    return result


def _average_noun_scores(tokens: list, word2score: dict) -> dict:
    """
    Get averaged scores of nouns in tokens.
    :param tokens: a list of words.
    :param word2score: word to (pos_score, neg_score, obj_score) mapping.
    :return {noun: averaged_score} mapping.
    """

    nouns = []
    total_score = 0.0
    counts = 0
    nouns_count = {}
    nouns_score = {}

    # compute scores of nouns
    for token in [t for t in tokens if t in word2score]:
        word_prop = word2score[token]  # word properties: [pos_score, neg_score, obj_score]
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
