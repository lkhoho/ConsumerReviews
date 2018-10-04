import os
import re
import enum
import logging
import simplejson as json
import nltk
from nltk.corpus import sentiwordnet as swn
import pandas as pd


@enum.unique
class LemmatizationMode(enum.Enum):
    """
    Lemmatizer modes.
    """

    PARAGRAPH = 'paragraph'
    VOCABULARY = 'vocabulary'

    def __str__(self):
        return self.value


def lemmatize(lemmatization_config, mode: LemmatizationMode,
              stopwords: set, store: str, working_dir: str, **context):
    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    tagger = nltk.tag.stanford.StanfordPOSTagger(lemmatization_config.pos_tagger['model'],
                                                 path_to_jar=lemmatization_config.pos_tagger['jar'])
    input_file = context['task_instance'].xcom_pull(task_ids='fetch__' + store)['output_files'][0]
    logging.info('Lemmatize file={} with mode={}'.format(input_file, str(mode)))
    df = pd.read_csv(working_dir + os.path.sep + input_file)
    if mode is LemmatizationMode.PARAGRAPH:
        fn = _lem_by_para
    elif mode is LemmatizationMode.VOCABULARY:
        fn = _lem_by_voc
    else:
        logging.error('Unknown lemmatization type: ' + str(mode))
        return
    result = fn(pids=df['PID'].astype('int'), texts=df['BEFORE_REVIEW'].astype('str'), lem_config=lemmatization_config,
                tagger=tagger, stopwords=stopwords)
    dir_path = os.path.sep.join([working_dir, 'lemmatized_and_scored'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_name = store + '__' + str(mode) + '.json'
    with open(os.path.sep.join([dir_path, file_name]), 'w') as fp:
        json.dump(result, fp)
    return {
        'input_files': [input_file],
        'output_files': [os.path.sep.join(['lemmatized_and_scored', file_name])]
    }


def _lem_by_voc(pids: list, texts: list, lem_config, tagger, stopwords: set):
    result = []
    vocabulary = set()
    texts_lower = [text.strip().lower() for text in texts]

    for text in texts_lower:
        for sent in nltk.sent_tokenize(text):
            tokens = [x for x in re.findall(r'[a-zA-Z]+', sent) if len(x) > 1]
            tokens = set(filter(lambda x: x not in stopwords, tokens))
            vocabulary |= tokens
    tagged_voc = {_word: _pos for (_word, _pos) in tagger.tag(list(vocabulary))}

    for pid, text in zip(pids, texts_lower):
        words = []
        for sent in nltk.sent_tokenize(text):
            tokens = [x for x in re.findall(r'[a-zA-Z]+', sent) if len(x) > 1]
            tokens = list(filter(lambda x: x not in stopwords, tokens))
            tokens.append(lem_config.sentence_sep)
            for word in tokens:
                pos = lem_config.pos_mapping.get(tagged_voc.get(word), lem_config.invalid_pos)
                if word in tagged_voc and pos != lem_config.invalid_pos:
                    word = lem_config.lemmatizer.lemmatize(word, pos=pos)
                    words.append(word + '_' + pos)
                elif word == lem_config.sentence_sep:
                    words.append(lem_config.sentence_sep)
        result.append({'pid': pid, 'text': ' '.join(words)})
    return result


def _lem_by_para(pids: list, texts: list, lem_config, tagger, stopwords, verbose=500):
    result = []
    for pid, text in zip(pids, texts):
        if pid % verbose == 0:
            logging.info('Lemmatize text {}/{} by paragraph.'.format(pid, len(texts)))
        text = text.strip().lower()
        sentences = nltk.sent_tokenize(text)
        words = []
        for sent in sentences:
            words += [x for x in sent.split() if x not in stopwords and len(x) > 1]
            words.append(lem_config.sentence_sep)
        text = [x for x in words if x not in stopwords and len(x) > 1]
        tagged = [x for x in tagger.tag(text) if len(x[0]) > 1]
        words = []
        for t in tagged:
            raw_words = re.findall(r'[a-zA-Z]+|#{3}', t[0])
            pos = lem_config.pos_mapping.get(t[1], lem_config.invalid_pos)
            if len(raw_words) > 0 and (raw_words[0] not in stopwords) and pos != lem_config.invalid_pos:
                word = raw_words[0]
                word = lem_config.lemmatizer.lemmatize(word, pos=pos)
                words.append(word + '_' + pos)
            elif len(raw_words) > 0 and raw_words[0] == lem_config.sentence_sep:
                words.append(lem_config.sentence_sep)
        result.append({'pid': pid, 'text': ' '.join(words)})
    return result


def compute_sentiment_score(sentence_sep, mode, store, working_dir, **context):
    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    input_file = context['task_instance'].xcom_pull(task_ids=context['task'].upstream_task_ids[-1])['output_files'][0]
    logging.info('Compute sentiment score on file=' + input_file)
    with open(working_dir + os.path.sep + input_file) as fp:
        json_file = json.load(fp)
    texts = [x['text'] for x in json_file]
    word_2_score, no_score = _sentiment_score_helper(texts, sentence_sep)
    dir_path = os.path.sep.join([working_dir, 'lemmatized_and_scored'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    score_file_name = store + '__scored__' + str(mode) + '.json'
    no_score_file_name = store + '__noscore__' + str(mode) + '.json'
    with open(os.path.sep.join([dir_path, score_file_name]), 'w') as fp:
        json.dump(word_2_score, fp)
    with open(os.path.sep.join([dir_path, no_score_file_name]), 'w') as fp:
        json.dump(no_score, fp)
    return {
        'input_files': [input_file],
        'output_files': {
            'scored': os.path.sep.join(['lemmatized_and_scored', score_file_name]),
            'noscore': os.path.sep.join(['lemmatized_and_scored', no_score_file_name])
        }
    }


def _sentiment_score_helper(texts, sentence_sep, batch_size=100):
    texts = [text.strip().lower() for text in texts]
    words = set()
    for text in texts:
        words |= set([x for x in text.split() if len(x) > 1 and not x.startswith(sentence_sep)])
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


def clean_undesired(sentence_sep, mode, store, working_dir, **context):
    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    input_file = context['task_instance'].xcom_pull(task_ids=context['task'].upstream_task_ids[-1])['output_files'][0]
    logging.info('Clean undesired terms from file=' + input_file)
    with open(working_dir + os.path.sep + input_file) as fp:
        json_file = json.load(fp)
    result = [{'pid': x['pid'],
               'text': ' '.join([w for w in x['text'].split() if not w.startswith(sentence_sep)])}
              for x in json_file]
    dir_path = os.path.sep.join([working_dir, 'lemmatized_and_scored'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_name = store + '__cleaned__' + str(mode) + '.json'
    with open(os.path.sep.join([dir_path, file_name]), 'w') as fp:
        json.dump(result, fp)
    return {
        'input_files': [input_file],
        'output_files': [os.path.sep.join(['lemmatized_and_scored', file_name])]
    }
