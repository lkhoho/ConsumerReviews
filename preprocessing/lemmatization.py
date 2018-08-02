import os
import re
import enum
import simplejson as json
import nltk
import pandas as pd
from nltk.corpus import sentiwordnet as swn
from utils.logging import get_logger

log = get_logger(__name__)

sentence_sep = '###'
invalid_pos = 'x'
POS = {
    'JJ': 'a', 'JJR': 'a', 'JJS': 'a', 'NN': 'n', 'NNP': 'n', 'NNS': 'n', 'NNPS': 'n', 'RB': 'r',
    'RBR': 'r', 'RBS': 'r', 'VB': 'v', 'VBD': 'v', 'VBG': 'v', 'VBN': 'v', 'VBP': 'v'
}


@enum.unique
class LemmatizationMode(enum.Enum):
    """
    Lemmatizer modes.
    """

    PARAGRAPH = 'paragraph'
    VOCABULARY = 'vocabulary'

    def __str__(self):
        return self.value


def read_stopwords(file):
    result = set(nltk.corpus.stopwords.words('english'))
    with open(file) as fp:
        result |= {word.strip().lower for word in fp}
    return result


def lemmatize(tagger_config, part_of_speech, lemmatizer, mode, stopwords, store, working_dir, **kwargs):
    exec_date = kwargs['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    tagger = _get_tagger(tagger_config)

    if part_of_speech is None or part_of_speech.lower() == 'default':
        part_of_speech = POS

    if lemmatizer is None or lemmatizer.lower() == 'default':
        lemmatizer = nltk.WordNetLemmatizer()

    if stopwords is None or stopwords.lower() == 'default':
        path = os.path.sep.join([os.path.dirname(__file__), 'resource', 'stopwords.txt'])
        stopwords = read_stopwords(path)

    file_name = store + '__original__.csv'
    df = pd.read_csv(working_dir + os.path.sep + file_name)
    if mode is LemmatizationMode.PARAGRAPH:
        fn = _lem_by_para
    elif mode is LemmatizationMode.VOCABULARY:
        fn = _lem_by_voc
    else:
        log.error('Unknown lemmatization type: ' + str(mode))
        return
    result = fn(df['PID'].astype('int'), df['BEFORE_REVIEW'].astype('str'),
                tagger, part_of_speech, lemmatizer, stopwords)
    file_name = store + '__lemmatized__' + str(mode) + '.json'
    with open(working_dir + os.path.sep + file_name, 'w') as fp:
        json.dump(result, fp)


def _get_tagger(tagger_config):
    model = tagger_config['model']
    model_path = tagger_config['model_path'][model]
    jar_path = tagger_config['jar_path']
    return nltk.tag.stanford.StanfordPOSTagger(model_path, path_to_jar=jar_path)


def _lem_by_voc(pids, texts, tagger, part_of_speech, lem, stopwords):
    result = {}
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
            tokens.append(sentence_sep)
            for word in tokens:
                pos = part_of_speech.get(tagged_voc.get(word), invalid_pos)
                if word in tagged_voc and pos != invalid_pos:
                    word = lem.lemmatize(word, pos=pos)
                    words.append(word + '_' + pos)
                elif word == sentence_sep:
                    words.append(sentence_sep)
        result[pid] = (' '.join(words))
    return result


def _lem_by_para(pids, texts, tagger, part_of_speech, lem, stopwords, verbose=500):
    result = {}
    for pid, text in zip(pids, texts):
        if pid % verbose == 0:
            log.info('Lemmatize text {}/{} by paragraph.'.format(pid, len(texts)))
        text = text.strip().lower()
        sentences = nltk.sent_tokenize(text)
        words = []
        for sent in sentences:
            words += [x for x in sent.split() if x not in stopwords and len(x) > 1]
            words.append(sentence_sep)
        text = [x for x in words if x not in stopwords and len(x) > 1]
        tagged = [x for x in tagger.tag(text) if len(x[0]) > 1]
        words = []
        for t in tagged:
            raw_words = re.findall(r'[a-zA-Z]+|#{3}', t[0])
            pos = part_of_speech.get(t[1], invalid_pos)
            if len(raw_words) > 0 and (raw_words[0] not in stopwords) and pos != invalid_pos:
                word = raw_words[0]
                word = lem.lemmatize(word, pos=pos)
                words.append(word + '_' + pos)
            elif len(raw_words) > 0 and raw_words[0] == sentence_sep:
                words.append(sentence_sep)
        result[pid] = (' '.join(words))
    return result


def compute_sentiment_score(mode, store, working_dir, **kwargs):
    exec_date = kwargs['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    file_name = store + '__lemmatized__' + str(mode) + '.json'
    with open(working_dir + os.path.sep + file_name) as fp:
        json_file = json.load(fp)
    word_2_score, no_score = _sentiment_score_helper(json_file.values())
    score_file_name = store + '__scored__' + str(mode) + '.json'
    no_score_file_name = store + '__noscore__' + str(mode) + '.json'
    with open(working_dir + os.path.sep + score_file_name, 'w') as fp:
        json.dump(word_2_score, fp)
    with open(working_dir + os.path.sep + no_score_file_name, 'w') as fp:
        json.dump(no_score, fp)


def _sentiment_score_helper(texts, batch_size=100):
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


def clean_undesired(mode, store, working_dir, **kwargs):
    exec_date = kwargs['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    file_name = store + '__lemmatized__' + str(mode) + '.json'
    with open(working_dir + os.path.sep + file_name) as fp:
        json_file = json.load(fp)
    result = {pid: ' '.join([w for w in text.split() if not w.startswith(sentence_sep)])
              for (pid, text) in json_file.items()}
    file_name = store + '__cleaned__' + str(mode) + '.json'
    with open(working_dir + os.path.sep + file_name, 'w') as fp:
        json.dump(result, fp)
