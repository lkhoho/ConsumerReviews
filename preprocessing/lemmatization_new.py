import os
import enum
import logging
# import simplejson as json
import nltk
import emoji
import pandas as pd
from typing import Set, Optional
# from nltk.corpus import sentiwordnet as swn
from nltk.tokenize import regexp_tokenize
from airflow.models import Variable


@enum.unique
class LemmatizationMode(enum.Enum):
    """
    Lemmatizer modes.
    """

    PARAGRAPH = 'paragraph'
    VOCABULARY = 'vocabulary'

    def __str__(self):
        return self.value


def lemmatize(upstream_task: str, 
              text_field: str, 
              stopwords: Set[str], 
              config: dict,
              include_index=False, 
              **context):
    """
    Perform lemmatization of review texts.
    :param upstream_task: Upstream task ID.
    :param text_field: Name of field containing texts which are lemmatized.
    :param stopwords: A set of stopwords.
    :param config: Lemmatization configuration.
    :param include_index: Should include index when saving dataframe.
    :param context: Jinja template variables in Airflow.
    :return: Name of data file that lemmatized texts are added.
    """

    working_dir = Variable.get('working_dir')
    os.makedirs(working_dir, exist_ok=True)
    logging.info('Working dir=' + working_dir)

    # if upstream_task is None:
    #     logging.info('Data pathname=' + data_pathname)
    #     df = pd.read_csv(data_pathname)
    #     logging.info('Dataframe shape={}'.format(df.shape))
    #     pos = data_pathname.rfind(os.sep)
    #     filename = data_pathname[pos + 1:]
    # else:
    logging.info('Upstream task=' + upstream_task)
    filename = context['ti'].xcom_pull(task_ids=upstream_task)
    logging.info('Filename=' + filename)
    df = pd.read_csv(os.sep.join([working_dir, context['ds_nodash'], filename]))
    logging.info('Dataframe shape={}'.format(df.shape))

    mode = config['pos_tagger']['mode']

    if mode is not LemmatizationMode.VOCABULARY and mode is not LemmatizationMode.PARAGRAPH:
        logging.error('Unknown lemmatization type: ' + str(mode))
        return

    lemmatizer = nltk.WordNetLemmatizer()
    tagger = nltk.tag.stanford.StanfordPOSTagger(model_filename=config['pos_tagger']['model'],
                                                 path_to_jar=config['pos_tagger']['jar'],
                                                 java_options=config['pos_tagger']['java_options'])
    logging.info('File to lemmatize={} with mode={}'.format(filename, str(mode)))

    df = standardize_text(df, text_field=text_field, output_field='standardized_text')
    logging.info('Standardized shape={}'.format(df.shape))

    df = tokenize(df, text_field='standardized_text', output_field='tokens')
    df['tokens'] = df['tokens'].astype(str)
    logging.info('Tokenized shape={}'.format(df.shape))

    df = remove_words(df, text_field='tokens', stopwords=stopwords)
    pos_mapping = config['pos_tagger']['pos_mapping']

    if mode is LemmatizationMode.VOCABULARY:
        voc = compute_vocabulary(df, token_field='tokens')
        logging.info('Vocabulary size={}'.format(len(voc)))
        tagged_voc = {token: pos_mapping.get(pos, 'n')
                      for (token, pos) in tagger.tag(list(voc)) if len(token) > 1}
        df['lemmatized'] = df['tokens'].apply(
            lambda tokens: [lemmatizer.lemmatize(token, pos=tagged_voc[token]) + '_' + tagged_voc[token]
                            for token in tokens.split()]
        )
    else:
        df['lemmatized'] = df['tokens'].apply(
            lambda tokens: [lemmatizer.lemmatize(token, pos=pos) + '_' + pos
                            for token, pos in [(_token, config['pos_mapping'].get(_pos, config['invalid_pos']))
                                               for _token, _pos in tagger.tag(tokens.split()) if len(_token) > 1]]
        )

    pos = filename.rfind('.')
    filename = filename[:pos] + '__lem{}.csv'.format(mode)
    save_path = os.sep.join([working_dir, context['ds_nodash']])
    os.makedirs(save_path, exist_ok=True)
    df.to_csv(save_path + os.sep + filename, index=include_index)

    return filename


# def lemmatization_by_vocabulary(df: pd.DataFrame, vocabulary: Set[str], lem_config, tagger):
#     result = []
#     tagged_voc = {word: pos for (word, pos) in tagger.tag(list(vocabulary))}
#
#     for pid, text in zip(pids, texts_lower):
#         words = []
#         for sent in nltk.sent_tokenize(text):
#             tokens = [x for x in re.findall(r'[a-zA-Z]+', sent) if len(x) > 1]
#             tokens = list(filter(lambda x: x not in stopwords, tokens))
#             tokens.append(lem_config.sentence_sep)
#             for word in tokens:
#                 pos = lem_config.pos_mapping.get(tagged_voc.get(word), lem_config.invalid_pos)
#                 if word in tagged_voc and pos != lem_config.invalid_pos:
#                     word = lem_config.lemmatizer.lemmatize(word, pos=pos)
#                     words.append(word + '_' + pos)
#                 elif word == lem_config.sentence_sep:
#                     words.append(lem_config.sentence_sep)
#         result.append({'pid': pid, 'text': ' '.join(words)})
#     return result


# def lemmatization_by_paragraph(pids: list, texts: list, lem_config, tagger, stopwords, verbose=500):
#     result = []
#     for pid, text in zip(pids, texts):
#         if pid % verbose == 0:
#             logging.info('Lemmatize text {}/{} by paragraph.'.format(pid, len(texts)))
#         text = text.strip().lower()
#         sentences = nltk.sent_tokenize(text)
#         words = []
#         for sent in sentences:
#             words += [x for x in sent.split() if x not in stopwords and len(x) > 1]
#             words.append(lem_config.sentence_sep)
#         text = [x for x in words if x not in stopwords and len(x) > 1]
#         tagged = [x for x in tagger.tag(text) if len(x[0]) > 1]
#         words = []
#         for t in tagged:
#             raw_words = re.findall(r'[a-zA-Z]+|#{3}', t[0])
#             pos = lem_config.pos_mapping.get(t[1], lem_config.invalid_pos)
#             if len(raw_words) > 0 and (raw_words[0] not in stopwords) and pos != lem_config.invalid_pos:
#                 word = raw_words[0]
#                 word = lem_config.lemmatizer.lemmatize(word, pos=pos)
#                 words.append(word + '_' + pos)
#             elif len(raw_words) > 0 and raw_words[0] == lem_config.sentence_sep:
#                 words.append(lem_config.sentence_sep)
#         result.append({'pid': pid, 'text': ' '.join(words)})
#     return result


# def compute_sentiment_score(sentence_sep, mode, store, working_dir, **context):
#     exec_date = context['execution_date'].strftime('%Y%m%d')
#     working_dir += os.path.sep + exec_date
#     input_file = context['task_instance'].xcom_pull(task_ids='lemmatize__{}_{}'.format(store, str(mode)))['output_files'][0]
#     logging.info('Compute sentiment score on file=' + input_file)
#     with open(working_dir + os.path.sep + input_file) as fp:
#         json_file = json.load(fp)
#     texts = [x['text'] for x in json_file]
#     word_2_score, no_score = _sentiment_score_helper(texts, sentence_sep)
#     dir_path = os.path.sep.join([working_dir, 'lemmatized_and_scored'])
#     if not os.path.exists(dir_path):
#         os.makedirs(dir_path)
#     score_file_name = store + '__scored__' + str(mode) + '.json'
#     no_score_file_name = store + '__noscore__' + str(mode) + '.json'
#     with open(os.path.sep.join([dir_path, score_file_name]), 'w') as fp:
#         json.dump(word_2_score, fp)
#     with open(os.path.sep.join([dir_path, no_score_file_name]), 'w') as fp:
#         json.dump(no_score, fp)
#     return {
#         'input_files': [input_file],
#         'output_files': {
#             'scored': os.path.sep.join(['lemmatized_and_scored', score_file_name]),
#             'noscore': os.path.sep.join(['lemmatized_and_scored', no_score_file_name])
#         }
#     }
#
#
# def _sentiment_score_helper(texts, sentence_sep, batch_size=100):
#     texts = [text.strip().lower() for text in texts]
#     words = set()
#     for text in texts:
#         words |= set([x for x in text.split() if len(x) > 1 and not x.startswith(sentence_sep)])
#     result = {}
#     no_score_words = []
#     num_words = len(words)
#     num_chunks = (num_words + batch_size - 1) // batch_size
#     word_list = list(words)
#     for i in range(num_chunks):
#         start = i * batch_size
#         end = (i + 1) * batch_size if (i + 1) * batch_size < num_words else num_words
#         sublist = word_list[start:end]
#         for w in sublist:
#             word = w[:-2]
#             part = w[-1]
#             word_str = '{}.{}.01'.format(word, part)
#             try:
#                 res = swn.senti_synset(word_str)
#                 result[w] = (res.pos_score(), res.neg_score(), res.obj_score())
#             except:
#                 no_score_words.append(w)
#     return result, no_score_words
#
#
# def clean_undesired(sentence_sep, mode, store, working_dir, **context):
#     exec_date = context['execution_date'].strftime('%Y%m%d')
#     working_dir += os.path.sep + exec_date
#     input_file = context['task_instance'].xcom_pull(task_ids='lemmatize__{}_{}'.format(store, str(mode)))['output_files'][0]
#     logging.info('Clean undesired terms from file=' + input_file)
#     with open(working_dir + os.path.sep + input_file) as fp:
#         json_file = json.load(fp)
#     result = [{'pid': x['pid'],
#                'text': ' '.join([w for w in x['text'].split() if not w.startswith(sentence_sep)])}
#               for x in json_file]
#     dir_path = os.path.sep.join([working_dir, 'lemmatized_and_scored'])
#     if not os.path.exists(dir_path):
#         os.makedirs(dir_path)
#     file_name = store + '__cleaned__' + str(mode) + '.json'
#     with open(os.path.sep.join([dir_path, file_name]), 'w') as fp:
#         json.dump(result, fp)
#     return {
#         'input_files': [input_file],
#         'output_files': [os.path.sep.join(['lemmatized_and_scored', file_name])]
#     }


def standardize_text(df: pd.DataFrame,
                     text_field: str,
                     output_field: str) -> pd.DataFrame:
    """
    Remove irrelevant characters, URLs and convert all characters to lowercase for texts in dataframe.
    :param df: Dataframe that contains texts to be cleaned.
    :param text_field: Name of field that contains texts.
    :param output_field: Name of output text field.
    :return: A pandas dataframe with cleaned texts in either new column of replacing original texts.
    """

    df[output_field] = df[text_field].apply(
        lambda column: emoji.get_emoji_regexp().sub(u'', column)
    )

    df[output_field] = df[output_field].str.replace("'m", ' am')
    df[output_field] = df[output_field].str.replace("’m", ' am')
    df[output_field] = df[output_field].str.replace("´m", ' am')

    df[output_field] = df[output_field].str.replace("'ve", ' have')
    df[output_field] = df[output_field].str.replace("’ve", ' have')
    df[output_field] = df[output_field].str.replace("´ve", ' have')

    df[output_field] = df[output_field].str.replace("'d", ' would')
    df[output_field] = df[output_field].str.replace("’d", ' would')
    df[output_field] = df[output_field].str.replace("´d", ' would')

    df[output_field] = df[output_field].str.replace("n't", ' not')
    df[output_field] = df[output_field].str.replace("n’t", ' not')
    df[output_field] = df[output_field].str.replace("n´t", ' not')

    df[output_field] = df[output_field].str.replace("'ll", ' will')
    df[output_field] = df[output_field].str.replace("’ll", ' will')
    df[output_field] = df[output_field].str.replace("´ll", ' will')

    df[output_field] = df[output_field].str.replace("'s", ' is')
    df[output_field] = df[output_field].str.replace("’", ' is')
    df[output_field] = df[output_field].str.replace("´s", ' is')

    df[output_field] = df[output_field].str.replace('/', ' ')
    df[output_field] = df[output_field].str.replace('\.{2,}', '.')
    df[output_field] = df[output_field].str.replace('!{2,}', '!')
    df[output_field] = df[output_field].str.replace('\?{2,}', '?')
    df[output_field] = df[output_field].str.replace('€+', '')
    df[output_field] = df[output_field].str.replace('[0-9$&~\\()[\]{}<>%\'"“”‘’，;…+\-_=*]+', '')
    df[output_field] = df[output_field].str.replace(r'http\S+', '')
    df[output_field] = df[output_field].str.replace(r'http', '')
    df[output_field] = df[output_field].str.replace(r'@\S+', '')
    df[output_field] = df[output_field].str.replace(r'@', 'at')
    df[output_field] = df[output_field].str.lower()
    df[output_field] = df[output_field].astype(str)

    return df


def remove_words(df: pd.DataFrame,
                 text_field: str,
                 stopwords: Set[str],
                 short_words_size: int = 3,
                 output_field: Optional[str] = None) -> pd.DataFrame:
    """
    Remove stopwords and short words in texts in dataframe.
    :param df: Dataframe that contains stopwords to be removed.
    :param text_field: Name of field that contains texts.
    :param stopwords: A set of stopwords.
    :param short_words_size: Words of length less than this value will be removed. Default is 3.
    :param output_field: Name of output text field. None means original field is replaced.
    :return: A pandas dataframe with stopwords removed as either new column or replacing original texts.
    """

    out_field = output_field if output_field is not None else text_field
    df[out_field] = df[text_field].apply(
        lambda text: ' '.join([word for word in text.split()
                               if word not in stopwords and len(word) >= short_words_size])
    )
    return df


def tokenize(df: pd.DataFrame,
             text_field: str,
             output_field: str) -> pd.DataFrame:
    """
    Tokenize texts in dataframe.
    :param df: Dataframe that contains texts.
    :param text_field: Name of field that contains texts.
    :param output_field: Name of field that contains tokens.
    :return: A pandas dataframe that tokens of texts are computed as new column.
    """

    df[output_field] = df[text_field].apply(
        lambda column: ' '.join(regexp_tokenize(column, pattern=r'[\s,.?!-:]+', gaps=True, discard_empty=True))
    )
    return df


def compute_vocabulary(df: pd.DataFrame,
                       token_field: str) -> Set[str]:
    """
    Compute vocabulary of whole dataset and return the result.
    :param df: Dataframe that contains texts to compute vocabulary from.
    :param token_field: Name of the field that contains tokens.
    :return: The computed vocabulary of whole dataset.
    """

    voc = set()
    for _, tokens in df[token_field].items():
        voc |= set(tokens.split())
    return voc
