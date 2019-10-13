"""
File: cleanup.py
Author: Ke Liu <lkhoho@gmail.com>
Description: Clean up data so that a model can learn meaningful features and not overfit
             on irrelevant noise.

             Here is a checklist of to use to clean up data:
             1) Remove all irrelevant characters such as any non-alphanumeric characters,
                "@" twitter mentions or URLs.
             2) Tokenize text by separating it into individual words (tokens).
             3) Convert all characters to lowercase, in order to treat words such as
                "hello", "Hello", "HELLO" the same.
             4) Consider combining misspelled or alternately spelled words to a single
                representation (e.g. "cool"/"kewl"/"cooool").
             5) Consider lemmatization (reduce words such as "am", "is", "are" to a common
                form "be".
"""

import logging
import pandas as pd
import nltk
from typing import List, Dict, Set, Tuple, Union, Optional
from nltk.tokenize import RegexpTokenizer
from nltk.tag import pos_tag
from nltk.corpus import sentiwordnet as swn
from sklearn.feature_extraction.text import TfidfVectorizer


SentimentScore = Tuple[int, int, int]  # positive/negative/objective scores, respectively
WordToSentimentScoreMap = Dict[str, SentimentScore]
NoScoreWords = List[str]


log = logging.getLogger(__name__)


def standardize_text(df: pd.DataFrame,
                     text_field: str,
                     output_field: Optional[str] = None) -> pd.DataFrame:
    """
    Remove irrelevant characters, URLs and convert all characters to lowercase for texts in dataframe.
    :param df: Dataframe that contains texts to be cleaned.
    :param text_field: Name of field that contains texts.
    :param output_field: Name of output text field. None means original field is replaced.
    :return: A pandas dataframe with cleaned texts in either new column of replacing original texts.
    """

    out_field = output_field if output_field is not None else text_field
    df[out_field] = df[text_field].str.replace(r'http\S+', '')
    df[out_field] = df[text_field].str.replace(r'http', '')
    df[out_field] = df[text_field].str.replace(r'@\S+', '')
    df[out_field] = df[text_field].str.replace(r'[^A-Za-z0-9(),!?@\'\`\"\_\n]', ' ')
    df[out_field] = df[text_field].str.replace(r'@', 'at')
    df[out_field] = df[text_field].str.lower()
    return df


def remove_stopwords(df: pd.DataFrame,
                     text_field: str,
                     stopwords: Set[str],
                     output_field: Optional[str] = None) -> pd.DataFrame:
    """
    Remove stopwords in texts in dataframe.
    :param df: Dataframe that contains stopwords to be removed.
    :param text_field: Name of field that contains texts.
    :param stopwords: A set of stopwords.
    :param output_field: Name of output text field. None means original field is replaced.
    :return: A pandas dataframe with stopwords removed as either new column or replacing original texts.
    """

    out_field = output_field if output_field is not None else text_field
    df[out_field] = df[text_field].apply(
        lambda x: ' '.join([word for word in x.split() if word not in stopwords]))
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

    tokenizer = RegexpTokenizer(r'\w+')  # tokenize word-by-word
    df[output_field] = df[text_field].apply(tokenizer.tokenize)
    return df


def compute_vocabulary(df: pd.DataFrame,
                       token_field: str,
                       output_field: Optional[str] = None) -> Optional[Set[str]]:
    """
    If :output_field: is None, compute vocabulary of whole dataset and return the result. Otherwise,
    compute vocabulary for each sample and store the result in :output_field: column.
    :param df: Dataframe that contains texts to compute vocabulary from.
    :param token_field: Name of the field that contains tokens.
    :param output_field: Name of the field that store computed vocabulary.
    :return: The computed vocabulary of whole dataset, or for every-sample-vocabulary case, None.
    """

    if output_field is None:
        # compute vocabulary for whole dataset
        voc = set()
        for _, tokens in df[token_field].items():
            voc |= set(tokens)
        return voc
    else:
        # compute vocabulary for each sample
        df[output_field] = df[token_field].apply(lambda x: set(x))
        return None


def tag_vocabulary(vocabulary: List[str],
                   lang: str = Union['eng', 'rus'],
                   pos_mapping: Dict[str, str] = None) -> List[Tuple[str, str]]:
    """
    Part-of-speech tagging of given list of tokens. Only English and Russian are supported (by NLTK).
    :param vocabulary: Vocabulary that will be tagged.
    :param lang: Language of given tokens. Only English and Russian are supported (by NLTK).
    :param pos_mapping: Customized part-of-speech symbol mapping.
    :return: List of tagged tokens (tuple of (token, pos)).
    """

    if pos_mapping is None:
        return pos_tag(vocabulary, lang=lang)
    else:
        return [(word, pos_mapping.get(pos)) for (word, pos) in pos_tag(vocabulary, lang=lang)]


def tag_dataset(df: pd.DataFrame,
                token_field: str,
                output_field: str,
                lang: str = Union['eng', 'rus'],
                pos_mapping: Dict[str, str] = None) -> pd.DataFrame:
    """
    Part-of-speech tagging of each sample in given dataset. Only English and Russian are supported (by NLTK).
    :param df: Dataframe that contains tokens to be tagged.
    :param token_field: Name of field that contains tokens to be tagged.
    :param output_field: Name field that stores tagged result.
    :param lang: Language of tokens.
    :param pos_mapping: Customized part-of-speech symbol mapping.
    :return: Dataframe that has tagged tokens as new column.
    """

    df[output_field] = df[token_field].apply(lambda x: tag_vocabulary(x, lang, pos_mapping))
    return df


def lemmatize(df: pd.DataFrame,
              tagged_field: Optional[str],
              token_field: Optional[str],
              tagged_vocabulary: Optional[Dict[str, str]],
              output_field: str) -> pd.DataFrame:
    """
    If tagged field name is not None, lemmatize tagged tokens from given dataframe. Otherwise, lemmatize tokens
    with tagged vocabulary.
    :param df: Dataframe that contains tagged tokens or tokens which will be lemmatized.
    :param tagged_field: Name of field that contains tagged tokens to be lemmatized.
    :param token_field: Name of field that contains tokens to be lemmatized.
    :param tagged_vocabulary: Tagged vocabulary that is used to lemmatize tokens.
    :param output_field: Name of field that stores the lemmatized tokens.
    :return: Dataframe that contains lemmatized tokens as new column.
    """

    lemmatizer = nltk.WordNetLemmatizer()
    if tagged_field is not None:
        # lemmatize tagged tokens
        df[output_field] = df[tagged_field].apply(
            lambda x: [lemmatizer.lemmatize(word, pos=pos) + '_' + pos for (word, pos) in x]
        )
    elif token_field is not None and tagged_vocabulary is not None:
        # lemmatize tokens
        df[output_field] = df[token_field].apply(
            lambda x: [lemmatizer.lemmatize(word, pos=tagged_vocabulary[word]) + '_' + tagged_vocabulary[word]
                       for word in x]
        )
    else:
        raise ValueError('When tagged field name is None, token field name and tagged vocabulary cannot both be None.')

    return df


def compute_sentiment_score(df: pd.DataFrame,
                            tagged_field: Optional[str],
                            tagged_vocabulary: Optional[Dict[str, str]],
                            batch_size: int = 100) -> Tuple[WordToSentimentScoreMap, NoScoreWords]:
    """
    Compute sentiment scores for tagged words in batches.
    :param df: Dataframe contains tagged words that sentiment scores will be computed.
    :param tagged_field: Name of field that contains tagged words.
    :param tagged_vocabulary: Vocabulary with tagged words.
    :param batch_size: Batch size. Default is 100.
    :return: Tuple contains word to sentiment_score map and list of words that have no score.
    """

    # if tagged_field is None and tagged_vocabulary is None:
    #     raise ValueError('Tagged field and tagged vocabulary cannot both be None.')
    #
    # if tagged_field is not None:
    #     voc = {word for (word, ) df[tagged_field].to_numpy().tolist()}
    # elif tagged_vocabulary is not None:
    #     voc = tagged_vocabulary
    pass


def compute_TFIDF(texts: List[str],
                  label: Optional[str],
                  label_values: Optional[Union[List[str], pd.Series]]) \
        -> pd.DataFrame:
    """
    Compute TFIDF matrix from :texts:.
    :param texts: A list of texts that TFIDF matrix is calculated from.
    :param label: Name of label column. A label column indicates the category of samples (e.g. positive/negative).
    :param label_values: Values of label column.
    :return: Dataframe with TFIDF matrix with labels as the last column (if not None).
    """

    if (label is None) ^ (label_values is None):
        raise ValueError('Label and label values should both be None or not None.')

    vectorizer = TfidfVectorizer()
    tfidf = vectorizer.fit_transform(texts)
    df = pd.DataFrame(data=tfidf.toarray(), columns=vectorizer.get_feature_names())
    if label is not None:
        if type(label_values) == pd.Series:
            df[label] = label_values
        else:
            df[label] = pd.Series(label_values)
    return df
