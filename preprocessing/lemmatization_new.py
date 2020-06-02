import os
import argparse
import enum
import nltk
import emoji
import pandas as pd
from typing import Set, Optional
from nltk.tokenize import regexp_tokenize


@enum.unique
class LemmatizationMode(enum.Enum):
    """
    Lemmatizer modes.
    """

    VOCABULARY = 1
    PARAGRAPH = 2


def parse_cli():
    parser = argparse.ArgumentParser(description='Lemmatize texts. Supports by-vocabulary and by-paragraph modes.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of text data file to lemmatize. Supports CSV file only.')
    parser.add_argument('mode', type=int, choices=[1, 2],
                        help='Lemmatization mode. 1 for by-vocabulary. 2 for by-paragraph.')
    parser.add_argument('--stopwords', default=os.sep.join(['resources', 'stopwords.txt']), 
                        help='Path of stopwords file. One stopword per line. ' \
                             'Default stopwords file will be used if not provided.')
    parser.add_argument('--text_field', type=str, default='content', 
                        help='Text field name. Default field name "content" will be used if not provided.')
    parser.add_argument('--model', default=os.sep.join(['lib', 'stanford-postagger', 'models', 
                                                        'english-left3words-distsim.tagger']), 
                        help='Path of POS tagger model. Default tagger model will be used if not provided.')
    parser.add_argument('--jar', default=os.sep.join(['lib', 'stanford-postagger', 'stanford-postagger.jar']), 
                        help='Path of POS tagger JAR file. Default JAR file will be used if not provided.')
    parser.add_argument('--java_opts', default='-Xms512m -Xmx4g', 
                        help='Java options used by POS tagger. Default options are "-Xms512m -Xmx4g".')
    parser.add_argument('-r', '--random_state', type=int, default=41,
                        help='Seed of random number. A prime number is preferred. Default is 41.')
    return parser.parse_args()


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

    # df[output_field] = df[text_field].apply(
    #     lambda column: emoji.get_emoji_regexp().sub(u'', column)
    # )

    df[output_field] = df[text_field].str.replace("'m", ' am')
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


if __name__ == '__main__':
    pos_mapping = {
        'JJ': 'a',
        'JJR': 'a',
        'JJS': 'a',
        'NN': 'n',
        'NNP': 'n',
        'NNS': 'n',
        'NNPS': 'n',
        'RB': 'r',
        'RBR': 'r',
        'RBS': 'r',
        'VB': 'v',
        'VBD': 'v',
        'VBG': 'v',
        'VBN': 'v',
        'VBP': 'v',
        'VBZ': 'v'
    }
    invalid_pos = 'n'  # unknown part-of-speech symbol from tagger

    args = parse_cli()
    if args.mode == 1:
        mode = LemmatizationMode.VOCABULARY
    elif args.mode == 2:
        mode = LemmatizationMode.PARAGRAPH
    else:
        raise ValueError('Unknown lemmatization mode: {}'.format(args.mode))

    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Lemmatization mode: {}'.format(mode.name))
    print('Stopwords file: {}'.format(args.stopwords))
    print('Text field: {}'.format(args.text_field))
    print('POS model: {}'.format(args.model))
    print('POS JAR: {}'.format(args.jar))
    print('POS Java opts: {}'.format(args.java_opts))
    print('Random seed: {}'.format(args.random_state))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))
    lemmatizer = nltk.WordNetLemmatizer()
    tagger = nltk.tag.stanford.StanfordPOSTagger(model_filename=args.model,
                                                 path_to_jar=args.jar,
                                                 java_options=args.java_opts)
    print('POS tagger created.')

    stopwords = set()
    with open(args.stopwords) as fp:
        for line in fp.readlines():
            stopwords.add(line.strip())
    print('There are {} stopwords.'.format(len(stopwords)))

    std_column = 'standardized_text'  # standardized column name
    df = standardize_text(df, text_field=args.text_field, output_field=std_column)
    print('Standardized shape={}'.format(df.shape))

    token_column = 'tokens'  # tokenized column name
    df = tokenize(df, text_field=std_column, output_field=token_column)
    df[token_column] = df[token_column].astype(str)
    print('Tokenized shape={}'.format(df.shape))

    df = remove_words(df, text_field=token_column, stopwords=stopwords)
    
    if mode is LemmatizationMode.VOCABULARY:
        voc = compute_vocabulary(df, token_field=token_column)
        print('Vocabulary size={}'.format(len(voc)))
        tagged_voc = {token: pos_mapping.get(pos, invalid_pos)
                      for (token, pos) in tagger.tag(list(voc)) if len(token) > 1}
        df['lemmatized'] = df['tokens'].apply(
            lambda tokens: [lemmatizer.lemmatize(token, pos=tagged_voc[token]) + '_' + tagged_voc[token]
                            for token in tokens.split()]
        )
    else:
        df['lemmatized'] = df['tokens'].apply(
            lambda tokens: [lemmatizer.lemmatize(token, pos=pos) + '_' + pos
                            for token, pos in [(_token, pos_mapping.get(_pos, invalid_pos))
                                               for _token, _pos in tagger.tag(tokens.split()) if len(_token) > 1]]
        )

    filename = os.path.splitext(args.data_file)[0] + '__lem-{}.csv'.format(mode.name)
    print('Output file: {}'.format(filename))
    df.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    print('Done!')
