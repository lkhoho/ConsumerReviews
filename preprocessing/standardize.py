import argparse
import emoji
import os
import pandas as pd
# import numpy as np
# from typing import Tuple
# from textblob import TextBlob
# from multiprocessing import Pool, cpu_count


def parse_cli():
    parser = argparse.ArgumentParser(description='Standardize texts.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of text data file to lemmatize. Supports CSV file only.')
    parser.add_argument('--text_field', type=str, default='content',
                        help='Text field name. Default field name "content" will be used if not provided.')
    parser.add_argument('--output_field', type=str, default='standardized',
                        help='Output field name. Default field name "standardized" will be used if not provided.')
    parser.add_argument('-r', '--random_state', type=int, default=41,
                        help='Seed of random number. A prime number is preferred. Default is 41.')
    return parser.parse_args()


# def _correct_spelling(helper_args: Tuple[pd.DataFrame, str, str]) -> pd.DataFrame:
#     dataframe, input_field, result_field = helper_args[0], helper_args[1], helper_args[2]
#     dataframe[result_field] = dataframe[input_field].apply(
#         lambda column: str(TextBlob(column).correct())
#     )
#     return dataframe


def standardize_text(df: pd.DataFrame, text_field: str, output_field: str) -> pd.DataFrame:
    """
    Convert all characters to lowercased and remove irrelevant characters, URLs in dataframe.
    :param df: Dataframe that contains texts to be cleaned.
    :param text_field: Name of field that contains texts.
    :param output_field: Name of output text field.
    :return: A pandas dataframe with cleaned texts in either new column of replacing original texts.
    """

    df[output_field] = df[text_field].apply(
        lambda column: emoji.get_emoji_regexp().sub(u'', column)
    )

    # df_split = np.array_split(df, cpu_count())
    # helper_field1 = [output_field for _ in range(len(df_split))]
    # helper_field2 = [output_field for _ in range(len(df_split))]
    # pool = Pool(cpu_count())
    # df = pd.concat(pool.map(_correct_spelling, zip(df_split, helper_field1, helper_field2)))
    #
    # df[output_field] = df[output_field].apply(
    #     lambda column: str(TextBlob(column).correct())
    # )

    df[output_field] = df[output_field].str.lower()

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
    df[output_field] = df[output_field].str.replace("’s", ' is')
    df[output_field] = df[output_field].str.replace("´s", ' is')

    df[output_field] = df[output_field].str.replace(r'\b[\w\d]+\.\s*com\b', 'QQCOM')

    df[output_field] = df[output_field].str.replace('/', ' ')
    df[output_field] = df[output_field].str.replace('\.{1,}', '. ')
    df[output_field] = df[output_field].str.replace('!{1,}', '! ')
    df[output_field] = df[output_field].str.replace('\?{1,}', '? ')
    df[output_field] = df[output_field].str.replace('€+', '')
    df[output_field] = df[output_field].str.replace('[0-9$&~\\()[\]{}<>%\'"“”‘’，;…+\-_=*]+', '')
    df[output_field] = df[output_field].str.replace(r'http\S+', '')
    df[output_field] = df[output_field].str.replace(r'http', '')
    df[output_field] = df[output_field].str.replace(r'@\S+', '')
    df[output_field] = df[output_field].str.replace(r'@', 'at')

    df[output_field] = df[output_field].astype(str)

    return df


def filter_text_length(dataframe: pd.DataFrame, 
                        column: str, 
                        min_length: int, 
                        max_length: int = None) -> pd.DataFrame:
    """
    Remove rows whose `text_field` do not meet given length requirement. 
    """
    print(f'Before cleaning, dataframe shape={dataframe.shape}')
    dataframe[column] = dataframe[column].apply(lambda text: text.strip())
    if min_length is not None:
        dataframe = dataframe[dataframe[column].str.len() >= min_length]
    if max_length is not None:
        dataframe = dataframe[dataframe[column].str.len() <= max_length]
    print(f'After cleaning, dataframe shape={dataframe.shape}')
    return dataframe


if __name__ == '__main__':
    args = parse_cli()
    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Text field: {}'.format(args.text_field))
    print('Output field: {}'.format(args.output_field))
    print('Random seed: {}'.format(args.random_state))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))

    df[args.text_field] = df[args.text_field].astype(str)
    df = filter_text_length(df, column=args.text_field, min_length=5)

    df = standardize_text(df, text_field=args.text_field, output_field=args.output_field)
    print('Standardized shape={}'.format(df.shape))

    df = filter_text_length(df, column=args.output_field, min_length=1)

    filename = os.path.splitext(args.data_file)[0] + '__std.csv'
    print('Output file: {}'.format(filename))
    df.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    print('Done!')
