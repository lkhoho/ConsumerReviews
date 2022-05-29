import argparse
import json
import emoji
import os
import pandas as pd
# import numpy as np
# from typing import Tuple
# from textblob import TextBlob
# from multiprocessing import Pool, cpu_count


def parse_cli():
    parser = argparse.ArgumentParser(description='Add labels based on given criteria to dataset.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of text data file to lemmatize. Supports CSV file only.')
    parser.add_argument('--criteria', type=str, 
                        help='A dict-format string of criteria and labels. e.g. {"A+": 5.0, "A": 4.0, "B-": 3.0}')
    parser.add_argument('--criteria_file', type=str,
                        help='Path to a file which defines criteria and labels as dict.')
    parser.add_argument('--criteria_field', type=str, default='scores',
                        help='Criteria field name. Default field name "scores" will be used if not provided.')
    parser.add_argument('--label_field', type=str, default='posneg',
                        help='Label field name. Default field name "posneg" will be used if not provided.')
    return parser.parse_args()


# def _correct_spelling(helper_args: Tuple[pd.DataFrame, str, str]) -> pd.DataFrame:
#     dataframe, input_field, result_field = helper_args[0], helper_args[1], helper_args[2]
#     dataframe[result_field] = dataframe[input_field].apply(
#         lambda column: str(TextBlob(column).correct())
#     )
#     return dataframe





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
    print(f'Working directory: {args.working_dir}')
    print(f'Data file: {args.data_file}')
    print(f'Criteria: {args.criteria}')
    print(f'Criteria file: {args.criteria_file}')
    print(f'Criteria field: {args.criteria_field}')
    print(f'Label field: {args.label_field}')

    criteria = None
    if args.criteria is not None:
        criteria = json.loads(args.criteria)
    elif args.criteria_file is not None:
        with open(args.criteria_file) as fp:
            criteria = json.load(fp)
    else:
        print('No criteria or criteria file is detected. Terminate.')

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print(f'Dataframe shape={df.shape}')
    df[args.label_field] = pd.Series(data=[criteria[score] for score in df[args.criteria_field]])
    print(f'After adding label column, dataframe shape={df.shape}')
    filename = os.path.splitext(args.data_file)[0] + f'__{args.label_field}.csv'
    print('Output file: {}'.format(filename))
    df.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    print('Done!')
