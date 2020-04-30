import os
import re
import argparse
import itertools
import enum
import operator
import json
import pandas as pd
from collections import Counter, OrderedDict
from sklearn.feature_selection import chi2, SelectKBest


@enum.unique
class RankingMode(enum.Enum):
    """
    Feature ranking modes.
    """

    FREQUENCY = 1
    CHI_SQUARE = 2


def parse_cli():
    parser = argparse.ArgumentParser(description='Rank tagged features in descending order. Supports frequency and chi-square modes.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of tagged text file to lemmatize. Supports CSV file only.')
    parser.add_argument('mode', type=int, choices=[1, 2],
                        help='Feature-ranking mode. 1 for frequency. 2 for chi-square.')
    parser.add_argument('tagged_field', help='Tagged text field name.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    if args.mode == 1:
        mode = RankingMode.FREQUENCY
    elif args.mode == 2:
        mode = RankingMode.CHI_SQUARE
    else:
        raise ValueError('Unknown feature ranking mode: {}'.format(args.mode))

    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Feature ranking mode: {}'.format(mode.name))
    print('Tagged field: {}'.format(args.tagged_field))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))

    if mode is RankingMode.FREQUENCY:
        texts = []
        for lemmatized in df[args.tagged_field]:
            lemmatized = lemmatized[1:-1].replace('\'', '').split(', ')
            texts.extend(lemmatized)
        values = Counter(texts)
    elif mode is RankingMode.CHI_SQUARE:
        chi2_selector = _chi_square_feature_selection(df, label_field, num_features=df.shape[1] + 1)
        logging.info('chi2_selector={}'.format(chi2_selector))
        scores = chi2_selector.scores_.tolist()
        values = {index: score for index, score in enumerate(scores)}
        values = sorted(values.items(), key=lambda kv: kv[1], reverse=True)
        values = OrderedDict(values)
    else:
        raise ValueError('Unsupported feature ranking mode: {}'.format(mode))

    pos = filename.rfind('.')
    filename = filename[:pos] + '__rank{}.json'.format(mode.name)
    save_path = os.sep.join([working_dir, context['ds_nodash']])
    os.makedirs(save_path, exist_ok=True)
    with open(save_path + os.sep + filename, 'w') as fp:
        json.dump(values, fp)

    return filename
