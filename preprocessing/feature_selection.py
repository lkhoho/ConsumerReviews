import os
import re
import argparse
import itertools
import enum
import operator
import pandas as pd
import numpy as np
from typing import List, Dict, Set, Union
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_selection import chi2, SelectKBest


@enum.unique
class FeatureMode(enum.Enum):
    """
    Feature modes.
    """

    FREQ = 1
    CHI2 = 2


def parse_cli():
    parser = argparse.ArgumentParser(description='Select partial/all features from TFIDF file. Supports frequency (in descending order) and chi-square modes.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('tfidf_file', help='Path of TFIDF file whose dimension are reduced. Supports CSV file only.')
    parser.add_argument('mode', type=int, choices=[1, 2], help='Feature-ranking mode. 1 for frequency. 2 for chi-square.')
    parser.add_argument('-l', '--labels', help='Comma-separated target variables that will be excluded during feature selection.')
    parser.add_argument('-n', '--num_features', help='Comma-separated # of features to select. All features are selected if omitted.')
    parser.add_argument('-f', '--tagged_file', help='Path of tagged file for Frequency mode.')
    parser.add_argument('-t', '--tagged_field', help='Tagged text field name for Frequency mode.')
    parser.add_argument('-c', '--chi2_label', help='Label used to compute score function for Chi-square mode.')
    return parser.parse_args()


def _sort_features_by_freq(corpus: pd.Series) -> List[str]:
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(corpus.values)
    features = vectorizer.get_feature_names()
    counts = np.sum(X.toarray(), axis=0, dtype=np.int32)  # sum feature counts by column
    if len(features) != len(counts):
        raise AssertionError('Length of features does not equal to length of counts.')
    frequency = {features[i]: counts[i] for i in range(len(features))}
    return [x[0] for x in sorted(frequency.items(), key=operator.itemgetter(1), reverse=True)]


def _chi2_feature_selection(df: pd.DataFrame, chi2_label: str, labels: List[str],
                            num_features: Union[List[int], str]):
    y = df[chi2_label]
    columns_to_drop = [chi2_label] + labels
    X = df.drop(columns=columns_to_drop)
    if num_features == 'all' or num_features > X.shape[1]:
        num_features = 'all'
    print('# of features in data frame: {}'.format(num_features))
    chi2_selector = SelectKBest(chi2, k=num_features)
    chi2_selector.fit(X, y)
    return chi2_selector


if __name__ == '__main__':
    args = parse_cli()
    
    if args.mode == 1:
        mode = FeatureMode.FREQ
        if args.tagged_file is None or args.tagged_field is None:
            raise ValueError('Tagged file name or tagged field cannot be empty for frequency mode.')
    elif args.mode == 2:
        mode = FeatureMode.CHI2
        if args.chi2_label is None:
            raise ValueError('Chi2 label cannot be empty for chi-square mode.')
    else:
        raise ValueError('Unknown feature ranking mode: {}'.format(args.mode))
    
    if args.labels is not None:
        labels = args.labels.split(',')
    else:
        labels = []

    if args.num_features is not None:
        num_features = [int(x) for x in args.num_features.split(',')]
    else:
        num_features = 'all'

    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.tfidf_file))
    print('Select mode: {}'.format(mode.name))
    print('Tagged field: {}'.format(args.tagged_field))
    print('Labels: {}'.format(labels))
    print('# features to select: {}'.format(num_features))
    if mode is FeatureMode.FREQ:
        print('Tagged file: {}'.format(args.tagged_file))
        print('Tagged field: {}'.format(args.tagged_field))
    if mode is FeatureMode.CHI2:
        print('Chi2 label: {}'.format(args.chi2_label))

    df = pd.read_csv(os.sep.join([args.working_dir, args.tfidf_file]))
    print('TFIDF dataframe shape={}'.format(df.shape))
    if mode is FeatureMode.FREQ:
        df_tag = pd.read_csv(os.sep.join([args.working_dir, args.tagged_file]))
        print('Tagged dataframe shape={}'.format(df_tag.shape))
        selected_features = _sort_features_by_freq(df_tag[args.tagged_field])
        if num_features == 'all':
            selected_features.extend(labels)
            result = df[selected_features]
            print('Output shape: {}'.format(result.shape))
            filename = os.path.splitext(args.tfidf_file)[0] + '__{}_all.csv'.format(mode.name)
            print('Output file: {}'.format(filename))
            result.to_csv(os.sep.join([args.working_dir, filename]), index=False)
        else:
            for num in num_features:
                features = selected_features[:num]
                features.extend(labels)
                result = df[features]
                print('Output shape: {}'.format(result.shape))
                filename = os.path.splitext(args.tfidf_file)[0] + '__{}_{}.csv'.format(mode.name, num)
                print('Output file: {}'.format(filename))
                result.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    elif mode is FeatureMode.CHI2:
        if num_features == 'all':
            chi2_selector = _chi2_feature_selection(df, args.chi2_label, labels, num_features)
            result = df.iloc[:, chi2_selector.get_support(indices=True)]
            for label in labels:
                result.loc[:, label] = df[label]
            print('Output shape: {}'.format(result.shape))
            filename = os.path.splitext(args.tfidf_file)[0] + '__{}_all.csv'.format(mode.name)
            print('Output file: {}'.format(filename))
            result.to_csv(os.sep.join([args.working_dir, filename]), index=False)
        else:
            for num in num_features:
                chi2_selector = _chi2_feature_selection(df, args.chi2_label, labels, num)
                result = df.iloc[:, chi2_selector.get_support(indices=True)]
                for label in labels:
                    result.loc[:, label] = df[label]
                print('Output shape: {}'.format(result.shape))
                filename = os.path.splitext(args.tfidf_file)[0] + '__{}_{}.csv'.format(mode.name, num)
                print('Output file: {}'.format(filename))
                result.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    else:
        raise ValueError('Unknown feature ranking mode: {}'.format(args.mode))


# def feature_ranking(mode: FeatureMode, tagged_field: str, label_field: str):
#     if mode is FeatureMode.FREQUENCY:
#         texts = []
#         for tagged in df[tagged_field]:
#             tagged = tagged[1:-1].replace('\'', '').split(', ')
#             texts.extend(tagged)
#         values = Counter(texts)
#     elif mode is FeatureMode.CHI_SQUARE:
#         chi2_selector = _chi2_feature_selection(df, label_field, num_features=df.shape[1] + 1)
#         print('chi2_selector={}'.format(chi2_selector))
#         scores = chi2_selector.scores_.tolist()
#         values = {index: score for index, score in enumerate(scores)}
#         values = sorted(values.items(), key=lambda kv: kv[1], reverse=True)
#         values = OrderedDict(values)
#     else:
#         raise ValueError('Unsupported feature ranking mode: {}'.format(mode))

#     pos = filename.rfind('.')
#     filename = filename[:pos] + '__rank{}.json'.format(mode.name)
#     save_path = os.sep.join([working_dir, context['ds_nodash']])
#     os.makedirs(save_path, exist_ok=True)
#     with open(save_path + os.sep + filename, 'w') as fp:
#         json.dump(values, fp)

#     return filename
