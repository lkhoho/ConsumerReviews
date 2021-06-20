import argparse
import os
import numpy as np
import pandas as pd
from sklearn.feature_selection import chi2, SelectKBest


def parse_cli():
    parser = argparse.ArgumentParser(description='Rank features by chi-square in descending order.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Name of TFIDF data file. Supports CSV file only.')
    parser.add_argument('label_field', help='Label field name to.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    
    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Label field: {}'.format(args.label_field))
    
    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))

    y = df[args.label_field]
    x = df.drop(columns=[args.label_field])
    selector = SelectKBest(score_func=chi2, k='all')  # select all features
    selector.fit(x, y)
    features = x.columns.values[selector.get_support()]
    scores = selector.scores_[selector.get_support()]
    assert len(features) == len(scores), \
           'Length of features ({}) does not equal to length of scores ({}).'.format(len(features), len(scores))
    scores = {features[i]: scores[i] for i in range(len(features))}
    scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    result = [(x[0], x[1]) for x in scores]

    filename = os.path.splitext(args.data_file)[0] + '__ranking-chi2.txt'
    print('Output file: {}'.format(filename))
    with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
        for fq in result:
            fp.write('{}: {}\n'.format(fq[0], fq[1]))
    print('Done!')
