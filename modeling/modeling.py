import os
import argparse
import json
import pandas as pd
import matplotlib.pylab as plt
from multiprocessing import cpu_count
from uuid import uuid4
from typing import List
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score
from sklearn.svm import SVC


def parse_cli():
    parser = argparse.ArgumentParser(description='Build prediction model and plot results.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='TFIDF data file.')
    parser.add_argument('label', help='Label field name.')
    parser.add_argument('freq_file', help='File containing list of words ranked by frequency.')
    parser.add_argument('chi2_file', help='File containing list of words ranked by chi-square.')
    parser.add_argument('-n', '--num_features',
                        default='10,25,50,75,100,125,150,175,200,225,250,275,300,350,400,450,500,550,600,650,700,750,'
                                '800,850,900,950,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000,2095',
                        help='# of features as a comma-separated list. Default list '
                             '"10,25,50,75,100,125,150,175,200,225,250,275,300,350,400,450,500,550,600,650,700,'
                             '750,800,850,900,950,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000,2095" '
                             'will be used if not provided.')
    parser.add_argument('-s', '--scorers', default='accuracy,balanced_accuracy,roc_auc',
                        help='Comma-separated scorers. Default scores "accuracy,balanced_accuracy,roc_auc" will be '
                             'used if not provided.')
    parser.add_argument('-x', '--x_lim', default='0,2200',
                        help='Comma-separated range of X axis in plot. Default [0.0, 2200.0] will be used '
                             'if not provided.')
    parser.add_argument('-y', '--y_lim', default='0,1',
                        help='Comma-separated range of Y axis in plot. Default [0.0, 1.0] will be used '
                             'if not provided.')
    return parser.parse_args()


def kfold_cv(classifier,
             tfidf: pd.DataFrame,
             all_features: List[str],
             num_features: List[int],
             scorers: List[str]):
    le = LabelEncoder()
    y = le.fit_transform(tfidf[args.label])
    X = tfidf.drop(columns=[args.label])

    results = {
        scorer: {
            str(num): {
                'raw': 0,
                'mean': 0
            } for num in num_features
        } for scorer in scorers
    }

    for num in num_features:
        features = all_features[:num]
        for scorer in scorers:
            print('Running k-fold CV: num_features={}, scorer={}'.format(num, scorer))
            scores = cross_val_score(classifier, X[features], y, cv=10, scoring=scorer, n_jobs=cpu_count())
            results[scorer][str(num)]['raw'] = scores.tolist()
            results[scorer][str(num)]['mean'] = scores.mean()

    return results


if __name__ == '__main__':
    args = parse_cli()

    print('Working directory: {}'.format(args.working_dir))
    print('TFIDF file: {}'.format(args.data_file))
    print('Label field: {}'.format(args.label))

    freq_features = []
    with open(os.sep.join([args.working_dir, args.freq_file])) as fp:
        for line in fp:
            freq_features.append(line.strip())
    print('{} features in frequency file: {}'.format(len(freq_features), args.freq_file))

    chi2_features = []
    with open(os.sep.join([args.working_dir, args.chi2_file])) as fp:
        for line in fp:
            chi2_features.append(line.strip())
    print('{} features in chi-square file: {}'.format(len(chi2_features), args.chi2_file))

    num_features = [int(x) for x in args.num_features.split(',')]
    print('# of features: {}'.format(num_features))

    scorers = args.scorers.split(',')
    print('scorers: {}'.format(scorers))

    x_lim = tuple(float(x) for x in args.x_lim.split(','))
    y_lim = tuple(float(y) for y in args.y_lim.split(','))
    print('X axis range: {}'.format(x_lim))
    print('Y axis range: {}'.format(y_lim))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))

    results = {}

    print('Running K-fold CV for frequency ranking ...')
    freq_results = kfold_cv(SVC(kernel='linear', C=1),
                            tfidf=df,
                            all_features=freq_features,
                            num_features=num_features,
                            scorers=scorers)
    results['freq'] = freq_results

    print('Running K-fold CV for chi2 ranking ...')
    chi2_results = kfold_cv(SVC(kernel='linear', C=1),
                            tfidf=df,
                            all_features=chi2_features,
                            num_features=num_features,
                            scorers=scorers)
    results['chi2'] = chi2_results
    score_filename = '{}.json'.format(uuid4())
    with open(os.sep.join([args.working_dir, score_filename]), 'w') as fp:
        json.dump(results, fp)
    print('Save score results to file {}'.format(score_filename))

    # start plotting
    dir_path = os.sep.join([args.working_dir, 'figures'])
    os.makedirs(dir_path, exist_ok=True)

    for ranking in {'freq', 'chi2'}:
        for scorer in scorers:
            fig = plt.figure()
            scorer_data = results[ranking][scorer]
            n_feats = sorted([int(x) for x in scorer_data.keys()])
            values = [scorer_data[str(x)]['mean'] for x in n_feats]
            plt.plot(n_feats, values, 'bo-')
            plt.xlim(x_lim)
            plt.ylim(y_lim)
            plt.xlabel('# of features')
            plt.ylabel('performance')
            title = 'Mean scores for {} {}'.format(ranking, scorer)
            fig.savefig(os.sep.join([dir_path, title + '.png']))
    print('Done!')
