import os
import argparse
import enum
import pandas as pd
from sklearn.utils import resample


@enum.unique
class RebalanceMode(enum.Enum):
    """
    Sample rebalance modes.
    """

    OVER_SAMPLING = 1
    DOWN_SAMPLING = 2


def parse_cli():
    parser = argparse.ArgumentParser(description='Rebalance samples. Supports over-sampling and down-sampling.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of sample data to rebalance. Supports CSV file only.')
    parser.add_argument('mode', type=int, choices=[1, 2],
                        help='Rebalance mode. 1 for over-sampling. 2 for down-sampling.')
    parser.add_argument('-l', '--label', type=str, default='posneg', help='Target variable name.')
    parser.add_argument('-p', '--pos_label', default='pos', help='Value of positive label.')
    parser.add_argument('-n', '--neg_label', default='neg', help='Value of negative label.')
    parser.add_argument('-r', '--random_state', type=int, default=41,
                        help='Seed of random number. A prime number is preferred. Default is 41.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    if args.mode == 1:
        mode = RebalanceMode.OVER_SAMPLING
    elif args.mode == 2:
        mode = RebalanceMode.DOWN_SAMPLING
    else:
        raise ValueError('Unknown rebalance mode: {}'.format(args.mode))

    print('Working directory: ' + args.working_dir)
    print('File to rebalance: ' + args.data_file)
    print('Rebalance mode: {}'.format(mode.name))
    print('Label: {}'.format(args.label))
    print('Positive label: {}'.format(args.pos_label))
    print('Negative label: {}'.format(args.neg_label))
    print('Random seed: {}\n'.format(args.random_state))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))

    value_counts = df[args.label].value_counts(sort=True, ascending=False)
    print('Counts before re-sampling: \n{}\n'.format(value_counts))
    if value_counts[args.pos_label] >= value_counts[args.neg_label]:
        major_label = args.pos_label
        minor_label = args.neg_label
    else:
        major_label = args.neg_label
        minor_label = args.pos_label
    df_major = df[df[args.label] == major_label]
    df_minor = df[df[args.label] == minor_label]

    if mode is RebalanceMode.DOWN_SAMPLING:
        df_resampled = resample(df_major, replace=True, n_samples=df_minor.shape[0], random_state=args.random_state)
        df_result = pd.concat([df_resampled, df_minor])
    elif mode is RebalanceMode.OVER_SAMPLING:
        df_resampled = resample(df_minor, replace=True, n_samples=df_major.shape[0], random_state=args.random_state)
        df_result = pd.concat([df_resampled, df_major])
    else:
        raise ValueError('Unknown rebalance mode: {}'.format(mode))
    value_counts = df_result[args.label].value_counts()
    print('Counts after re-sampling: \n{}\n'.format(value_counts))
    filename = os.path.splitext(args.data_file)[0] + '__balanced-{}.csv'.format(mode.name)
    print('Output file: {}'.format(filename))
    df_result.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    print('Done!')
