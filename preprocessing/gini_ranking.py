import pandas as pd
import argparse
import math
import os


def parse_cli():
    parser = argparse.ArgumentParser(description='Compute GINI index.')
    parser.add_argument('working_dir', help='Working directory.')
    parser.add_argument('data_file', help='Name of input tfidf file to calculate gini.')
    parser.add_argument('label', help='Label field name.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    print('working_dir = {}'.format(args.working_dir))
    print('input data = {}'.format(args.data_file))
    print('label = {}'.format(args.label))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))      
    df_shape = df.shape
    print('Dataset shape={}'.format(df_shape))

    result = [('feature', 'gini', args.label, 'num_pos', 'num_neg', 'total')]
    result2 = []
    for column in df:
        if column == args.label:
            continue
        
        idx_app = df.index[df[column] > 0]  # reviews' Id that a particular word appears
        idx_napp = df.index[~df.index.isin(idx_app)]  # reviews' Id that a particular word doesn't appear
        assert df_shape[0] == (len(idx_app) + len(idx_napp)), 'appear_index + not_appear_index != num_reviews'
    
        num_pos_app, num_neg_app = 0, 0  # number of positive/negative reviews that a particular word appears
        num_pos_napp, num_neg_napp = 0, 0  # number of positive/negative reviews that a particular word doesn't appear
    
        for label in (df.loc[idx_app, args.label] == 'pos'):
            if label:
                num_pos_app += 1
            else:
                num_neg_app += 1
        for label in (df.loc[idx_napp, args.label] == 'pos'):
            if label:
                num_pos_napp += 1
            else:
                num_neg_napp += 1
        assert num_pos_app + num_neg_app == len(idx_app), 'appear_pos + appear_neg != num_appear'
        assert num_pos_napp + num_neg_napp == len(idx_napp), 'not_appear_pos + not_appear_neg != num_not_appear'

        # gini index of appeared case
        gini_app = (num_pos_app / len(idx_app))**2 + (num_neg_app / len(idx_app))**2
        
        # gini index of not appeared case
        gini_napp = (num_pos_napp / len(idx_napp))**2 + (num_neg_napp / len(idx_napp))**2
    
        posneg_app = 1 if num_pos_app > num_neg_app else (0 if num_neg_app > num_pos_app else 2)
        posneg_napp = 1 if num_pos_napp > num_neg_napp else (0 if num_neg_napp > num_pos_napp else 2)
        result.append((column, gini_app, posneg_app, num_pos_app, num_neg_app, len(idx_app)))
        result2.append((column, gini_app))
        #result.append((column + '_0', gini_napp, posneg_napp, num_pos_napp, num_neg_napp, len(idx_napp)))
    
    result2 = sorted(result2, key=lambda x: x[1], reverse=True)

    filename = os.path.splitext(args.data_file)[0] + '__gini.csv'
    print('Output filename: {}'.format(filename))
    with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
        for line in result:
            fp.write(','.join([str(x) for x in line]))
            fp.write('\n')
    
    filename = os.path.splitext(args.data_file)[0] + '__ranking-gini.txt'
    print('Output filename: {}'.format(filename))
    with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
        for line in result2:
            fp.write('{}: {}\n'.format(line[0], line[1]))
    print('Finished.')
