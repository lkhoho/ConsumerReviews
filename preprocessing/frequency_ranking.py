import argparse
import os
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


def parse_cli():
    parser = argparse.ArgumentParser(description='Rank features by frequency in descending order.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of lemmatized text. Supports CSV file only.')
    parser.add_argument('--text_field', default='lemmatized', 
                        help='Lemmatized text field name. Default value "lemmatized" will be used if not provided.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    
    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Text field: {}'.format(args.text_field))
    
    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))

    corpus = df[args.text_field]
    vec = CountVectorizer()
    x = vec.fit_transform(corpus.to_list())
    features = vec.get_feature_names()
    counts = np.sum(x.toarray(), axis=0, dtype=np.int32)  # sum features counts by column
    assert len(features) == len(counts), \
           'Length of features ({}) does not equal to length of counts ({}).'.format(len(features), len(counts))

    frequency = {features[i]: counts[i] for i in range(len(features))}
    frequency = sorted(frequency.items(), key=lambda x: x[1], reverse=True)
    result = [(x[0], x[1]) for x in frequency]

    filename = os.path.splitext(args.data_file)[0] + '__ranking-freq.txt'
    print('Output file: {}'.format(filename))
    with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
        for fq in result:
            fp.write('{}: {}\n'.format(fq[0], fq[1]))
    print('Done!')
