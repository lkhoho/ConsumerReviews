import os
import argparse
import csv
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer


def parse_cli():
    parser = argparse.ArgumentParser(description='Compute TFIDF matrix from text file.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of text file. Supports CSV file only.')
    parser.add_argument('token_field', help='Name of tokenized text column.')
    parser.add_argument('-l', '--label', help='Optional target variable name.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    
    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Token field: {}'.format(args.token_field))
    print('Label: {}'.format(args.label))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))
    vectorizer = TfidfVectorizer()
    texts = [''.join(text) for text in df[args.token_field]]
    tfidf = vectorizer.fit_transform(texts)
    df_result = pd.DataFrame(data=tfidf.toarray(), columns=vectorizer.get_feature_names())
    if args.label is not None:
        df_result[args.label] = df[args.label]
    print('TFIDF shape={}'.format(df_result.shape))

    filename = os.path.splitext(args.data_file)[0] + '__tfidf.csv'
    print('Output file: {}'.format(filename))
    df_result.to_csv(os.sep.join([args.working_dir, filename]), index=False, 
                     quoting=csv.QUOTE_NONNUMERIC, chunksize=10000)
    print('Done!')
