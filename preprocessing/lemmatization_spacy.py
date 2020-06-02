import argparse
import os
import pandas as pd
import spacy


def parse_cli():
    parser = argparse.ArgumentParser(description='Lemmatize texts using Spacy.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of text data file to lemmatize. Supports CSV file only.')
    parser.add_argument('--text_field', type=str, default='standardized', 
                        help='Text field name. Default field name "standardized" will be used if not provided.')
    parser.add_argument('--model', type=str, default='en_core_web_sm', 
                        help='Spacy language model. Default model "en_core_web_sm" will be used if not provided.')
    parser.add_argument('--exclude_file', default=os.sep.join(['resources', 'nonaspects.txt']), 
                        help='Path to text file containing words that will be removed. One word per line.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()

    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Text field: {}'.format(args.text_field))
    print('Spacy model: {}'.format(args.model))
    print('Exclude file: {}'.format(args.exclude_file))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))

    excluded_words = set()
    with open(args.exclude_file) as fp:
        for line in fp:
            excluded_words.add(line.strip())
    print('{} words will be removed as non-aspects.'.format(len(excluded_words)))

    nlp = spacy.load(args.model)
    df['lemmatized'] = df[args.text_field].apply(
        lambda text: [token.lemma_.strip() for token in nlp(text)
                      if token.lemma_ != '-PRON-' and 
                      token.lemma_ not in nlp.Defaults.stop_words and
                      token.lemma_ not in excluded_words and  
                      len(token.lemma_) > 1]
    )

    filename = os.path.splitext(args.data_file)[0] + '__lem-SPACY.csv'
    print('Output file: {}'.format(filename))
    df.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    print('Done!')
