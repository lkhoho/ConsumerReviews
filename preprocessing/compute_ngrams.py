from preprocessing.ngrams import SentCustomProperties, ReviewSents, Unigramer
import argparse
import os
import pandas as pd
import spacy


def parse_cli():
    parser = argparse.ArgumentParser(description='Compute N-grams of texts.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of original text data file. Supports CSV file only.')
    parser.add_argument('grams', type=str, help='Comma-separated n-grams to compute. 1 for unigram. ' \
                        '2 for bigrame. 3 for trigram. Example: 1,2 for both unigram and bigram.')
    parser.add_argument('--id_field', type=str, default='review_id', 
                        help='ID field name. Default "review_id" will be used if not provided.')
    parser.add_argument('--text_field', type=str, default='content', 
                        help='Text field name. Default field name "content" will be used if not provided.')
    parser.add_argument('--rating_field', type=str, default='overall_rating', 
                        help='Rating field name. Default field name "overall_rating" will be used if not provided.')
    parser.add_argument('--model', type=str, default='en_core_web_sm', 
                        help='Spacy language model. Default model "en_core_web_sm" will be used if not provided.')
    parser.add_argument('--min_pct', type=float, default=0.001, 
                        help='Minimum percent of all sentences that contains wanted aspects. Default value 0.001 ' \
                             'will be used if not provided.')
    parser.add_argument('--a_pct', type=float, default=0.09, 
                        help='Minimum percent of average frequency of amod or acomp tokens that serve as noun modifier.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    
    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('N-gram to compute: {}'.format(args.grams))
    print('ID field: {}'.format(args.id_field))
    print('Text field: {}'.format(args.text_field))
    print('Rating field: {}'.format(args.rating_field))
    print('Spacy model: {}'.format(args.model))
    print('Minimum percent: {}'.format(args.min_pct))
    print('A percent: {}'.format(args.a_pct))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))
    nlp = spacy.load(args.model)
    rs = ReviewSents(data=df, id_field=args.id_field, text_field=args.text_field, rating_field=args.rating_field)
    uni = Unigramer()
    res = uni.candidate_unigrams(rs, min_pct=args.min_pct, a_pct=args.a_pct)
    print('Size of unigrams: {}'.format(len(uni.cnt_dict)))
    
    words = [x.strip() for x in res]
    print('There are {} aspects in unigram.'.format(len(words)))

    filename = os.path.splitext(args.data_file)[0] + '__uni.txt'
    with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
        for word in words:
            fp.write(word)
            fp.write('\n')
    print('Done!')
