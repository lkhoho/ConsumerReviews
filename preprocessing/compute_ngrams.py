from preprocessing.ngrams import ReviewSents, Unigramer, Bigramer, Trigramer
import argparse
import os
import pandas as pd
import spacy


def parse_cli():
    parser = argparse.ArgumentParser(description='Compute N-grams of texts.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of original text data file. Supports CSV file only.')
    parser.add_argument('ngrams', type=int,
                        help='N-grams to compute. 1 for unigram. 2 for bigrame. 3 for trigram. ' \
                             'Compute unigram is prerequisite of bigram and trigram, and bigram is prerequisite ' \
                             'of trigram.')
    parser.add_argument('--id_field', type=str, default='review_id',
                        help='ID field name. Default "review_id" will be used if not provided.')
    parser.add_argument('--text_field', type=str, default='standardized',
                        help='Text field name. Default field name "standardized" will be used if not provided.')
    parser.add_argument('--rating_field', type=str, default='overall_rating',
                        help='Rating field name. Default field name "overall_rating" will be used if not provided.')
    parser.add_argument('--model', type=str, default='en_core_web_sm',
                        help='Spacy language model. Default model "en_core_web_sm" will be used if not provided.')
    parser.add_argument('--min_pct', type=float, default=0.001,
                        help='Minimum percent of all sentences that contains wanted aspects. Default value 0.001 ' \
                             'will be used if not provided.')
    parser.add_argument('--a_pct', type=float, default=0.09,
                        help='Minimum percent of average frequency of amod or acomp tokens that serve as noun modifier.')
    parser.add_argument('--pmi_pct', type=float, default=0.09,
                        help='Minimum percent of average frequency.')
    parser.add_argument('--tri_review_pct', type=float, default=0.09,
                        help='Percentage of reviews two bigrams linked in a trigram must appear together in.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()

    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('N-gram to compute: {}'.format(args.ngrams))
    print('ID field: {}'.format(args.id_field))
    print('Text field: {}'.format(args.text_field))
    print('Rating field: {}'.format(args.rating_field))
    print('Spacy model: {}'.format(args.model))
    print('Minimum percent: {}'.format(args.min_pct))
    print('A percent: {}'.format(args.a_pct))
    print('PMI percent: {}'.format(args.pmi_pct))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))
    nlp = spacy.load(args.model)
    rs = ReviewSents(data=df, id_field=args.id_field, text_field=args.text_field, rating_field=args.rating_field)

    uni, bi, tri = Unigramer(), None, None
    uni.candidate_unigrams(rs, min_pct=args.min_pct, a_pct=args.a_pct)

    if args.ngrams == 2:
        bi = Bigramer(unigramer=uni)
        bi.candidate_bigrams(corpus=rs, min_pct=args.min_pct, pmi_pct=args.pmi_pct)

        # update unigramer
        # uni.update_review_count(bigramer=bi)
    elif args.ngrams == 3:
        bi = Bigramer(unigramer=uni)
        bi.candidate_bigrams(corpus=rs, min_pct=args.min_pct, pmi_pct=args.pmi_pct)

        tri = Trigramer(bi)
        tri.candidate_trigrams(corpus=rs, review_pct=args.tri_review_pct)

        # update unigramer and bigramer
        # uni.update_review_count(bigramer=bi, trigramer=tri)
        # bi.pop_bigrams(trigramer=tri)

    uni_words = [x.strip() for x in uni.unigrams]
    print('There are {} aspects in unigram.'.format(len(uni_words)))
    filename = os.path.splitext(args.data_file)[0] + '__uni.txt'
    print('Output file: {}'.format(filename))
    with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
        for word in uni_words:
            fp.write('{}\n'.format(word))

    if args.ngrams > 1:
        bi_freq = {bigram: len(bi.sent_dict[bigram]) for bigram in bi.bigrams}
        bi_freq = sorted(bi_freq.items(), key=lambda x: x[1], reverse=True)
        filename = os.path.splitext(args.data_file)[0] + '__bi.txt'
        print('Output file: {}'.format(filename))
        with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
            for bigram, freq in bi_freq:
                fp.write('{}: {}\n'.format(bigram, freq))

        if args.ngrams > 2:
            tri_freq = {trigram: len(tri.sent_dict[trigram]) for trigram in tri.trigrams}
            tri_freq = sorted(tri_freq.items(), key=lambda x: x[1], reverse=True)
            filename = os.path.splitext(args.data_file)[0] + '__tri.txt'
            print('Output file: {}'.format(filename))
            with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
                for trigram, freq in tri_freq:
                    fp.write('{}: {}\n'.format(trigram, freq))

    print('Done!')


from textblob import TextBlob

txt = 'good'
txtobj = TextBlob(txt)
b = txtobj.singularize()
c = txtobj.detect_language()