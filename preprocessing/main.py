import sys
sys.path.append('D:\\Developer\\ConsumerReviews')

from preprocessing.ngrams import ReviewSents, Unigramer, Bigramer, Trigramer
from preprocessing.polarizer import Polarizer
import pandas as pd


if __name__ == "__main__":
    df = pd.read_csv('D:\\ny_reviews\\ny_hotel_reviews_12345star_en.csv')
    rs = ReviewSents(df, 'review_id', 'content', 'overall_rating')
    unigramer = Unigramer()
    res = unigramer.candidate_unigrams(rs, min_pct=0.0001)
    print('Unigrams: \n---------\n{}\nSize: {}\n'.format(res, len(res)))
    # uni_freq = {x:unigramer.cnt_dict[x] for x in res}
    print('Unigrams Freq: \n---------\n{}\nSize: {}\n'.format(unigramer.cnt_dict, len(unigramer.cnt_dict)))
    
    # bigramer = Bigramer(unigramer)
    # res = bigramer.candidate_bigrams(rs)
    # print('Bigrams: \n---------\n{}\nSize: {}\n'.format(res, len(res)))
    # print('Bigrams PMI: \n---------\n{}\nSize: {}\n'.format(bigramer.pmi, len(bigramer.pmi)))

    # trigramer = Trigramer(bigramer)
    # res = trigramer.candidate_trigrams(rs)
    # print('Trigrams: \n---------\n{}\nSize: {}\n'.format(res, len(res)))

    # polarizer = Polarizer(unigramer, bigramer, trigramer)
    # polarizer.polarize_aspects(rs)
    # aspects = list(unigramer.unigrams)
    # aspects += list(bigramer.bigrams)
    # aspects += list(trigramer.trigrams)
    # for asp in aspects:
    #     polarizer.print_polarity(asp)
    #     print('\n')
    print('Done!')
