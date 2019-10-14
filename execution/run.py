import logging
import os
import pandas as pd
from random import choices
from preprocessing.cleanup import *

logging.basicConfig(level=logging.INFO)
root = r'D:\Developer\ConsumerReviews'

pos_mapping = {
    'JJ': 'a',
    'JJR': 'a', 
    'JJS': 'a', 
    'NN': 'n', 
    'NNP': 'n', 
    'NNS': 'n', 
    'NNPS': 'n', 
    'RB': 'r',
    'RBR': 'r', 
    'RBS': 'r', 
    'VB': 'v', 
    'VBD': 'v', 
    'VBG': 'v', 
    'VBN': 'v', 
    'VBP': 'v'
}

#def down_sampling(smaller, larger, filename):
#    num = smaller.shape[0]
#    indices = larger.index
#    chosen = list(choices(indices, k=num))
#    result = pd.concat([larger.iloc[indices.isin(chosen)], smaller])
#    result.to_csv(r'C:\Users\lkhoho\OneDrive\Projects\text_reviews\data\{}.csv'.format(filename))
#    print('Done!')

#if dfbep.shape[0] < dfben.shape[0]:
#    smaller = dfbep
#    larger = dfben
#else:
#    smaller = dfben
#    larger = dfbep
#down_sampling(smaller, larger, 'bellagio')

#if dfcip.shape[0] < dfcin.shape[0]:
#    smaller = dfcip
#    larger = dfcin
#else:
#    smaller = dfcin
#    larger = dfcip
#down_sampling(smaller, larger, 'circus')

#if dftip.shape[0] < dftin.shape[0]:
#    smaller = dftip
#    larger = dftin
#else:
#    smaller = dftin
#    larger = dftip
#down_sampling(smaller, larger, 'ti')

if __name__ == '__main__':
    df = pd.read_csv(r'C:\Users\lkhoho\OneDrive\Projects\text_reviews\data\bellagio.csv')
    df['content'] = df['content'].astype(str)
    logging.info('Original shape={}'.format(df.shape))

    #dfbe = df[df['hotel_name'] == 'Bellagio']
    #dfci = df[df['hotel_name'] == 'Circus Circus Hotel, Casino & Theme Park']
    #dfti = df[df['hotel_name'] == 'TI - Treasure Island Hotel and Casino']
    #logging.info('Bellagio dataframe shape={}'.format(dfbe.shape))
    #logging.info('TI dataframe shape={}'.format(dfti.shape))
    #logging.info('Circus dataframe shape={}'.format(dfci.shape))

    ## positives
    #dfbep = dfbe[dfbe['overall_rating'] >= 4]
    #dfcip = dfci[dfci['overall_rating'] >= 4]
    #dftip = dfti[dfti['overall_rating'] >= 4]
    
    ## negatives
    #dfben = dfbe[dfbe['overall_rating'] < 4]
    #dfcin = dfci[dfci['overall_rating'] < 4]
    #dftin = dfti[dfti['overall_rating'] < 4]



    df = standardize_text(df, text_field='content', output_field='content_std')
    logging.info('Standadized shape={}'.format(df.shape))
   
    df = tokenize(df, 'content_std', 'tokens')

    stopwords = set()
    with open(os.sep.join([root, r'resource\stopwords.txt'])) as fp:
        for line in fp.readlines():
            stopwords.add(line.strip())
    logging.info('Stopwords size={}'.format(len(stopwords)))

    df = remove_words(df, 'tokens', stopwords=stopwords)
    
    voc = compute_vocabulary(df, token_field='tokens')
    logging.info('Vocabulary size={}'.format(len(voc)))

    voc_tag = tag_vocabulary(list(voc), lang='eng', pos_mapping=pos_mapping)
    print(voc_tag)