from collections import defaultdict
from nltk.corpus import sentiwordnet as swn
from spacy.lang.en import English
import argparse
import numpy as np
import os
import re
import pandas as pd
import spacy


# list of POS tag belonging to nouns
noun_tags = set(['NN', 'NNP', 'NNS'])

# adjectives which score cannot be retrieved from sentiwordnet
no_score_adjs = []


def parse_cli():
    parser = argparse.ArgumentParser(description='Compute scores for aspect-related adjectives in texts.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Name of original review data file. Supports CSV file only.')
    parser.add_argument('aspect_file', help='Name of extracted aspects data file.')
    parser.add_argument('--id_field', type=str, default='review_id', 
                        help='ID field name in data file. Default field name "review_id" will be used if not provided.')
    parser.add_argument('--text_field', type=str, default='content', 
                        help='Text field name in data file. Default field name "content" will be used if not provided.')
    parser.add_argument('--model', type=str, default='en_core_web_sm', 
                        help='Spacy language model. Default model "en_core_web_sm" will be used if not provided.')
    return parser.parse_args()


def _compute_scores(row, id_field: str, text_field: str, nlp: English, result: pd.DataFrame):
    row_id = row[id_field]
    text = row[text_field]
    doc = nlp(text)
    dep_dict = defaultdict(list)  # dependency dict
    acomp_dict = defaultdict(list)  # acomp dict

    for token in doc:
        dep_dict[token.head].append((token.lemma_, token.tag_, token.dep_, token.head.head))
        if token.dep_ == 'acomp':
            acomp_dict[token.head] = token
    
    for head_word, token in acomp_dict.items():
        for child in filter(lambda x: x.tag_ in noun_tags and x.lemma_ in result.columns, head_word.children):
                dep_dict[child].append((token.lemma_, token.tag_, token.dep_, token.head.head))

    for token, token_tuple in dep_dict.items():
        if token.tag_ in noun_tags and token.lemma_ in result.columns:
            adj_scores = []
            is_negated = False
            for t in token_tuple:
                if t[1] == 'JJ' and (t[2] == 'amod' or t[2] == 'acomp'):
                    synsets = list(swn.senti_synsets(t[0]))
                    if len(synsets) > 0:
                        # synset = synsets[0]  # use the first synset to get scores
                        for syn in synsets:
                            arr = re.split('[.:]', str(syn))
                            if arr[1] == 'a' and arr[2] == '01':
                                synset = syn
                                break
                            elif arr[1] == 's' and arr[2] == '01':
                                synset = syn
                                break
                            else:
                                synset = synsets[0]
                    else:
                        no_score_adjs.append(t)
                        continue
                    pos_score = synset.pos_score()
                    neg_score = synset.neg_score()
                    if pos_score > neg_score:
                        score = pos_score
                    elif pos_score < neg_score:
                        score = neg_score * -1.0
                    else:
                        score = 0.0
                    
                    # find all negation words for current adjectives
                    for child in t[3].children:
                        if child.dep_ == 'neg':
                            is_negated = not is_negated
                    
                    adj_scores.append(score * -1.0 if is_negated else score)
            noun_score = sum(adj_scores)  # noun score is the summation of all related adjective scores
            result.at[row_id, token.lemma_] = noun_score  # fill in noun score into right cell in data frame


if __name__ == '__main__':
    args = parse_cli()
    
    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Aspect file: {}'.format(args.aspect_file))
    print('ID field: {}'.format(args.id_field))
    print('Text field: {}'.format(args.text_field))
    print('Spacy model: {}'.format(args.model))

    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    df_shape = df.shape
    print('Dataframe shape={}'.format(df_shape))

    aspects = set()
    with open(os.sep.join([args.working_dir, args.aspect_file])) as fp:
        for line in fp:
            arr = line.split(':')
            aspects.add(arr[0].strip())
    print('Find {} aspects.'.format(len(aspects)))

    result = pd.DataFrame(data={x: [0.0 for _ in range(df_shape[0])] for x in aspects})
    result = result.set_index(df[args.id_field])
    nlp = spacy.load(args.model)
    df.apply(_compute_scores, axis=1, id_field=args.id_field, text_field=args.text_field, nlp=nlp, result=result)

    print('{} adjectives whose scores cannot be retrieved.'.format(len(no_score_adjs)))
    for t in no_score_adjs:
        print(t)

    filename = os.path.splitext(args.data_file)[0] + '__nounscores.csv'
    print('Result dataframe shape={}'.format(result.shape))
    print('Output file: {}'.format(filename))
    result.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    print('Done!')
