import argparse
import os
import pandas as pd


def parse_cli():
    parser = argparse.ArgumentParser(description='Generate set A, B, C based on two word lists.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('a_file', help='Path of file for set A. Each word should in {word: value} format.')
    parser.add_argument('b_file', help='Path of file for set B. Each word should in {word: value} format.')
    parser.add_argument('k', type=int, help='# of words to choose from set A and B.')
    parser.add_argument('tfidf', help='TFIDF file. Only CSV format is supported.')
    parser.add_argument('--label', default='posneg', 
                        help='Optional label field in dataframe. Default value "posneg" will be used if not provided.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    
    print('Working directory: {}'.format(args.working_dir))
    print('Set A file: {}'.format(args.a_file))
    print('Set B file: {}'.format(args.b_file))
    print('K: {}'.format(args.k))
    print('TFIDF file: {}'.format(args.tfidf))
    print('Label field: {}'.format(args.label))

    df = pd.read_csv(os.sep.join([args.working_dir, args.tfidf]))
    print('Dataframe shape: {}'.format(df.shape))
    
    data_a = []  # A
    count = 0
    with open(os.sep.join([args.working_dir, args.a_file])) as fp:
        for line in fp:
            if count < args.k:
                arr = line.split(':')
                data_a.append(arr[0].strip())
                count += 1
    print('{} word-value pairs are read from set A file.'.format(len(data_a)))

    data_b = []  # B
    count = 0
    with open(os.sep.join([args.working_dir, args.b_file])) as fp:
        for line in fp:
            if count < args.k:
                arr = line.split(':')
                data_b.append(arr[0].strip())
                count += 1
    print('{} word-value pairs are read from set B file.'.format(len(data_b)))
    
    set_common = set(data_a) & set(data_b)
    len_common = len(set_common)
    print('Size of C: {}'.format(len_common))
    filename = os.sep.join([args.working_dir, 'C.txt'])
    with open(filename, 'w') as fp:
        for word in set_common:
            fp.write(word)
            fp.write('\n')
    print('Write common words to {}'.format(filename))

    data_a = [x for x in data_a if x not in set_common]  # A = A - C
    print('Size of A - C: {}'.format(len(data_a)))

    data_b = [x for x in data_b if x not in set_common]  # B = B - C
    print('Size of B - C: {}'.format(len(data_b)))
    
    filename = os.path.splitext(args.tfidf)[0] + '__filtered_A.csv'
    print('Output file: {}'.format(filename))
    cols = data_a[:len_common]
    cols.append(args.label)
    df_a = df[cols]
    print('filtered_A shape: {}'.format(df_a.shape))
    df_a.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    
    filename = os.path.splitext(args.tfidf)[0] + '__filtered_B.csv'
    print('Output file: {}'.format(filename))
    cols = data_b[:len_common]
    cols.append(args.label)
    df_b = df[cols]
    print('filtered_B shape: {}'.format(df_b.shape))
    df_b.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    
    filename = os.path.splitext(args.tfidf)[0] + '__filtered_C.csv'
    print('Output file: {}'.format(filename))
    cols = list(set_common)
    cols.append(args.label)
    df_c = df[cols]
    print('filtered_C shape: {}'.format(df_c.shape))
    df_c.to_csv(os.sep.join([args.working_dir, filename]), index=False)

    print('Done!')
