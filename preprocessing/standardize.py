import argparse
import emoji
import os
import pandas as pd


def parse_cli():
    parser = argparse.ArgumentParser(description='Standardize texts.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of text data file to lemmatize. Supports CSV file only.')
    parser.add_argument('--text_field', type=str, default='content',
                        help='Text field name. Default field name "content" will be used if not provided.')
    parser.add_argument('--output_field', type=str, default='standardized', 
                        help='Output field name. Default field name "standardized" will be used if not provided.')
    parser.add_argument('-r', '--random_state', type=int, default=41,
                        help='Seed of random number. A prime number is preferred. Default is 41.')
    return parser.parse_args()


def standardize_text(df: pd.DataFrame, text_field: str, output_field: str) -> pd.DataFrame:
    """
    Convert all characters to lowercased and remove irrelevant characters, URLs in dataframe.
    :param df: Dataframe that contains texts to be cleaned.
    :param text_field: Name of field that contains texts.
    :param output_field: Name of output text field.
    :return: A pandas dataframe with cleaned texts in either new column of replacing original texts.
    """

    df[output_field] = df[text_field].apply(
        lambda column: emoji.get_emoji_regexp().sub(u'', column)
    )

    df[output_field] = df[output_field].str.lower()

    df[output_field] = df[output_field].str.replace("'m", ' am')
    df[output_field] = df[output_field].str.replace("’m", ' am')
    df[output_field] = df[output_field].str.replace("´m", ' am')

    df[output_field] = df[output_field].str.replace("'ve", ' have')
    df[output_field] = df[output_field].str.replace("’ve", ' have')
    df[output_field] = df[output_field].str.replace("´ve", ' have')

    df[output_field] = df[output_field].str.replace("'d", ' would')
    df[output_field] = df[output_field].str.replace("’d", ' would')
    df[output_field] = df[output_field].str.replace("´d", ' would')

    df[output_field] = df[output_field].str.replace("n't", ' not')
    df[output_field] = df[output_field].str.replace("n’t", ' not')
    df[output_field] = df[output_field].str.replace("n´t", ' not')

    df[output_field] = df[output_field].str.replace("'ll", ' will')
    df[output_field] = df[output_field].str.replace("’ll", ' will')
    df[output_field] = df[output_field].str.replace("´ll", ' will')

    df[output_field] = df[output_field].str.replace("'s", ' is')
    df[output_field] = df[output_field].str.replace("’", ' is')
    df[output_field] = df[output_field].str.replace("´s", ' is')

    df[output_field] = df[output_field].str.replace('air conditioner', 'ac')
    df[output_field] = df[output_field].str.replace('air conditioning', 'ac')
    df[output_field] = df[output_field].str.replace('a.c.', 'ac')
    df[output_field] = df[output_field].str.replace('television', 'tv')
    df[output_field] = df[output_field].str.replace('tvs', 'tv')
    df[output_field] = df[output_field].str.replace('t.v.', 'tv')

    df[output_field] = df[output_field].str.replace('/', ' ')
    df[output_field] = df[output_field].str.replace('\.{1,}', '. ')
    df[output_field] = df[output_field].str.replace('!{1,}', '! ')
    df[output_field] = df[output_field].str.replace('\?{1,}', '? ')
    df[output_field] = df[output_field].str.replace('€+', '')
    df[output_field] = df[output_field].str.replace('[0-9$&~\\()[\]{}<>%\'"“”‘’，;…+\-_=*]+', '')
    df[output_field] = df[output_field].str.replace(r'http\S+', '')
    df[output_field] = df[output_field].str.replace(r'http', '')
    df[output_field] = df[output_field].str.replace(r'@\S+', '')
    df[output_field] = df[output_field].str.replace(r'@', 'at')
    
    df[output_field] = df[output_field].astype(str)

    return df


if __name__ == '__main__':
    args = parse_cli()
    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Text field: {}'.format(args.text_field))
    print('Output field: {}'.format(args.output_field))
    print('Random seed: {}'.format(args.random_state))
    
    df = pd.read_csv(os.sep.join([args.working_dir, args.data_file]))
    print('Dataframe shape={}'.format(df.shape))
    
    df = standardize_text(df, text_field=args.text_field, output_field=args.output_field)
    print('Standardized shape={}'.format(df.shape))

    filename = os.path.splitext(args.data_file)[0] + '__std.csv'
    print('Output file: {}'.format(filename))
    df.to_csv(os.sep.join([args.working_dir, filename]), index=False)
    print('Done!')

