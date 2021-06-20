import argparse
import os


def parse_cli():
    parser = argparse.ArgumentParser(description='Filter frequency/chi2/gini words by aspects.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('data_file', help='Path of frequency/chi2/gini file. Each word should in {word: value} format.')
    parser.add_argument('aspect_file', help='Path of aspect file.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cli()
    
    print('Working directory: {}'.format(args.working_dir))
    print('Data file: {}'.format(args.data_file))
    print('Aspect file: {}'.format(args.aspect_file))
    
    data = dict()
    with open(os.sep.join([args.working_dir, args.data_file])) as fp:
        for line in fp:
            arr = line.split(':')
            data[arr[0].strip()] = float(arr[1].strip())
    print('{} word-value pairs are read.'.format(len(data)))
    
    aspects = set()
    with open(os.sep.join([args.working_dir, args.aspect_file])) as fp:
        for line in fp:
            aspects.add(line.strip())
    print('{} aspects are read.'.format(len(aspects)))
    
    filename = os.path.splitext(args.data_file)[0] + '__filtered.txt'
    print('Output file: {}'.format(filename))
    count = 0
    with open(os.sep.join([args.working_dir, filename]), 'w') as fp:
        for word, value in filter(lambda item: item[0] in aspects, data.items()):
            fp.write('{}: {}\n'.format(word, value))
            count += 1
    print('{} word-value pairs are reserved.'.format(count))
    print('Done!')
