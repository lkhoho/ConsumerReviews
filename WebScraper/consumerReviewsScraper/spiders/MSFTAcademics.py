# Instead of using scrapy spiders, we use Evaluate API provided by
# Microsoft Research Services
#
# Remember to set "MSFTResearchAPIKey" environment variable before using this script.

import argparse
import requests
import csv
import json
import os
import sys


attr_name_mappings = {
    'Id': 'ID',
    'DN': 'Original paper title',
    'Y': 'Publish year',
    'CC': 'Citation count',
    'AA.AfN': 'Author affiliation name',
    'AA.DAuN': 'Original author name',
    'VFN': 'Journal fullname',
    'IA': 'Inverse abstract'
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36 Edg/88.0.705.68',
}

# FIELDS
ID = 'Id'
JOURNAL = 'Journal'
TITLE = 'Title'
AUTHORS = 'Authors'
PUBLISH_YEAR = 'Publish Year'
ABSTRACT = 'Abstract'
CITATION = 'Citation Count'


def parse_cli():
    parser = argparse.ArgumentParser(
        description='Call Microsoft Research Services API to get academics papers of a list of journals. '\
                    'Write results to a CSV file. Refer https://docs.microsoft.com/en-us/academic-services/project-academic-knowledge/reference-evaluate-method '\
                    'to get more info. Set MSFTResearchAPIKey environment variable of your key before using this script.')
    parser.add_argument('working_dir', help='Working directory')
    parser.add_argument('--journal_file', help='Path of journal files, one at a line.')
    parser.add_argument('--journal_list', help='Comma separated journal names.')
    parser.add_argument('--attrs', default='Id,DN,Y,CC,AA.AfN,AA.DAuN,VFN,IA',
                        help='Comma separated attributes to get. Refer '\
                             'https://docs.microsoft.com/en-us/academic-services/project-academic-knowledge/reference-entity-attributes '\
                             'to get attribute meanings.')
    parser.add_argument('-y', '--year_expr', type=str, default='Y=[2001,2021]', 
                        help='Year expression for querying. Refer '\
                             'https://docs.microsoft.com/en-us/academic-services/project-academic-knowledge/reference-query-expression-syntax '\
                             'for supported format and more details.')
    parser.add_argument('--batch_size', type=int, default=500, help='# of records in result of one API call.')
    parser.add_argument('-o', '--output_file', default='output.csv',
                        help='Output filename. Default filename "output.csv" will be used if not provided.')
    return parser.parse_args()


def construct_abstract(inverse_abstract: dict) -> str:
    """
    Reconstruct paper abstract from inverse abstract, whose keys are words and values are indices of abstract.
    """

    size = inverse_abstract['IndexLength']
    result = ['' for _ in range(size)]
    for word, indices in inverse_abstract['InvertedIndex'].items():
        for index in indices:
            result[index] = word
    return ' '.join(result).lower()


def get_authors(entity: dict) -> str:
    values = []
    for aa in entity['AA']:
        values.append(aa['DAuN'] + '@' + aa.get('AfN', ''))
    return ';'.join(values)


if __name__ == '__main__':
    args = parse_cli()
    print('Working directory: {}'.format(args.working_dir))

    if args.journal_file is not None:
        print('Journal file: {}'.format(args.journal_file))
        with open(args.journal_file) as fp:
            journals = [x.strip().lower() for x in fp]
    elif args.journal_list is not None:
        print('Journal list: {}'.format(args.journal_list))
        journals = [x.strip().lower() for x in args.journal_list.split(',')]
    else:
        sys.exit('Both journal file and list are not provided. Abort.')
    print('# of journals to collect: {}'.format(len(journals)))

    attrs_str = '\n'
    attrs = args.attrs.split(',')
    for attr in attrs:
        attrs_str += '\t{}        - {}\n'.format(attr, attr_name_mappings[attr])
    print('Attributes: {}'.format(attrs_str))

    print('Year expression: {}'.format(args.year_expr))
    print('Batch size: {}'.format(args.batch_size))
    print('Output file: {}'.format(args.output_file))

    key = os.getenv('MSFTResearchAPIKey')
    if key is None:
        sys.exit('MSFTResearchAPIKey is not set. Abort.')

    print('\nStarting scrape papers ...')
    expr_str = "And(Composite(J.JN=='{journal_name}'), %s)" % args.year_expr
    payload = {
        'subscription-key': key,
        'attributes': args.attrs,
        'orderby': '{}:desc'.format('Y'),  # order by publish year
        'count': args.batch_size
    }

    field_names = [ID, JOURNAL, TITLE, AUTHORS, PUBLISH_YEAR, ABSTRACT, CITATION]
    fp = open(os.sep.join([args.working_dir, args.output_file]), 'w', newline='', encoding='utf-8')
    fpe = open(os.sep.join([args.working_dir, 'errors.json']), 'w', encoding='utf-8')
    writer = csv.DictWriter(fp, fieldnames=field_names)
    writer.writeheader()

    incorrect_data = []
    for jou in journals:
        offset = 0
        while True:
            payload['offset'] = offset
            payload['expr'] = expr_str.format(journal_name=jou)

            res = requests.get(
                'https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate', 
                params=payload, headers=headers)
            data = res.json()  # use encoding and JSON decoder in requests
            offset += args.batch_size

            if len(data['entities']) == 0:
                print('Journal [{}] finished.'.format(jou))
                break

            for ent in data['entities']:
                if ('IA' not in ent):
                    continue
                try:
                    abstract = construct_abstract(ent['IA'])
                    writer.writerow({
                        ID: ent['Id'],
                        PUBLISH_YEAR: ent['Y'],
                        JOURNAL: ent['VFN'],
                        CITATION: ent['CC'],
                        AUTHORS: get_authors(ent),
                        ABSTRACT: abstract,
                        TITLE: ent['DN']
                    })
                except Exception as exc:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    filename = exc_tb.tb_frame.f_code.co_filename
                    line_number = exc_tb.tb_lineno
                    print('ERROR: {}\nType: {}\nFile: {}\nLine: {}'.format(
                        exc_obj, exc_type, filename, line_number))
                    incorrect_data.append(ent)

    json.dump(incorrect_data, fpe)
    fp.close()
    fpe.close()
    print('Done!')
