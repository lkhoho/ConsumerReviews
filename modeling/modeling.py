import os
import re
import logging
import simplejson as json
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score
import matplotlib.pylab as plt
from preprocessing.lemmatization import LemmatizationMode
from preprocessing.feature_selection import FeatureSelectionMode


def kfold_cv(classifier, scorers: list, lem_mode: LemmatizationMode, feature_mode: FeatureSelectionMode,
             store: str, working_dir: str, **context):
    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    input_files = context['task_instance'].xcom_pull(
        task_ids='feature_selection__{}_{}_{}'.format(store, str(lem_mode), str(feature_mode)))['output_files']
    logging.info('Input files=' + str(input_files))
    regex = r'.+{}_{}_(?P<label>[A-Z_]+)_(?P<nlp>\w+_\w+)_\w+_(?P<num_features>\d+)\.csv'\
        .format(str(store), str(lem_mode))
    le = LabelEncoder()
    results = {}
    for file in input_files:
        logging.info('keliu: file=' + file)
        label = re.match(regex, file).group('label')
        nlp = re.match(regex, file).group('nlp')
        num_features = re.match(regex, file).group('num_features')
        logging.info('keliu: label={}, nlp={}, num_features={}'.format(label, nlp, str(num_features)))

        base_key = '__'.join([store, str(lem_mode), label, nlp, str(feature_mode)])
        df = pd.read_csv(working_dir + os.path.sep + file)
        y = le.fit_transform(df[label])
        X = df.loc[:, df.columns != label]
        for scorer in scorers:
            scores = cross_val_score(classifier, X, y, cv=10, scoring=scorer)
            results.setdefault(base_key, {}).setdefault(scorer, {}).setdefault(str(num_features), {})['raw'] = scores.tolist()
            results[base_key][scorer][str(num_features)]['mean'] = scores.mean()
    with open(os.path.sep.join([working_dir, 'results.json']), 'w') as fp:
        json.dump(results, fp)
    # plot_graph(os.path.sep.join([working_dir, 'results.json']), scorers, working_dir)


def plot_graph(data_file: str, scorers: list, working_dir: str):
    with open(data_file) as fp:
        data = json.load(fp)

    dir_path = os.path.sep.join([working_dir, 'figures'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    for graph_name, graph_data in data.items():
        for scorer in scorers:
            fig = plt.figure()
            scorer_data = graph_data[scorer]
            num_features = sorted([int(x) for x in scorer_data.keys()])
            values = [scorer_data[str(x)]['mean'] for x in num_features]
            plt.plot(num_features, values, 'bo')
            plt.ylim(0.0, 1.0)
            plt.xlabel('# of features')
            plt.ylabel('mean of score')
            plt.title(graph_name)
            fig.savefig(os.path.sep.join([dir_path, graph_name + '_' + scorer + '.png']))
    logging.info('Done plotting!')


if __name__ == '__main__':
    plot_graph('/Users/keliu/consumer_reviews_working/20181002/results.json', ['accuracy', 'f1', 'roc_auc'],
               '/Users/keliu/consumer_reviews_working/20181002')
