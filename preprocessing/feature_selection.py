import os
import re
import itertools
import logging
import operator
import simplejson as json
import pandas as pd
from collections import Counter, OrderedDict
from enum import IntEnum
from sklearn.feature_selection import chi2, SelectKBest
from airflow.models import Variable
from preprocessing.lemmatization import LemmatizationMode


class FeatureSelectionMode(IntEnum):
    """
    Feature selection modes.
    """

    FREQUENCY = 1
    CHI_SQUARE = 2


class FeatureRankingMode(IntEnum):
    """
    Modes of ranking features.
    """

    FREQUENCY = 1
    CHI_SQUARE = 2


def feature_ranking(upstream_task: str, 
                    mode: FeatureRankingMode, 
                    text_field: str, 
                    label_field: str, 
                    **context):
    """
    Rank features by given mode, and save the features in descending order.
    :param upstream_task: Upstream task ID. If this is the first task in workflow, pass in None.
    :param mode: Mode of how features are ranked. Supported modes: frequency and chi-square.
    :param text_field: Name of text column.
    :param label_field: Name of target variable column.
    :param context: Jinja template variables in Airflow.
    :return: File name of ranked features with scores, as a dict, in descending order.
    """

    task_instance = context['ti']
    working_dir = Variable.get('working_dir')
    os.makedirs(working_dir, exist_ok=True)
    logging.info('upstream_task=' + upstream_task)
    filename = task_instance.xcom_pull(task_ids=upstream_task)

    # filename = 'bellagio__lemvocabulary.csv'
    # filename = 'bellagio__balanced__lemvocabulary__tfidf.csv'

    logging.info('Filename=' + filename)
    df = pd.read_csv(os.sep.join([working_dir, context['ds_nodash'], filename]))
    logging.info('Dataframe shape={}'.format(df.shape))

    if mode is FeatureRankingMode.FREQUENCY:
        texts = []
        for lemmatized in df[text_field]:
            lemmatized = lemmatized[1:-1].replace('\'', '').split(', ')
            texts.extend(lemmatized)
        values = Counter(texts)
    elif mode is FeatureRankingMode.CHI_SQUARE:
        chi2_selector = _chi_square_feature_selection(df, label_field, num_features=df.shape[1] + 1)
        logging.info('chi2_selector={}'.format(chi2_selector))
        scores = chi2_selector.scores_.tolist()
        values = {index: score for index, score in enumerate(scores)}
        values = sorted(values.items(), key=lambda kv: kv[1], reverse=True)
        values = OrderedDict(values)
    else:
        raise ValueError('Unsupported feature ranking mode: {}'.format(mode))

    pos = filename.rfind('.')
    filename = filename[:pos] + '__rank{}.json'.format(mode.name)
    save_path = os.sep.join([working_dir, context['ds_nodash']])
    os.makedirs(save_path, exist_ok=True)
    with open(save_path + os.sep + filename, 'w') as fp:
        json.dump(values, fp)

    return filename


def feature_selection(lem_mode: LemmatizationMode, feature_mode: FeatureSelectionMode, num_features: list,
                      store: str, working_dir: str, include_index: bool, **context):
    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    input_files = [xcom['output_files']
                   for xcom in context['task_instance'].xcom_pull(task_ids=[
                    'process_all_words__{}_{}'.format(store, str(lem_mode)),
                    'process_nouns__{}_{}'.format(store, str(lem_mode)),
                    'transform_objective__{}_{}'.format(store, str(lem_mode))])]
    input_files = list(itertools.chain(*input_files))
    logging.info('Input files=' + str(input_files))
    dir_path = os.path.sep.join([working_dir, 'feature_selection'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    regex = r'.+{}_{}_(?P<label>[A-Z_]+)_(?P<nlp>[a-z]+)_.+\.csv'.format(store, str(lem_mode))
    output_files = []
    for file in input_files:
        label = re.match(regex, file).group('label')
        nlp = re.match(regex, file).group('nlp')
        df_for_ranking = pd.read_csv(working_dir + os.path.sep + file[:file.rindex(os.path.sep)] + os.path.sep +
                                     '_'.join([store, str(lem_mode), label, nlp]) + '_tfidf.csv')
        df = pd.read_csv(working_dir + os.path.sep + file)
        if feature_mode is FeatureSelectionMode.FREQUENCY:
            selected_features = _frequency_feature_selection(df_for_ranking, label)
            for num in num_features:
                features = selected_features[:num]
                features.append(label)
                result = df[features]
                filename = file[file.rindex(os.path.sep) + 1:file.rindex('.')] + '_{}_{}.csv'\
                    .format(str(feature_mode), str(num))
                output_files.append(filename)
                result.to_csv(os.path.sep.join([dir_path, filename]), index=include_index)
        elif feature_mode is FeatureSelectionMode.CHI_SQUARE:
            for num in num_features:
                chi2_selector = _chi_square_feature_selection(df_for_ranking, label, num)
                result = df.iloc[:, chi2_selector.get_support(indices=True)]
                result.loc[:, label] = df[label]
                filename = file[file.rindex(os.path.sep) + 1:file.rindex('.')] + '_{}_{}.csv'\
                    .format(str(feature_mode), str(num))
                output_files.append(filename)
                result.to_csv(os.path.sep.join([dir_path, filename]), index=include_index)
    return {
        'input_files': input_files,
        'output_files': [os.path.sep.join(['feature_selection', filename]) for filename in output_files]
    }


def _frequency_feature_selection(data_frame, label) -> list:
    frequency = {}
    for feature in data_frame:
        if feature == label:
            continue
        values = data_frame[feature]
        frequency[feature] = len(data_frame[values > 0.0])
    return [x[0] for x in sorted(frequency.items(), key=operator.itemgetter(1), reverse=True)]


def _chi_square_feature_selection(data_frame, label, num_features):
    y = data_frame[label]
    X = data_frame.drop(label, axis=1)
    if num_features > X.shape[1]:
        num_features = 'all'
    logging.info('num_features={}'.format(num_features))
    chi2_selector = SelectKBest(chi2, k=num_features)
    chi2_selector.fit(X, y)
    return chi2_selector
