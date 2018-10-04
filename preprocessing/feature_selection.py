import os
import enum
import re
import itertools
import logging
import operator
import pandas as pd
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from preprocessing.lemmatization import LemmatizationMode


@enum.unique
class FeatureSelectionMode(enum.Enum):
    """
    Feature selection modes.
    """

    FREQUENCY = 'freq'
    CHI_SQUARE = 'chi2'

    def __str__(self):
        return self.value


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
    regex = r'.+{}_{}_(?P<label>[A-Z_]+)_.+\.csv'.format(str(store), str(lem_mode))
    output_files = []
    for file in input_files:
        df = pd.read_csv(working_dir + os.path.sep + file)
        label = re.match(regex, file).group('label')
        if feature_mode is FeatureSelectionMode.FREQUENCY:
            selected_features = _frequency_feature_selection(df, label)
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
                selected_df = _chi_square_feature_selection(df, label, num)
                selected_df[label] = df[label]
                filename = file[file.rindex(os.path.sep) + 1:file.rindex('.')] + '_{}_{}.csv'\
                    .format(str(feature_mode), str(num))
                output_files.append(filename)
                selected_df.to_csv(os.path.sep.join([dir_path, filename]), index=include_index)
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


def _chi_square_feature_selection(data_frame, label, num_features) -> pd.DataFrame:
    y = data_frame[label]
    X = data_frame.drop(label, axis=1)
    if num_features > X.shape[1]:
        num_features = 'all'
    chi2_selector = SelectKBest(chi2, k=num_features)
    chi2_selector.fit(X, y)
    idx_selected = chi2_selector.get_support(indices=True)
    return data_frame.iloc[:, idx_selected]
