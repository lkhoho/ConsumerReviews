import os
import glob
import enum
import operator
import pandas as pd
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from utils.logging import get_logger

log = get_logger(__name__)


@enum.unique
class FeatureSelectionMode(enum.Enum):
    """
    Feature selection modes.
    """

    FREQUENCY = 'freq'
    CHI_SQUARE = 'chi2'

    def __str__(self):
        return self.value


def feature_selection(lem_mode, feature_mode, num_features, label_name, store, working_dir, include_index, **kwargs):
    exec_date = kwargs['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    wd = os.getcwd()
    os.chdir(working_dir)
    files = glob.glob(store + '__nlp_raw_tfidf__' + str(lem_mode) + '*.csv') \
        + glob.glob(store + '__nlp_nouns_tfidf__' + str(lem_mode) + '*.csv')
    dir_name = working_dir + os.path.sep + str(feature_mode)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    for file in files:
        df = pd.read_csv(working_dir + os.path.sep + file)
        if feature_mode is FeatureSelectionMode.FREQUENCY:
            selected_features = _frequency_feature_selection(df, label_name)
            # log.info('keliu: {}: {}'.format(file, len(selected_features)))
            for num in num_features:
                features = selected_features[:num]
                features.append(label_name)
                result = df[features]
                file_name = dir_name + os.path.sep + file[:file.rindex('.')] + '_{}_{}.csv'\
                    .format(str(feature_mode), str(num))
                result.to_csv(file_name, index=include_index)
        elif feature_mode is FeatureSelectionMode.CHI_SQUARE:
            for num in num_features:
                selected_df = _chi_square_feature_selection(df, label_name, num)
                # log.info('keliu: ' + str(selected_df.shape))
                selected_df[label_name] = df[label_name]
                file_name = dir_name + os.path.sep + file[:file.rindex('.')] + '_{}_{}.csv'\
                    .format(str(feature_mode), str(num))
                selected_df.to_csv(file_name, index=include_index)
    os.chdir(wd)


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
