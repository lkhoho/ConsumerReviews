import os
import logging
import pandas as pd
from sklearn import utils


def over_sampling_reviews(target_fields: list, store, working_dir, include_index, **context):
    """
    Perform over-sampling to balance positive and negative samples.

    :param target_fields: fields that over-sampling bases on
    :param store: store name of dataset
    :param working_dir: directory to save result(s)
    :param include_index: should include indices of samples in resulting dataset
    :param context: task instance context
    :return: paths of resulting files relative to project root
    """

    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    input_file = context['task_instance'].xcom_pull(task_ids='curate__{}'.format(store))['output_files'][0]
    logging.info('Input file=' + input_file)
    logging.info('Target fields=' + str(target_fields))
    df = pd.read_csv(working_dir + os.sep + input_file)
    output_files = []
    for field in target_fields:
        value_counts = df[field].value_counts()
        logging.info('Value counts before over-sampling for field {}: pos={}, neg={}'.format(
            field, value_counts['pos'], value_counts['neg']))
        majority = 'pos' if value_counts['pos'] > value_counts['neg'] else 'neg'
        minority = 'neg' if majority == 'pos' else 'pos'
        logging.info('Majority: {}, minority: {}'.format(majority, minority))
        df_majority = df[df[field] == majority]
        df_minority = df[df[field] == minority]
        df_minority_resampled = utils.resample(df_minority, replace=True, n_samples=df_majority.shape[0],
                                               random_state=41)
        df_oversampled = pd.concat([df_majority, df_minority_resampled])
        fields_to_drop = list(filter(lambda x: x != field, target_fields))
        df_oversampled.drop(fields_to_drop, axis=1, inplace=True)
        value_counts = df_oversampled[field].value_counts()
        assert value_counts['pos'] + value_counts['neg'] == df_oversampled.shape[0]
        logging.info('Value counts after over-sampling for field: {}: pos={}, neg={}\n'.format(
            field, value_counts['pos'], value_counts['neg']))
        dir_path = os.path.sep.join([working_dir, 'balanced'])
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_name = store + '__' + field + '.csv'
        df_oversampled.to_csv(os.path.sep.join([dir_path, file_name]), index=include_index)
        output_files.append(os.path.sep.join(['balanced', file_name]))
    return {
        'input_files': [input_file],
        'output_files': output_files
    }
