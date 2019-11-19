import os
import logging
import enum
import pandas as pd
from typing import Union
from sklearn.utils import resample
from airflow.models import Variable


class RebalanceMode(enum.IntEnum):
    """
    Sample rebalance modes.
    """

    OVER_SAMPLING = '1'
    DOWN_SAMPLING = '2'


def rebalance_reviews(upstream_task: str, 
                      label_field: str, 
                      mode: RebalanceMode, 
                      positive_label: Union[int, str], 
                      negative_label: Union[int, str],
                      random_state=41, 
                      include_index=False, 
                      **context):
    """
    Perform over-sampling or down-sampling to balance positive and negative reviews.
    :param upstream_task: Upstream task ID.
    :param label_field: Column of target variable name.
    :param mode: Mode of rebalance: over-sampling or down-sampling.
    :param positive_label: Label for positive samples.
    :param negative_label: Label for negative samples.
    :param random_state: Seed of random number. A prime number is preferred. Default is 41.
    :param include_index: Should include index when saving dataframe.
    :param context: Jinja template variables in Airflow.
    :return: Name of data file that has been re-sampled.
    """

    working_dir = Variable.get('working_dir')
    os.makedirs(working_dir, exist_ok = True)
    logging.info('Working dir=' + working_dir)
    logging.info('Upstream task=' + upstream_task)
    logging.info('Target variable=' + label_field)
    logging.info('Rebalance mode={}'.format(mode))
    logging.info('Positive lable={}. Negative label={}'.format(positive_label, negative_label))

    # if upstream_task is None:
    #     logging.info('Data pathname=' + data_pathname)
    #     df = pd.read_csv(data_pathname)
    # else:
    filename = context['ti'].xcom_pull(task_ids=upstream_task)
    logging.info('Input filename=' + filename)
    df = pd.read_csv(os.sep.join([working_dir, context['ds_nodash'], filename]))

    value_counts = df[label_field].value_counts(sort=True, ascending=False)
    logging.info('Counts before re-sampling={}'.format(value_counts))
    if value_counts[positive_label] >= value_counts[negative_label]:
        major_label = positive_label
        minor_label = negative_label
    else:
        major_label = negative_label
        minor_label = positive_label
    df_major = df[df[label_field] == major_label]
    df_minor = df[df[label_field] == minor_label]

    if mode is RebalanceMode.DOWN_SAMPLING:
        df_resampled = resample(df_major, replace=True, n_samples=df_minor.shape[0], random_state=random_state)
        df_result = pd.concat([df_resampled, df_minor])
    elif mode is RebalanceMode.OVER_SAMPLING:
        df_resampled = resample(df_minor, replace=True, n_samples=df_major.shape[0], random_state=random_state)
        df_result = pd.concat([df_resampled, df_major])
    else:
        raise ValueError('Unknown rebalance mode: {}'.format(mode))
    value_counts = df_result[label_field].value_counts()
    logging.info('Counts after re-sampling={}'.format(value_counts))
    pos = data_pathname.rfind('/')
    filename = data_pathname[pos + 1:]
    pos = filename.rfind('.')
    filename = filename[:pos] + '__balanced.csv'
    save_path = os.sep.join([working_dir, context['ds_nodash']])
    os.makedirs(save_path, exist_ok=True)
    df_result.to_csv(save_path + os.sep + filename, index=include_index)

    return filename
