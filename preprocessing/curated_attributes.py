import os
import logging
from typing import List, Union
import numpy as np
import scipy.stats as stats
import pandas as pd


site_experience_ratings = ['SITE_SHIPPING_OPTIONS', 'SITE_SHIPPING_CHARGES', 'SITE_EASE_OF_FINDING',
                           'SITE_PRICE_RELATIVE_TO_OTHER', 'SITE_CLARITY_PRODUCT_INFO',
                           'SITE_CHARGES_STATED_CLEARLY', 'SITE_SITE_DESIGN', 'SITE_PRODUCT_SELECTION',
                           'SITE_CHECKOUT_SATISFACTION']


class CuratedAttribute(object):
    def __init__(self, name: str, depends_on: Union[list, str], output_name: str, **kwargs):
        self.name = name
        self.depends_on = depends_on
        self.output_name = output_name
        self.positive_threshold = kwargs.get('positive_threshold', None)
        self.as_label = kwargs.get('as_label', False)  # may be used as data label or not


def _ensure_type(fields, expected_type: type):
    if fields is not None and not isinstance(fields, expected_type):
        raise TypeError('{} should be type {}, but got {}.'.format(str(fields), expected_type, type(fields)))


def _calculate_variance(dataframe: pd.DataFrame, fields_to_calculate: list, output_field: str):
    _ensure_type(fields_to_calculate, list)
    fields = fields_to_calculate if fields_to_calculate is not None else []
    variance = []
    for _, row in dataframe.iterrows():
        values = list(filter(lambda x: x is not None, [row[field] for field in fields]))
        variance.append(np.var(values))
    dataframe[output_field] = variance
    logging.info('Calculating variance finished.')
    return dataframe


def _calculate_kurtosis(dataframe: pd.DataFrame, fields_to_calculate: list, output_field: str):
    _ensure_type(fields_to_calculate, list)
    fields = fields_to_calculate if fields_to_calculate is not None else []
    kurtosis = []
    for _, row in dataframe.iterrows():
        values = list(filter(lambda x: x is not None, [row[field] for field in fields]))
        kurtosis.append(stats.kurtosis(np.array(values)))
    dataframe[output_field] = kurtosis
    logging.info('Calculating kurtosis finished.')
    return dataframe


def _calculate_skewness(dataframe: pd.DataFrame, fields_to_calculate: list, output_field: str):
    _ensure_type(fields_to_calculate, list)
    fields = fields_to_calculate if fields_to_calculate is not None else []
    skewness = []
    for _, row in dataframe.iterrows():
        values = list(filter(lambda x: x is not None, [row[field] for field in fields]))
        skewness.append(stats.skew(np.array(values)))
    dataframe[output_field] = skewness
    logging.info('Calculating skewness finished.')
    return dataframe


def _calculate_range(dataframe: pd.DataFrame, fields_to_calculate: list, output_field: str):
    _ensure_type(fields_to_calculate, list)
    fields = fields_to_calculate if fields_to_calculate is not None else []
    _range = []
    for _, row in dataframe.iterrows():
        values = list(filter(lambda x: x is not None, [row[field] for field in fields]))
        _range.append(max(values) - min(values))
    dataframe[output_field] = _range
    logging.info('Calculating range finished.')
    return dataframe


def _calculate_label(dataframe: pd.DataFrame, fields_to_calculate: str,
                     positive_threshold: int, output_field: str):
    _ensure_type(fields_to_calculate, str)
    dataframe[output_field] = dataframe[fields_to_calculate] >= positive_threshold
    dataframe[output_field].replace(True, 'pos', inplace=True)
    dataframe[output_field].replace(False, 'neg', inplace=True)
    logging.info('Calculating label {} finished.'.format(fields_to_calculate))
    return dataframe


def compute_curated_attributes(attributes: List[CuratedAttribute], store, working_dir, include_index, **context):
    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    input_file = context['task_instance'].xcom_pull(task_ids=context['task'].upstream_task_ids[-1])['output_files'][0]
    logging.info('Input file=' + input_file)
    df = pd.read_csv(working_dir + os.sep + input_file)
    for attribute in attributes:
        attr_name = attribute.name
        depend_fields = attribute.depends_on
        output_field = attribute.output_name
        if attr_name.lower() == 'variance':
            df = _calculate_variance(df, depend_fields, output_field)
        if attr_name.lower() == 'skewness':
            df = _calculate_skewness(df, depend_fields, output_field)
        if attr_name.lower() == 'kurtosis':
            df = _calculate_kurtosis(df, depend_fields, output_field)
        if attr_name.lower() == 'range':
            df = _calculate_range(df, depend_fields, output_field)
        if attribute.as_label:
            pos_threshold = attribute.positive_threshold
            df = _calculate_label(df, depend_fields, pos_threshold, output_field)
    file_name = store + '.csv'
    df.to_csv(working_dir + os.path.sep + file_name, index=include_index)
    return {
        'input_files': [input_file],
        'output_files': [file_name]
    }
