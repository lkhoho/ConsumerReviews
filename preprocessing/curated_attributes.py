import os
import numpy as np
import scipy.stats as stats
import pandas as pd
from utils.logging import get_logger

log = get_logger(__name__)

site_experience_ratings = ['SITE_SHIPPING_OPTIONS', 'SITE_SHIPPING_CHARGES', 'SITE_EASE_OF_FINDING',
                           'SITE_PRICE_RELATIVE_TO_OTHER', 'SITE_CLARITY_PRODUCT_INFO',
                           'SITE_CHARGES_STATED_CLEARLY', 'SITE_SITE_DESIGN', 'SITE_PRODUCT_SELECTION',
                           'SITE_CHECKOUT_SATISFACTION']

predefined_curated_attributes = {
    'variance': {
        'depend_on_fields': site_experience_ratings,
        'output_field': 'VARIANCE'
    },
    'skewness': {
        'depend_on_fields': site_experience_ratings,
        'output_field': 'SKEWNESS'
    },
    'kurtosis': {
        'depend_on_fields': site_experience_ratings,
        'output_field': 'KURTOSIS'
    },
    'range': {
        'depend_on_fields': site_experience_ratings,
        'output_field': 'RANGE'
    }
}


def _calculate_variance(dataframe, fields_to_calculated, output_field):
    fields = fields_to_calculated if fields_to_calculated is not None else []
    variance = []
    for _, row in dataframe.iterrows():
        values = list(filter(lambda x: x is not None, [row[field] for field in fields]))
        variance.append(np.var(values))
    dataframe[output_field] = variance
    log.info('Calculating variance finished.')
    return dataframe


def _calculate_kurtosis(dataframe, fields_to_calculated, output_field):
    fields = fields_to_calculated if fields_to_calculated is not None else []
    kurtosis = []
    for _, row in dataframe.iterrows():
        values = list(filter(lambda x: x is not None, [row[field] for field in fields]))
        kurtosis.append(stats.kurtosis(np.array(values)))
    dataframe[output_field] = kurtosis
    log.info('Calculating kurtosis finished.')
    return dataframe


def _calculate_skewness(dataframe, fields_to_calculated, output_field):
    fields = fields_to_calculated if fields_to_calculated is not None else []
    skewness = []
    for _, row in dataframe.iterrows():
        values = list(filter(lambda x: x is not None, [row[field] for field in fields]))
        skewness.append(stats.skew(np.array(values)))
    dataframe[output_field] = skewness
    log.info('Calculating skewness finished.')
    return dataframe


def _calculate_range(dataframe, fields_to_calculated, output_field):
    fields = fields_to_calculated if fields_to_calculated is not None else []
    _range = []
    for _, row in dataframe.iterrows():
        values = list(filter(lambda x: x is not None, [row[field] for field in fields]))
        _range.append(max(values) - min(values))
    dataframe[output_field] = _range
    log.info('Calculating range finished.')
    return dataframe


def compute_curated_attributes(attributes_to_calculate, store, working_dir, include_index, **kwargs):
    exec_date = kwargs['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    file_name = store + '__original__.csv'
    df = pd.read_csv(working_dir + os.sep + file_name)
    for attr_name, attr_prop in attributes_to_calculate.items():
        depend_fields = attr_prop['depend_on_fields']
        if attr_name.lower() == 'variance':
            output_field = attr_prop['output_field']
            df = _calculate_variance(df, depend_fields, output_field)
        if attr_name.lower() == 'skewness':
            output_field = attr_prop['output_field']
            df = _calculate_skewness(df, depend_fields, output_field)
        if attr_name.lower() == 'kurtosis':
            output_field = attr_prop['output_field']
            df = _calculate_kurtosis(df, depend_fields, output_field)
        if attr_name.lower() == 'range':
            output_field = attr_prop['output_field']
            df = _calculate_range(df, depend_fields, output_field)
    file_name = store + '__curated__.csv'
    df.to_csv(working_dir + os.path.sep + file_name, index=include_index)
