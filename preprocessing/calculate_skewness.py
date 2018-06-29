import numpy as np
import scipy.stats as stats
from utils.logging import get_logger


class SkewnessCalculator(object):
    """
    Calculate skewness on values of list of given fields.
    """

    log = get_logger('SkewnessCalculator')

    def __init__(self, fields_to_calculate=None, output_field='skewness'):
        self.fields = fields_to_calculate if fields_to_calculate is not None else []
        self.output_field = output_field
        self.skewness = []

    def preprocess(self, dataframe, save_to_csv=False, file_path=None, index=False):
        for _, row in dataframe.iterrows():
            values = filter(lambda x: x != -1, [row[field] for field in self.fields])
            self.skewness.append(stats.skew(np.array(values)))
        dataframe[self.output_field] = self.skewness
        if save_to_csv:
            self.log.info('Saving data to ' + file_path)
            dataframe.to_csv(file_path, index=index)
        return dataframe
