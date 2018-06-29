import numpy as np
from utils.logging import get_logger


class VarianceCalculator(object):
    """
    Calculate variance on values of list of given fields.
    """

    log = get_logger('VarianceCalculator')

    def __init__(self, fields_to_calculate=None, output_field='variance'):
        self.fields = fields_to_calculate if fields_to_calculate is not None else []
        self.output_field = output_field
        self.variance = []

    def preprocess(self, dataframe, save_to_csv=False, file_path=None, index=False):
        for _, row in dataframe.iterrows():
            values = filter(lambda x: x != -1, [row[field] for field in self.fields])
            self.variance.append(np.var(values))
        dataframe[self.output_field] = self.variance
        if save_to_csv:
            self.log.info('Saving data to ' + file_path)
            dataframe.to_csv(file_path, index=index)
        return dataframe
