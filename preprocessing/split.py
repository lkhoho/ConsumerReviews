import os
import functools
import enum
import pandas as pd
from utils.logging import get_logger

log = get_logger(__name__)


class RangeValueError(ValueError):
    pass


@enum.unique
class RangeType(enum.IntEnum):
    """
    Types of range (close-close, close-open, open-close, open-open)
    """

    CLOSE_CLOSE = 1
    CLOSE_OPEN = 2
    OPEN_CLOSE = 3
    OPEN_OPEN = 4

    def __str__(self):
        return self.name


class Range(object):
    """
    Represent a mathematical range, having lower-bound, upper-bound, and
    range type (close-close, close-open, open-close, open-open).
    """

    def __init__(self, lowerBound, upperBound, rangeType):
        super().__init__()
        self.lb = lowerBound
        self.ub = upperBound
        self.type = rangeType

    def __str__(self):
        template = '{sym1}{lb},{ub}{sym2}'
        if self.type is RangeType.CLOSE_CLOSE:
            return template.format(sym1='[', sym2=']', lb=self.lb, ub=self.ub)
        elif self.type is RangeType.OPEN_CLOSE:
            return template.format(sym1='(', sym2=']', lb=self.lb, ub=self.ub)
        elif self.type is RangeType.CLOSE_OPEN:
            return template.format(sym1='[', sym2=')', lb=self.lb, ub=self.ub)
        elif self.type is RangeType.OPEN_OPEN:
            return template.format(sym1='(', sym2=')', lb=self.lb, ub=self.ub)
        else:
            raise RangeValueError('Unknown RangeType: ' + str(self.type))


predefined_range_definitions = {
    'VARIANCE': [Range('-inf', 1.0, RangeType.OPEN_CLOSE),
                 Range(1.0, 'inf', RangeType.OPEN_OPEN)],

    'SKEWNESS': [Range('-inf', 0.0, RangeType.OPEN_OPEN),
                 Range(0.0, 0.0, RangeType.CLOSE_CLOSE),
                 Range(0.0, 'inf', RangeType.OPEN_OPEN)],

    'KURTOSIS': [Range('-inf', 3.0, RangeType.OPEN_CLOSE),
                 Range(3.0, 'inf', RangeType.OPEN_OPEN)]
}


def split_dataset(range_definition, store, working_dir, include_index, **kwargs):
    exec_date = kwargs['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    if not _check_ranges(range_definition):
        log.error('Range definition is invalid.')
        return
    file_name = store + '__curated__.csv'
    df = pd.read_csv(working_dir + os.path.sep + file_name)
    for range_name, range_values in range_definition.items():
        _split_range_helper(df, range_name, range_values, store, working_dir, include_index)


def _check_ranges(range_definition: dict):
    result = True
    ranges = [item for sublist in range_definition.values() for item in sublist]
    for _range in ranges:
        if _range.type is RangeType.CLOSE_CLOSE:
            result = result and (_range.lb == _range.ub)

        if _range.lb == '-inf':
            result = result and (_range.type is RangeType.OPEN_CLOSE or _range.type is RangeType.OPEN_OPEN)
        elif _range.lb == 'inf':
            result = False

        if _range.ub == 'inf':
            result = result and (_range.type is RangeType.CLOSE_OPEN or _range.type is RangeType.OPEN_OPEN)
        elif _range.ub == '-inf':
            result = False

        result = result and (float(_range.ub) >= float(_range.lb))
    return result


def _split_range_helper(dataframe, range_name, range_values, store, working_dir, include_index):
    results = {}
    data_name_format = '{lb}_{ub}'
    values = dataframe[range_name]
    for _range in range_values:
        lb, ub = _range.lb, _range.ub
        name = data_name_format.format(lb=lb, ub=ub)
        if _range.type is RangeType.CLOSE_CLOSE:  # range is a number
            data = dataframe[values == lb]
            results[name] = data
        elif _range.type is RangeType.OPEN_CLOSE:
            data = dataframe[(float(lb) < values) & (values <= ub)]
            results[name] = data
        elif _range.type is RangeType.CLOSE_OPEN:
            data = dataframe[(lb <= values) & (values < float(ub))]
            results[name] = data
        elif _range.type is RangeType.OPEN_OPEN:
            data = dataframe[(float(lb) < values) & (values < float(ub))]
            results[name] = data
        else:
            raise RangeValueError('Unknown range type: ' + str(_range.type))
    num_rows = functools.reduce(lambda x, y: x + y, [v.shape[0] for (k, v) in results.items()])
    assert num_rows == dataframe.shape[0], \
        'Number of rows (={}) in splits is inconsistent with number of rows (={}) ' \
        'in dataframe.'.format(num_rows, dataframe.shape[0])
    for k, v in results.items():
        fname = '{}_{}'.format(range_name, k)
        score_names = ['SHOP_AGAIN', 'TO_RECOMMEND', 'SATISFACTION']
        file_name = working_dir + os.path.sep + store + '__split__{}__' + fname + '.csv'
        v1 = v.drop([score_names[1], score_names[2]], axis=1)
        v1.to_csv(file_name.format(score_names[0]),
                  index=include_index)
        v2 = v.drop([score_names[0], score_names[2]], axis=1)
        v2.to_csv(file_name.format(score_names[1]),
                  index=include_index)
        v2 = v.drop([score_names[0], score_names[1]], axis=1)
        v2.to_csv(file_name.format(score_names[2]),
                  index=include_index)
