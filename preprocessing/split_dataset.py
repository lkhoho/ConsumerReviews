import os
import functools
import enum
from utils.logging import get_logger


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


class RangeSplitter(object):
    """
    Split dataset by given set of range definitions.
    """

    log = get_logger('RangeSplitter')

    def __init__(self, store_name, **range_definitions):
        super().__init__()
        self.store_name = store_name
        self.range_definitions = range_definitions
        self._ranges_valid = self._check_ranges()
        self._results = {}
        if not self._ranges_valid:
            raise RangeValueError('Passed in invalid ranges. Range_definition={}'.format(str(self.range_definitions)))

    @property
    def flattened_results(self):
        """
        Get split results as dict.
        :return: split results, e.g.
        {
         'MidwayUSA_split_variance_-1.0_1.0': df1,
         'MidwayUSA_split_variance_1.0_2.0': df2, ...}
        }
        """

        results = {}
        for k1, v1 in self._results.items():
            name = '{store}_split_{range_name}_'.format(store=self.store_name, range_name=k1)
            for k2, v2 in v1.items():
                name += k2
                results[name] = v2
        return results

    @property
    def structured_results(self):
        """
        Get split results as multi-level dict.
        :return: split results, e.g.
        {
         'variance': {'-1.0_1.0': df1,
                      '1.0_2.0': df2, ...}
        }
        """

        return self._results

    def preprocess(self, dataframe, flatten=True, save_to_csv=False, path_to_save=None, index=False):
        for range_name, ranges in self.range_definitions.items():
            self._split_by_range(dataframe, range_name, ranges, save_to_csv, path_to_save, index)
        return self.flattened_results if flatten else self.structured_results

    def save_to_csv(self, path_to_save=None, index=False):
        self.log.info('Saving data splits to ' + path_to_save)
        for k, v in self.flattened_results.items():
            v.to_csv(os.path.sep.join(path_to_save, k + '.csv'), index=index)

    def _check_ranges(self):
        result = True
        for ranges in [v for (_, v) in self.range_definitions.items()]:
            for _range in ranges:
                if _range.type is RangeType.CC:
                    result = result and (_range.lb == _range.ub)

                if _range.lb == '-inf':
                    result = result and (_range.type is RangeType.OC or _range.type is RangeType.OO)
                elif _range.lb == 'inf':
                    result = False

                if _range.ub == 'inf':
                    result = result and (_range.type is RangeType.CO or _range.type is RangeType.OO)
                elif _range.lb == '-inf':
                    result = False

                result = result and (float(_range.ub) >= float(_range.lb))
        return result

    def _split_by_range(self, dataframe, range_name, ranges, save_to_csv, path_to_save, index):
        if self._results[range_name] is None:
            self._results[range_name] = {}
        data_name_format = '{lb}_{ub}'
        range_values = dataframe[range_name]
        for _range in ranges:
            lb, ub = _range.lb, _range.ub
            name = data_name_format.format(lb=lb, ub=ub)
            if _range.type is RangeType.CLOSE_CLOSE:  # range is a number
                data = dataframe[range_values == lb]
                self._results[range_name][name] = data
            elif _range.type is RangeType.OPEN_CLOSE:
                data = dataframe[(float(lb) < range_values) & (range_values <= ub)]
                self._results[range_name][name] = data
            elif _range.type is RangeType.CLOSE_OPEN:
                data = dataframe[(lb <= range_values) & (range_values < float(ub))]
                self._results[range_name][name] = data
            elif _range.type is RangeType.OPEN_OPEN:
                data = dataframe[(float(lb) < range_values) & (range_values < float(ub))]
                self._results[range_name][name] = data
            else:
                raise RangeValueError('Unknown range type: ' + str(_range.type))
        num_rows = functools.reduce(lambda x, y: x + y, [v.shape[0] for (k, v) in self._results[range_name]])
        assert num_rows == dataframe.shape[0], 'Number of rows in splits [{}] is inconsistent with number of rows ' \
                                               'in dataframe [{}]'.format(num_rows, dataframe.shape[0])
        if save_to_csv:
            self.save_to_csv(path_to_save, index)
