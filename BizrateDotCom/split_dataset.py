import enum
import re
import operator
import functools
import time
import os
import multiprocessing
import pandas as pd
import numpy as np
import scipy.stats as stats
from pymongo import MongoClient
from common import config

log = config.getLogger(__name__)


@enum.unique
class IntervalType(enum.Enum):
    """
    Type of interval (close-close, close-open, open-close, open-open).
    """

    CC = 1  # close-close, basically a number
    CO = 2  # close-open
    OC = 3  # open-close
    OO = 4  # open-open


class Interval(object):
    """
    Represent a mathematical interval, having lower-bound, upper-bound, and interval type (close-close, close-open,
    open-close, open-open).
    """

    def __init__(self, lowerBound, upperBound, intervalType):
        super().__init__()
        self.lowerBound = lowerBound
        self.upperBound = upperBound
        self.type = intervalType

    def __str__(self):
        template = '{sym1}{lb},{ub}{sym2}'
        if self.type == IntervalType.CC:
            return template.format(sym1='[', sym2=']', lb=self.lowerBound, ub=self.upperBound)
        elif self.type == IntervalType.OC:
            return template.format(sym1='(', sym2=']', lb=self.lowerBound, ub=self.upperBound)
        elif self.type == IntervalType.CO:
            return template.format(sym1='[', sym2=')', lb=self.lowerBound, ub=self.upperBound)
        else:
            return template.format(sym1='(', sym2=')', lb=self.lowerBound, ub=self.upperBound)


class DatasetSplitter(object):
    """
    Split dataset by given intervals.
    """

    log = config.getLogger('DatasetSplitter')

    def __init__(self, **kwargs):
        super().__init__()
        self.datasetPath = kwargs['datasetPath']
        self.datasetName = kwargs['datasetName']
        self.storeName = kwargs['storeName']

        self.varianceIntervals = kwargs['varianceIntervals']
        self.skewnessIntervals = kwargs['skewnessIntervals']
        self.kurtosisIntervals = kwargs['kurtosisIntervals']
        self.rangeIntervals = kwargs['rangeIntervals']
        self.isIntervalOK = self._checkIntervals()
        if not self.isIntervalOK:
            DatasetSplitter.log.error('Error: given intervals are invalid.')

        if self.varianceIntervals and self.skewnessIntervals and self.kurtosisIntervals and self.rangeIntervals:
            DatasetSplitter.log.debug('DatasetSplitter is constructed with:'
                                      '\tVarianceInterval = {}\n'
                                      '\tSkewnessInterval = {}\n'
                                      '\tKurtosisInterval = {}\n'
                                      '\tRangeInterval = {}\n'
                                      .format([str(x) for x in self.varianceIntervals],
                                              [str(x) for x in self.skewnessIntervals],
                                              [str(x) for x in self.kurtosisIntervals],
                                              [str(x) for x in self.rangeIntervals]))
        else:
            DatasetSplitter.log.error('Error: split intervals must be defined.')

        self.df = None
        if self.datasetPath and self.datasetName:
            path = self.datasetPath + self.datasetName
            self.df = pd.read_csv(path, index_col=False)
            DatasetSplitter.log.info('Dataframe is read from {}'.format(path))
        else:
            DatasetSplitter.log.error('Error: failed to read dataframe.')

    def splitByAllIntervals(self):
        self._splitByIntervals(self.varianceIntervals, 'variance')
        self._splitByIntervals(self.skewnessIntervals, 'skewness')
        self._splitByIntervals(self.kurtosisIntervals, 'kurtosis')
        self._splitByIntervals(self.rangeIntervals, 'range')

    def _checkIntervals(self) -> bool:
        result = True
        for intervals in (self.varianceIntervals, self.skewnessIntervals, self.kurtosisIntervals, self.rangeIntervals):
            for interval in intervals:
                if interval.type == IntervalType.CC:
                    result = result and (interval.lowerBound == interval.upperBound)

                if interval.lowerBound == '-inf':
                    result = result and (interval.type == IntervalType.OC or interval.type == IntervalType.OO)
                elif interval.lowerBound == 'inf':
                    result = False

                if interval.upperBound == 'inf':
                    result = result and (interval.type == IntervalType.CO or interval.type == IntervalType.OO)
                elif interval.upperBound == '-inf':
                    result = False

                result = result and (float(interval.upperBound) >= float(interval.lowerBound))

        return result

    def _splitByIntervals(self, splitIntervals: list, splitBy: str):
        splitValues = self.df[splitBy]
        splitResults = []  # element is of type (data, lb, up)
        for interval in splitIntervals:
            lb = interval.lowerBound
            ub = interval.upperBound
            if interval.type == IntervalType.CC:  # interval is a number
                splitResults.append((self.df[splitValues == lb], lb, ub))
            elif interval.type == IntervalType.OC:
                splitResults.append((self.df[(float(lb) < splitValues) & (splitValues <= ub)], lb, ub))
            elif interval.type == IntervalType.CO:
                splitResults.append((self.df[(lb <= splitValues) & (splitValues < float(ub))], lb, ub))
            elif interval.type == IntervalType.OO:
                splitResults.append((self.df[(float(lb) < splitValues) & (splitValues < float(ub))], lb, ub))
            else:
                DatasetSplitter.log.error('Error: unknown interval type. Accepted is {} but given {}.'.format(
                    [x.name for x in IntervalType], interval.type))
        nRows = functools.reduce(lambda x, y: x + y, [result[0].shape[0] for result in splitResults])
        assert nRows == self.df.shape[0], \
            'Error: nRows in splits [{}] is inconsistent with nRows in dataset [{}].'.format(nRows, self.df.shape[0])
        for result in splitResults:
            splitFilename = '{storeName}_split_{splitBy}_{lb}_{ub}.csv'\
                .format(storeName=self.storeName, splitBy=splitBy, lb=result[1], ub=result[2])
            DatasetSplitter.log.info('Writing split result to {}, with {} reviews.'
                                     .format(splitFilename, result[0].shape[0]))
            result[0].to_csv(self.datasetPath + splitFilename, index=False)


def splitDataFileHelper(filePath, filename, storeName):
    log.info('Start splitting file {}'.format(filename))
    startTime = time.time()
    varianceIntervals = [Interval(0.0, 0.0, IntervalType.CC),
                         Interval(0.0, 1.0, IntervalType.OC),
                         Interval(1.0, 'inf', IntervalType.OO)]
    skewnessIntervals = [Interval('-inf', 0.0, IntervalType.OO),
                         Interval(0.0, 0.0, IntervalType.CC),
                         Interval(0.0, 'inf', IntervalType.OO)]
    kurtosisIntervals = [Interval('-inf', -3.0, IntervalType.OC),
                         Interval(-3.0, 0.0, IntervalType.OC),
                         Interval(0.0, 'inf', IntervalType.OO)]
    rangeIntervals = [Interval(0.0, 0.0, IntervalType.CC),
                      Interval(0.0, 3.0, IntervalType.OC),
                      Interval(3.0, 'inf', IntervalType.OO)]
    splitter = DatasetSplitter(datasetPath=filePath, datasetName=filename, storeName=storeName,
                               varianceIntervals=varianceIntervals, skewnessIntervals=skewnessIntervals,
                               kurtosisIntervals=kurtosisIntervals, rangeIntervals=rangeIntervals)
    splitter.splitByAllIntervals()
    endTime = time.time()
    log.info('File %s is splitted in %.2f seconds.' % (filename, (endTime - startTime)))


if __name__ == '__main__':
    if input('Start from database (y/n)? ').lower() == 'y':
        client = MongoClient()
        db = client.get_database(config.databaseName)
        collection = db.get_collection(config.collectionName)
        log.info('Using database {}, collection {}. Collection has {} documents in total.'.format(
            db.name, collection.name, collection.count()))
        stores = collection.distinct(key='reviewStore')
        log.info('Collection has {} stores: {}'.format(len(stores), str(stores)))
        nReviewsByStore = {}
        for store in stores:
            count = collection.find({'reviewStore': store}).count()
            nReviewsByStore[store] = count
        nReviewsByStore = sorted(nReviewsByStore.items(), key=operator.itemgetter(1), reverse=True)
        log.info('# of reviews by each store: {}'.format(str(nReviewsByStore)))

        storeName = "Replacements, Ltd."
        overallScoreNames = ['overallSatisfaction', 'likelihoodToRecommend', 'wouldShopHereAgain']
        prepurchaseScoreNames = ['chargesStatedClearly', 'clarityProductInfo', 'designSite', 'easeOfFinding',
                                 'priceRelativeOtherRetailers', 'productSelection', 'satisfactionCheckout',
                                 'shippingCharges', 'varietyShippingOptions']

        filterHaving3OverallScoresAndNonEmptyContent = {
            'reviewStore': storeName,
            '$and': [{overallScoreNames[0]: {'$gt': -1}},
                     {overallScoreNames[1]: {'$gt': -1}},
                     {overallScoreNames[2]: {'$gt': -1}},
                     {'$where': 'this.content.length > 0'}
                     ]
        }
        filterHaving3OverallScoresAnd9PrepurchaseScoresAndNonEmptyContent = {
            'reviewStore': storeName,
            '$and': [{overallScoreNames[0]: {'$gt': -1}},
                     {overallScoreNames[1]: {'$gt': -1}},
                     {overallScoreNames[2]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[0]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[1]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[2]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[3]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[4]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[5]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[6]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[7]: {'$gt': -1}},
                     {'ratingsSiteExperience.' + prepurchaseScoreNames[8]: {'$gt': -1}},
                     {'content': {'$exists': 'true'}},
                     {'$where': 'this.content.length > 0'}
                     ]
        }
        docs = collection.find(filter=filterHaving3OverallScoresAndNonEmptyContent)
        reviews = []
        for doc in docs:
            prepurchaseScores = doc['ratingsSiteExperience'][prepurchaseScoreNames[0]] + \
                                doc['ratingsSiteExperience'][prepurchaseScoreNames[1]] + \
                                doc['ratingsSiteExperience'][prepurchaseScoreNames[2]] + \
                                doc['ratingsSiteExperience'][prepurchaseScoreNames[3]] + \
                                doc['ratingsSiteExperience'][prepurchaseScoreNames[4]] + \
                                doc['ratingsSiteExperience'][prepurchaseScoreNames[5]] + \
                                doc['ratingsSiteExperience'][prepurchaseScoreNames[6]] + \
                                doc['ratingsSiteExperience'][prepurchaseScoreNames[7]] + \
                                doc['ratingsSiteExperience'][prepurchaseScoreNames[8]]
            if prepurchaseScores > -9:
                reviews.append(doc)
        log.info('Store {storeName} has {nReviews} reviews having all 3 overall scores, non-empty content '
                 'and at least 8 prepurchase scores.'.format(storeName=storeName, nReviews=len(reviews)))
        client.close()
        log.info('Close connection to database [{}].\n'.format(config.databaseName))

        log.info('Start building dataframe.')
        author, date, content = [], [], []
        overallSatisfaction, likelihoodToRecommend, wouldShopHereAgain = [], [], []
        chargesStatedClearly, clarityProductInfo, designSite, \
            easeOfFinding, priceRelativeOtherRetailers, productSelection, \
            satisfactionCheckout, shippingCharges, varietyShippingOptions = [], [], [], [], [], [], [], [], []

        for review in reviews:
            author.append(review['author'])
            date.append(review['date'])
            content.append(review['content'])

            overallSatisfaction.append(review[overallScoreNames[0]])
            likelihoodToRecommend.append(review[overallScoreNames[1]])
            wouldShopHereAgain.append(review[overallScoreNames[2]])

            chargesStatedClearly.append(review['ratingsSiteExperience'][prepurchaseScoreNames[0]])
            clarityProductInfo.append(review['ratingsSiteExperience'][prepurchaseScoreNames[1]])
            designSite.append(review['ratingsSiteExperience'][prepurchaseScoreNames[2]])
            easeOfFinding.append(review['ratingsSiteExperience'][prepurchaseScoreNames[3]])
            priceRelativeOtherRetailers.append(review['ratingsSiteExperience'][prepurchaseScoreNames[4]])
            productSelection.append(review['ratingsSiteExperience'][prepurchaseScoreNames[5]])
            satisfactionCheckout.append(review['ratingsSiteExperience'][prepurchaseScoreNames[6]])
            shippingCharges.append(review['ratingsSiteExperience'][prepurchaseScoreNames[7]])
            varietyShippingOptions.append(review['ratingsSiteExperience'][prepurchaseScoreNames[8]])

        data = {
            'author': pd.Series(author),
            'date': pd.Series(date),
            'content': pd.Series(content),
            'overallSatisfaction': pd.Series(overallSatisfaction),
            'likelihoodToRecommend': pd.Series(likelihoodToRecommend),
            'wouldShopHereAgain': pd.Series(wouldShopHereAgain),
            'chargesStatedClearly': pd.Series(chargesStatedClearly),
            'clarityProductInfo': pd.Series(clarityProductInfo),
            'designSite': pd.Series(designSite),
            'easeOfFinding': pd.Series(easeOfFinding),
            'priceRelativeOtherRetailers': pd.Series(priceRelativeOtherRetailers),
            'productSelection': pd.Series(productSelection),
            'satisfactionCheckout': pd.Series(satisfactionCheckout),
            'shippingCharges': pd.Series(shippingCharges),
            'varietyShippingOptions': pd.Series(varietyShippingOptions),
        }
        df = pd.DataFrame(data=data)
        assert df.shape[0] == len(reviews), 'Error: inconsistent row count of dataframe and reviews.'
        assert df.shape[1] == (3 + 3 + 9), 'Error, inconsistent column count of dataframe and selected attributes.'
        log.info('Finished building dataframe. Shape of dataframe is {}.\n'.format(df.shape))

        log.info('Start computing variance, skewness, kurtosis and range for each review.')
        variance, skewness, kurtosis, range = [], [], [], []
        for index, row in df.iterrows():
            scores = [row[prepurchaseScoreNames[0]], row[prepurchaseScoreNames[1]], row[prepurchaseScoreNames[2]],
                      row[prepurchaseScoreNames[3]], row[prepurchaseScoreNames[4]], row[prepurchaseScoreNames[5]],
                      row[prepurchaseScoreNames[6]], row[prepurchaseScoreNames[7]], row[prepurchaseScoreNames[8]]]
            scores = [x for x in scores if x != -1]
            variance.append(np.var(scores))
            skewness.append(stats.skew(np.array(scores)))
            kurtosis.append(stats.kurtosis(scores))
            range.append(max(scores) - min(scores))
        df['variance'] = variance
        df['skewness'] = skewness
        df['kurtosis'] = kurtosis
        df['range'] = range
        log.info('Curated fields variance, skewness, kurtosis and range are derived. '
                 'New shape of dataframe is {}.\n'.format(df.shape))

        if not os.path.exists(config.splittedDataPath):
            log.info('{} does not exist and will be created.'.format(config.splittedDataPath))
            os.makedirs(config.splittedDataPath)

        filename = re.sub("[.,']", '', storeName).replace(' ', '_').replace('_&_', '_').lower()
        filename = '{}/{}.csv'.format(config.splittedDataPath, filename)
        df.to_csv(filename, index=False)
        log.info('Saved data with curated fields into file ' + filename)
    else:
        log.info('Skipping retrieving data from database.')
        time.sleep(1)

    if input('Start splitting dataset (y/n)? ').lower() == 'y':
        filenames = [x for x in os.listdir(config.splittedDataPath) if x.endswith('.csv') and '_split_' not in x]
        log.info('Using %d processes to split.' % config.numExecutors)
        pool = multiprocessing.Pool(processes=config.numExecutors)
        for filename in filenames:
            pos = filename.rfind('.')
            storeName = filename[:pos] if pos != -1 else ''
            pool.apply_async(splitDataFileHelper, args=(config.splittedDataPath, filename, storeName))
        pool.close()
        pool.join()
        log.info('All files are splitted.')
    else:
        log.info('Skipping splitting dataset.')
        time.sleep(1)
