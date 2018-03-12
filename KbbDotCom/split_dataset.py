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

if __name__ == '__main__':
    if input('Start from database (y/n)? ').lower() == 'y':
        client = MongoClient(config.connectionStr)
        db = client.get_database(config.databaseName)
        collection = db.get_collection(config.collectionName)
        log.info('Using database {}, collection {}. Collection has {} documents in total.'.format(
            db.name, collection.name, collection.count()))

        makeModels = []
        makes = collection.distinct(key='make')
        log.info('There are {} makes in total.'.format(len(makes)))
        for make in makes:
            makeCollections = collection.find({'make': make})
            models = makeCollections.distinct(key='model')
            log.info('Make {} has models {}.'.format(make, str(models)))
            for model in models:
                    makeModels.append('{}_{}'.format(make, model))
        log.info('Collection has {} makeModelYear: {}.'.format(len(makeModels), str(makeModels)))

        nReviewsByMakeModel = {}
        for makeModel in makeModels:
            arr = makeModel.split('_')
            make, model = arr[0], arr[1]
            count = collection.find({'make': make, 'model': model}).count()
            nReviewsByMakeModel[makeModel] = count
        nReviewsByMakeModel = sorted(nReviewsByMakeModel.items(), key=operator.itemgetter(1), reverse=True)
        log.info('# of reviews by each make_model: {}'.format(str(nReviewsByMakeModel)))

        if not os.path.exists(config.splittedDataPath):
            log.info('{} does not exist and will be created.'.format(config.splittedDataPath))
            os.makedirs(config.splittedDataPath)

        for makeModel in makeModels:
            arr = makeModel.split('_')
            make, model = arr[0], arr[1]
            scoreNames = ['overall', 'value', 'reliability', 'comfort', 'styling', 'quality', 'performance']
            filterHaving7ScoresAndNonEmptyContent = {
                'make': make,
                'model': model,
                '$and': [{'ratings.' + scoreNames[0]: {'$gt': -1}},
                         {'ratings.' + scoreNames[1]: {'$gt': -1}},
                         {'ratings.' + scoreNames[2]: {'$gt': -1}},
                         {'ratings.' + scoreNames[3]: {'$gt': -1}},
                         {'ratings.' + scoreNames[4]: {'$gt': -1}},
                         {'ratings.' + scoreNames[5]: {'$gt': -1}},
                         {'ratings.' + scoreNames[6]: {'$gt': -1}},
                         {'$where': 'this.content.length > 0'}]
            }
            reviews = list(collection.find(filter=filterHaving7ScoresAndNonEmptyContent))
            log.info('Make={make}, model={model} has {nReviews} reviews having all 7 overall scores and non-empty '
                     'content.'.format(make=make, model=model, nReviews=len(reviews)))

            data = {
                'author': pd.Series([review['author'] for review in reviews]),
                'date': pd.Series([review['date'] for review in reviews]),
                'title': pd.Series([review['title'] for review in reviews]),
                'content': pd.Series([review['content'] for review in reviews]),
                'make': pd.Series([review['make'] for review in reviews]),
                'model': pd.Series([review['model'] for review in reviews]),
                'year': pd.Series([review['year'] for review in reviews]),
                'pros': pd.Series([review['pros'] for review in reviews]),
                'cons': pd.Series([review['cons'] for review in reviews]),
                'nHelpful': pd.Series([review['nHelpful'] for review in reviews]),
                'nHelpfulOutOf': pd.Series([review['nHelpfulOutOf'] for review in reviews]),
                'nRecommend': pd.Series([review['nRecommend'] for review in reviews]),
                'nRecommendOutOf': pd.Series([review['nRecommendOutOf'] for review in reviews])
            }
            for scoreName in scoreNames:
                data[scoreName] = pd.Series([review['ratings'][scoreName] for review in reviews])
            df = pd.DataFrame(data=data)
            assert df.shape[0] == len(reviews), 'Error: inconsistent number of rows.'
            log.info('Finished building dataframe. Shape of dataframe is {}.'.format(df.shape))

            if config.splittedDataPath.endswith('/'):
                filename = config.splittedDataPath + makeModel + '.csv'
            else:
                filename = config.splittedDataPath + '/' + makeModel + '.csv'
            df.to_csv(filename, index=False)
            log.info('Saved data into file ' + filename + '\n')

        client.close()
        log.info('Close connection to database [{}].\n'.format(config.databaseName))
    else:
        log.info('Skipping retrieving data from database.')
        time.sleep(1)
