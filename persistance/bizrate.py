import operator
import pandas as pd
from pymongo import MongoClient
from utils.logging import get_logger

log = get_logger(__name__)

preset_filters = {
    'having_3_overall_scores_and_nonempty_content': {
        '$and': [{'﻿overallSatisfaction': {'$gt': -1}},
                 {'﻿likelihoodToRecommend': {'$gt': -1}},
                 {'﻿wouldShopHereAgain': {'$gt': -1}},
                 {'content': {'$exists': 'true'}},
                 {'$where': 'this.content.length > 0'}]
    },
    'having_3_overall_scores_and_9_site_experience_scores_and_nonempty_content': {
        '$and': [{'﻿overallSatisfaction': {'$gt': -1}},
                 {'﻿likelihoodToRecommend': {'$gt': -1}},
                 {'﻿wouldShopHereAgain': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿varietyShippingOptions': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿shippingCharges': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿easeOfFinding': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿priceRelativeOtherRetailers': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿clarityProductInfo': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿chargesStatedClearly': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿designSite': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿productSelection': {'$gt': -1}},
                 {'﻿ratingsSiteExperience.' + '﻿satisfactionCheckout': {'$gt': -1}},
                 {'content': {'$exists': 'true'}},
                 {'$where': 'this.content.length > 0'}]
    }
}


def count_documents_by_collection(database_name='ConsumerReviews', collection_name='bizrate') -> int:
    """
    Count total number of reviews in given collection.
    :param database_name: name of database to count.
    :param collection_name: name of collection to count.
    :return: the total number of reviews in given collection, or -1 if unable to count.
    """

    collection = _get_collection(database_name, collection_name)
    return collection.count() if collection is not None else -1


def count_documents_by_store(database_name='ConsumerReviews', collection_name='bizrate') -> "dict('store': 'number')":
    """
    Count the number of reviews by store in descending order.
    :param database_name: name of database to count.
    :param collection_name: name of collection to count.
    :return: sorted number of review by store in descending order.
    """

    collection = _get_collection(database_name, collection_name)
    if collection is None:
        return None
    stores = collection.distinct(key='reviewStore')
    result = {store: collection.find({'reviewStore': store}).count()
              for store in stores}
    return sorted(result.items(), key=operator.itemgetter(1), reverse=True)


def query(database_name, collection_name, **condition):
    """
    Query mongodb by given query condition.
    :param database_name: name of database to query.
    :param collection_name: name of collection to query.
    :param condition: name of collection to query.
    :return: queried documents in list, or None if unable to query.
    """

    collection = _get_collection(database_name, collection_name)
    return collection.find(condition) if collection is not None else None


def query_by_stores(database_name='ConsumerReviews', collection_name='bizrate', store_name='',
                    condition=preset_filters['having_3_overall_scores_and_nonempty_content']):
    """
    Query mongodb by given condition.
    :param database_name: name of database to query.
    :param collection_name: name of collection to query.
    :param store_name: store name to query.
    :param condition: query conditions.
    :return: queried document list, or None if unable to query.
    """

    collection = _get_collection(database_name, collection_name)
    if collection is None:
        return None
    condition['reviewStore'] = store_name
    return list(filter(lambda doc: (doc['ratingsSiteExperience']['﻿varietyShippingOptions'] +
                                    doc['ratingsSiteExperience']['﻿shippingCharges'] +
                                    doc['ratingsSiteExperience']['﻿easeOfFinding'] +
                                    doc['ratingsSiteExperience']['﻿priceRelativeOtherRetailers'] +
                                    doc['ratingsSiteExperience']['﻿clarityProductInfo'] +
                                    doc['ratingsSiteExperience']['﻿chargesStatedClearly'] +
                                    doc['ratingsSiteExperience']['﻿designSite'] +
                                    doc['ratingsSiteExperience']['﻿productSelection'] +
                                    doc['ratingsSiteExperience']['﻿satisfactionCheckout']) > -9,
                       collection.find(filter=condition)))


def build_dataframe(reviews: list) -> pd.DataFrame:
    """
    Build pandas dataframe from review list.
    :param reviews: list of reviews to build from.
    :return: pandas DataFrame object containing all fields in reviews.
    """

    data = {
        'pre_purchase_author': pd.Series([x['author'] for x in reviews]),
        'pre_purchase_date': pd.Series([x['date'] for x in reviews]),
        'pre_purchase_title': pd.Series([x['title'] for x in reviews]),
        'pre_purchase_content': pd.Series([x['content'] for x in reviews]),
        'after_purchase_author': pd.Series([x['﻿reviewAfterPurchase']['author'] for x in reviews]),
        'after_purchase_date': pd.Series([x['﻿reviewAfterPurchase']['date'] for x in reviews]),
        'after_purchase_title': pd.Series([x['﻿reviewAfterPurchase']['title'] for x in reviews]),
        'after_purchase_content': pd.Series([x['﻿reviewAfterPurchase']['content'] for x in reviews]),
        'review_store': pd.Series([x['﻿reviewStore'] for x in reviews]),
        'source': pd.Series([x['﻿source'] for x in reviews]),
        'overall_score_satisfaction': pd.Series([x['﻿overallSatisfaction'] for x in reviews]),
        'overall_score_to_recommend': pd.Series([x['﻿likelihoodToRecommend'] for x in reviews]),
        'overall_score_shop_again': pd.Series([x['﻿wouldShopHereAgain'] for x in reviews]),
        'site_exp_score_shipping_options': pd.Series([x['﻿ratingsSiteExperience']['﻿varietyShippingOptions']
                                                      for x in reviews]),
        'site_exp_score_shipping_charges': pd.Series([x['﻿ratingsSiteExperience']['﻿shippingCharges']
                                                      for x in reviews]),
        'site_exp_score_ease_finding': pd.Series([x['﻿ratingsSiteExperience']['﻿easeOfFinding'] for x in reviews]),
        'site_exp_score_price_relative': pd.Series([x['﻿ratingsSiteExperience']['﻿priceRelativeOtherRetailers']
                                                    for x in reviews]),
        'site_exp_score_clarity_product': pd.Series([x['﻿ratingsSiteExperience']['﻿clarityProductInfo']
                                                     for x in reviews]),
        'site_exp_score_charges_clearly': pd.Series([x['﻿ratingsSiteExperience']['﻿chargesStatedClearly']
                                                     for x in reviews]),
        'site_exp_score_site_design': pd.Series([x['﻿ratingsSiteExperience']['﻿designSite'] for x in reviews]),
        'site_exp_score_product_selection': pd.Series([x['﻿ratingsSiteExperience']['﻿productSelection']
                                                       for x in reviews]),
        'site_exp_score_satisfaction_checkout': pd.Series([x['﻿ratingsSiteExperience']['﻿satisfactionCheckout']
                                                           for x in reviews]),
        'after_purchase_score_product_availability': pd.Series([x['﻿ratingsAfterPurchase']['﻿productAvailability']
                                                                for x in reviews]),
        'after_purchase_score_ontime_delivery': pd.Series([x['﻿ratingsAfterPurchase']['﻿onTimeDelivery']
                                                           for x in reviews]),
        'after_purchase_score_product_met_expectation': pd.Series([x['﻿ratingsAfterPurchase']['﻿productMetExpectations']
                                                                   for x in reviews]),
        'after_purchase_score_order_tracking': pd.Series([x['﻿ratingsAfterPurchase']['﻿orderTracking']
                                                          for x in reviews]),
        'after_purchase_score_return_process': pd.Series([x['﻿ratingsAfterPurchase']['﻿returnsProcess']
                                                          for x in reviews]),
        'after_purchase_score_customer_support': pd.Series([x['﻿ratingsAfterPurchase']['﻿customerSupport']
                                                            for x in reviews])
    }
    df = pd.DataFrame(data)
    assert df.shape[0] == len(reviews), \
        'Inconsistent of row count of dataframe(num={}) and reviews(num={}).'.format(df.shape[0], len(reviews))
    num_features = 4 + 4 + 1 + 1 + 3 + 9 + 6
    assert df.shape[1] == num_features, \
        'Inconsistent of col count of dataframe(num={}) and fields(num={})'.format(df.shape[1], num_features)
    return df


def _get_collection(database_name, collection_name):
    if not _check_precondition(database_name, collection_name):
        return None
    client = MongoClient()
    db = client.get_database(database_name)
    return db.get_collection(collection_name)


def _check_precondition(database_name, collection_name) -> bool:
    if database_name is None or collection_name is None:
        return False
    else:
        return True


def _parse_expressions(expressions):
    result = []
    for expr in expressions:
        if '<=' in expr:
            splits = expr.split('<=')
            result.append({splits[0]: {'$lte': splits[1]}})
        elif '>=' in expr:
            splits = expr.split('>=')
            result.append({splits[0]: {'$gte': splits[1]}})
        elif '!=' in expr:
            splits = expr.split('!=')
            result.append({splits[0]: {'$ne': splits[1]}})
        elif '<' in expr:
            splits = expr.split('<')
            result.append({splits[0]: {'$lt': splits[1]}})
        elif '>' in expr:
            splits = expr.split('>')
            result.append({splits[0]: {'$gt': splits[1]}})
        elif '=' in expr:
            splits = expr.split('=')
            result.append({splits[0]: splits[1]})  # equivalent of "{splits[0]: {'$eq': splits[1]}}"
        elif 'not exists' in expr:
            pos = expr.index('not exists')
            result.append({expr[:pos].strip(): {'$exists': 'false'}})
        elif 'exists' in expr:
            pos = expr.index('exists')
            result.append({expr[:pos].strip(): {'$exists': 'true'}})
    return result
