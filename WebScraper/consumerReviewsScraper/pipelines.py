# -*- coding: utf-8 -*-

import re
import json
import csv
import logging
import sqlite3
from pony.orm import db_session, commit, ObjectNotFound
# import pymongo as mongo
# from stemming.porter2 import stem
from scrapy.exceptions import DropItem
from scrapy.utils.serialize import ScrapyJSONEncoder
from .dao import *
from .settings import DB_PROVIDER, DB_NAME
from .models import db
from .items.base import *
from .items.kbb import *
from .items.edmunds import *
from .items.orbitz import *
from .items.dianping import *
from .items.xcar import *


class SaveXcarItemsPipeline(object):
    """ Save items from xcar.com to Sqlite. """

    def __init__(self):
        self.db = db

    def open_spider(self, spider):
        # self.db.bind(provider=DB_PROVIDER, filename=DB_NAME, create_db=False)
        spider.logger.info('Connected to Sqlite file %s' % DB_NAME)

    def close_spider(self, spider):
        spider.logger.info('Disconnected to Sqlite file %s ' % DB_NAME)

    @db_session
    def process_item(self, item, spider):
        item_type = type(item)
        entity = None
        if item_type == XcarForum:
            try:
                existing = XcarForumEntity[item.get_id()]
                existing.set(**{
                    'name': item['name'],
                    'created_datetime': item['created_datetime']})
            except ObjectNotFound:
                entity = item.get_ORM_entity()
        elif item_type == XcarThread:
            try:
                existing = XcarThreadEntity[item.get_id()]
                existing.set(**{
                    'title': item['title'],
                    'forum': item['forum'],
                    'num_views': item['num_views'],
                    'num_replies': item['num_replies'],
                    'is_elite': item['is_elite'],
                    'created_datetime': item['created_datetime']})
            except ObjectNotFound:
                entity = item.get_ORM_entity()
        elif item_type == XcarUser:
            user = XcarUserEntity.upsert(id=item['id'], name=item['name'], gender=item['gender'],
                                         avatar_url=item['avatar_url'], register_date=item['register_date'],
                                         location=item['location'], coin=item['coin'], rank=item['rank'],
                                         num_follows=item['num_follows'], num_fans=item['num_fans'],
                                         num_posts=item['num_posts'], created_datetime=item['created_datetime'],
                                         manage=item['manage'])
        elif item_type == XcarPost:
            post = XcarPostEntity.upsert(id=item['id'], author=item['author'], content=item['content'],
                                         publish_datetime=item['publish_datetime'],
                                         created_datetime=item['created_datetime'], is_flag=item['is_flag'],
                                         thread=item['thread'])
            thread = XcarThreadEntity[item['thread']]
            thread.posts.add(post)
        else:
            spider.logger.warn('Unknown item scraped.')
        commit()
        return item


class SaveDianpingItemsPipeline(object):
    def __init__(self):
        self.db_name = '/Users/keliu/consumer_reviews_working/consumer_reviews.db'
        self.conn = None
        self.db_entity_dao = None

    def open_spider(self, spider):
        self.conn = sqlite3.connect(self.db_name)
        spider.logger.info('Connected to Sqlite file %s' % self.db_name)

    def close_spider(self, spider):
        self.conn.close()
        spider.logger.info('Disconnected to Sqlite file %s ' % self.db_name)

    def process_item(self, item, spider):
        if isinstance(item, DatabaseItem):
            if isinstance(item, DPCommunity):
                self.db_entity_dao = DPCommunityDao(self.conn)
                self.db_entity_dao.upsert(item)
                for manager in item['managers']:
                    self.conn.execute('insert or replace into dp_community_manager values (?, ?, ?)',
                                      (manager, item['city'], item['name']))
                self.conn.commit()
            elif isinstance(item, DPReview):
                self.db_entity_dao = DPReviewDao(self.conn)
                self.db_entity_dao.upsert(item)
                self.conn.execute('insert or replace into dp_topic_review values (?, ?)',
                                  (item['topic_id'], item['id']))
                self.conn.commit()
            elif isinstance(item, DPBadge):
                self.db_entity_dao = DPBadgeDao(self.conn)
                self.db_entity_dao.upsert(item)
            elif isinstance(item, DPBonus):
                self.db_entity_dao = DPBonusDao(self.conn)
                self.db_entity_dao.upsert(item)
            elif isinstance(item, DPMember):
                self.db_entity_dao = DPMemberDao(self.conn)
                self.db_entity_dao.upsert(item)
            elif isinstance(item, DPTopic):
                self.db_entity_dao = DPTopicDao(self.conn)
                self.db_entity_dao.upsert(item)
        else:
            raise NotImplementedError('Only support database item for now!')
        return item


class TokenizationPipeline(object):
    """ Tokenize review item content. """

    def __init__(self):
        self.tokenization_regex = r"[\s()<>[\]{}|,.:;?!&$'\"]"

    def process_item(self, item, spider):
        text = item["content"].lower()
        text = re.sub(r"['?,\-\\!\"]", " ", text)
        item["content"] = self.tokenize(text)
        return item

    def tokenize(self, text):
        tokens = []
        words = re.split(self.tokenization_regex, text.lower())
        for word in words:
            stripped = word.strip()
            if len(stripped) > 1:
                tokens.append(stripped)
        return tokens


class RemoveStopwordsPipeline(object):
    """ Remove stopwords and generate features for review content. """

    def __init__(self):
        self.stopwords_filename = "stopwords.txt"
        self.stopwords_fp = None
        self.stopwords = set()
        self.logger = logging.getLogger(__name__)

    def open_spider(self, spider):
        try:
            self.stopwords_fp = open(self.stopwords_filename)
        except OSError as exc:
            self.logger.error("Error: opening stopwords file {} failed. Exception: {}".format(self.stopwords_filename, exc))

        for word in self.stopwords_fp:
            self.stopwords.add(word)

        self.stopwords_fp.close()

    def process_item(self, item, spider):
        text = item["content"].lower()

        # remove comma in numbers
        numbers = re.findall(r"\s(\d+[,\d]*\d+)\s?", text)
        for number in numbers:
            text = text.replace(number, number.replace(",", ""))

        # remove other commas
        text = re.sub(r"['?,\-\\!\"]", " ", text)
        word_arr = text.split()
        for word in word_arr:
            if word in self.stopwords:
                word_arr.remove(word)
        item["content"] = " ".join(word_arr)
        return item


# class StemmingReviewsPipeline(object):
#     """ Stem features. """
#
#     def open_spider(self, spider):
#         pass
#
#     def close_spider(self, spider):
#         pass
#
#     def process_item(self, item, spider):
#         for i in range(len(item["content"])):
#             word = item["content"][i]
#             item["content"][i] = stem(word)
#         return item


# class SaveToMongoDb(object):
#     """
#     Persist items into MongoDB as documents.
#     """
#
#     def __init__(self):
#         self.conn_str = 'mongodb://lkhoho:56b2AmDI@lkhoho-cluster-shard-00-00-d12bg.mongodb.net:27017,lkhoho-cluster-shard-00-01-d12bg.mongodb.net:27017,lkhoho-cluster-shard-00-02-d12bg.mongodb.net:27017/admin?replicaSet=lkhoho-cluster-shard-0&ssl=true'
#         self.client = None
#         self.dbName = 'ConsumerReviews'
#         self.collectionName = None
#         self.collection = None  # document collection
#         self.nCurrentDocuments = 0  # current document count
#
#     def open_spider(self, spider):
#         position = spider.reviewSource.rfind('.')
#         self.collectionName = spider.reviewSource[:position]
#         self.client = mongo.MongoClient()
#         spider.logger.info('Open connection to database [{}] in MongoDB.'.format(self.dbName))
#         self.collection = self.client[self.dbName][self.collectionName]
#         self.nCurrentDocuments = self.collection.count()
#         assert self.nCurrentDocuments == self.nCurrentDocuments, 'Error: queried documents count != documents count'
#         spider.logger.info('Database {}, collection {} has {} documents for now.'.format(
#             self.dbName, self.collectionName, self.nCurrentDocuments))
#
#     def close_spider(self, spider):
#         self.client.close()
#         nItems = self.collection.count()
#         spider.logger.info('Close connection to database [{}] in MongoDB. {} new items are persisted.'.format(
#             self.dbName, nItems - self.nCurrentDocuments))
#
#     def process_item(self, item, spider):
#         if not self.collection.find_one({'id': item['id']}):
#             result = self.collection.insert_one(item)
#             spider.logger.info('Item inserted as {}.'.format(result.inserted_id))
#             return item
#         else:
#             raise DropItem("Item is already in MongoDB. Will be dropped.")


class SaveRawItemPipeline(object):
    """ Save raw items in JSON format. """

    def __init__(self):
        self.items = []
        self.fp = None
        self.encoder = ScrapyJSONEncoder()
        self.logger = logging.getLogger(__name__)

    def open_spider(self, spider):
        filename = spider.name + "_raw.json"
        try:
            self.fp = open(filename, "a")
        except OSError as exc:
            self.logger.error("Error: opening JSON file {} failed. Exception: {}".format(filename, exc))

    def close_spider(self, spider):
        json.dump(self.items, self.fp)
        self.fp.close()

    def process_item(self, item, spider):
        self.items.append(self.encoder.encode(item))
        return item


class SaveToCsvPipeline(object):
    """ Export items in CSV format. """

    def __init__(self):
        self.pos_features_fp = None
        self.neg_features_fp = None
        self.others_fp = None
        self.logger = logging.getLogger(__name__)

    def open_spider(self, spider):
        if getattr(spider, "is_pos_neg_separated", False):
            pos_features_filename = spider.name + "_features_pos.csv"
            neg_features_filename = spider.name + "_features_neg.csv"
            try:
                self.pos_features_fp = open(pos_features_filename, "a")
            except OSError as exc:
                self.logger.error("Error: opening features file {} failed. Exception: {}".format(pos_features_filename, exc))

            try:
                self.neg_features_fp = open(neg_features_filename, "a")
            except OSError as exc:
                self.logger.error("Error: opening features file {} failed. Exception: {}".format(neg_features_filename, exc))
        else:
            features_filename = spider.name + "_features.csv"
            try:
                self.pos_features_fp = open(features_filename, "a")
            except OSError as exc:
                self.logger.error("Error: opening features file {} failed. Exception: {}".format(features_filename, exc))

        others_filename = spider.name + "_others.csv"
        try:
            self.others_fp = open(others_filename, "a")
            others_writer = csv.writer(self.others_fp, delimiter=",", quoting=csv.QUOTE_ALL)

            if spider.name.startswith("kbb"):
                others_writer.writerow(KBBReviewItem.get_column_headers())
            elif spider.name.startswith("edmunds"):
                others_writer.writerow(EdmundsReviewItem.get_column_headers())
            elif spider.name.startswith("orbitz"):
                others_writer.writerow(OrbitzReviewItem.get_column_headers())
        except OSError as exc:
            self.logger.error("Error: opening others file {} failed. Exception: {}".format(others_filename, exc))

    def close_spider(self, spider):
        self.pos_features_fp.close()

        if self.neg_features_fp is not None:
            self.neg_features_fp.close()

        self.others_fp.close()

    def process_item(self, item, spider):
        others_writer = csv.writer(self.others_fp, delimiter=",", quoting=csv.QUOTE_ALL)
        if type(item).__name__ == "KBBReviewItem":
            others_writer.writerow(item.get_column_values())
        elif type(item).__name__ == "EdmundsReviewItem":
            others_writer.writerow(item.get_column_values())
        elif type(item).__name__ == "OrbitzReviewItem":
            others_writer.writerow(item.get_column_values())

        text = SaveToCsvPipeline.__tokens_to_str(item["content"])
        item["content"] = text
        if getattr(spider, "is_pos_neg_separated", False):
            try:
                if item["will_recommend"] == 1:
                    features_writer = csv.writer(self.pos_features_fp, quoting=csv.QUOTE_ALL)
                    features_writer.writerow(item["content"].split(","))
                elif item["will_recommend"] == 0:
                    features_writer = csv.writer(self.neg_features_fp, quoting=csv.QUOTE_ALL)
                    features_writer.writerow(item["content"].split(","))
                else:
                    self.logger.error("Error: value of will_recommend attribute is unknown.")
            except KeyError:
                self.logger.error("Error: item do not have will_recommend attribute.")
        else:
            features_writer = csv.writer(self.pos_features_fp, quoting=csv.QUOTE_ALL)
            features_writer.writerow(item["content"].split(","))
        return item

    @staticmethod
    def __tokens_to_str(tokens):
        s = ""
        if len(tokens) > 0:
            s += tokens[0]
            for i in range(1, len(tokens)):
                s += ","
                s += tokens[i]
        return s
