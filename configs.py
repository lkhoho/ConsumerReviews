import os
import yaml
import simplejson as json
import logging
from pathlib import Path
import nltk
from definitions import get_full_path
from utils.cipher import CipherUtils
from preprocessing.lemmatization import LemmatizationMode
from preprocessing.feature_selection import FeatureSelectionMode


class DatabaseConfig(object):
    def __init__(self, database: str, host: str, port: int, user: str, password: str, schema: str, encrypt=False):
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.schema = schema
        self.encrypted = encrypt
        if encrypt:
            self.password = CipherUtils.encrypt(password)
        else:
            self.password = password

    def __str__(self):
        return json.dumps({
            'database': self.database,
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': {
                'value': self.password,
                'encrypted': self.encrypted
            }
        })

    @staticmethod
    def from_file(config_file):
        with open(config_file) as stream:
            try:
                data = yaml.load(stream).get('database', None)
                if data is None:
                    raise ValueError('Database is not configured.')
                return DatabaseConfig(database=data.get('database', 'mysql'), host=data.get('host', 'localhost'),
                                      port=data.get('port', 3306), user=data.get('user', 'admin'),
                                      password=data.get('password', 'admin'), schema=data.get('schema', ''),
                                      encrypt=data.get('encrypt_password', False))
            except yaml.YAMLError as err:
                logging.error('Failed to load database config file {}. Error={}'
                              .format(config_file, json.dumps(err)))
                raise err


class CommonConfig(object):
    working_dir = str(Path.home()) + os.path.sep + 'consumer_reviews_working'
    stopwords = None
    stopwords_path = get_full_path('resource', 'stopwords.txt')

    @staticmethod
    def get_stopwords() -> set:
        if CommonConfig.stopwords is None:
            CommonConfig.stopwords = set(nltk.corpus.stopwords.words('english'))
            with open(CommonConfig.stopwords_path) as fp:
                CommonConfig.stopwords |= {word.strip().lower for word in fp}
        return CommonConfig.stopwords


class LemmatizationConfig(object):
    pos_tagger = {
        'model': get_full_path('lib', 'stanford-postagger', 'models', 'english-left3words-distsim.tagger'),
        # another model is english-bidirectional-distsim.tagger, which is slower but more accurate
        'jar': get_full_path('lib', 'stanford-postagger', 'stanford-postagger.jar'),
    }
    pos_mapping = {
        'JJ': 'a', 'JJR': 'a', 'JJS': 'a', 'NN': 'n', 'NNP': 'n', 'NNS': 'n', 'NNPS': 'n', 'RB': 'r',
        'RBR': 'r', 'RBS': 'r', 'VB': 'v', 'VBD': 'v', 'VBG': 'v', 'VBN': 'v', 'VBP': 'v'
    }
    lemmatizer = nltk.WordNetLemmatizer()
    modes = [LemmatizationMode.VOCABULARY]
    sentence_sep = '###'
    invalid_pos = 'x'


class FeatureSelectionConfig(object):
    modes = [FeatureSelectionMode.FREQUENCY, FeatureSelectionMode.CHI_SQUARE]
    num_features = [10, 25, 50, 100, 200, 500, 1000]
