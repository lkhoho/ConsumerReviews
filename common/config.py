import logging
import multiprocessing
from os.path import expanduser
from cryptography.fernet import Fernet
import nltk


# MongoDB config
_key = b'NZqFKZPTlQuEEVzNhVXGtpWdeiVxc8y9Vg1fpkwJBUg='
_encryptedPwd = b'gAAAAABagp1_bXqPvbLDnmfRMFrD3siN1Y6akPFD2d43HAk_WS-AUT5xULanRDC-LSJHSg5LVz8iWiLAQxOp9mBBS0dYZWWWgQ=='
_fernet = Fernet(_key)
databaseName = 'ConsumerReviews'
collectionName = 'kbb'
# connectionStr = 'mongodb://lkhoho:{pwd}@lkhoho-cluster-shard-00-00-d12bg.mongodb.net:27017,' \
#                 'lkhoho-cluster-shard-00-01-d12bg.mongodb.net:27017,' \
#                 'lkhoho-cluster-shard-00-02-d12bg.mongodb.net:27017/admin?' \
#                 'replicaSet=lkhoho-cluster-shard-0&ssl=true'.format(pwd=_fernet.decrypt(_encryptedPwd).decode('utf-8'))
connectionStr = None
############################################################################################################

# NLTK config
nltk.internals.config_java(options='-Xmx4000m')
############################################################################################################

# POS tagger config
POSDict = {
    'JJ': 'a', 'JJR': 'a', 'JJS': 'a', 'NN': 'n', 'NNP': 'n', 'NNS': 'n', 'NNPS': 'n', 'RB': 'r',
    'RBR': 'r', 'RBS': 'r', 'VB': 'v', 'VBD': 'v', 'VBG': 'v', 'VBN': 'v', 'VBP': 'v'
}
taggerConfig = {
    'jarPath': '/stanford-postagger/stanford-postagger.jar',
    'modelPath': {
        'left3words': '/stanford-postagger/models/english-left3words-distsim.tagger',
        'bidirectional': '/stanford-postagger/models/english-bidirectional-distsim.tagger'
    },
    'currentUsedModel': 'left3words'  # or 'bidirectional', which is slower but more accurate
}
############################################################################################################

# project config
numExecutors = int(multiprocessing.cpu_count() / 2)
websiteName = 'KbbDotCom'
stopwordsPath = '/Users/keliu/Developer/python/ConsumerReviews/common/data/stopwords.txt'
splittedDataPath = '/Volumes/HDD2/projects/ConsumerReviews/{}/splitData/'.format(websiteName)
processedDataPath = '/Volumes/HDD2/projects/ConsumerReviews/{}/processData/'.format(websiteName)
featureSelectedDataPath = '/Volumes/HDD2/projects/ConsumerReviews/{}/featureSelectionData/'.format(websiteName)
############################################################################################################

# BizrateDotCom config
bizrateDotComLabelConfig = {
    'scoreName': 'likelihoodToRecommend',
    'threshold': 9
}
############################################################################################################

# KbbDotCom config
kbbDotComLabelConfig = {
    'scoreName': 'overall',
    'threshold': 9
}
############################################################################################################


def getLogger(name):
    formatter = logging.Formatter(fmt='%(levelname)s [%(module)s] - %(asctime)s: %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


def getTagger():
    home = expanduser('~')
    currentModel = taggerConfig['currentUsedModel']
    modelPath = home + taggerConfig['modelPath'][currentModel]
    jarPath = home + taggerConfig['jarPath']
    return nltk.tag.stanford.StanfordPOSTagger(modelPath, path_to_jar=jarPath)
