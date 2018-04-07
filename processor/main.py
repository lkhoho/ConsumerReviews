import os
import re
import multiprocessing as mp
from common import config
from common.data_reader import readStopwords
from processor.lemmatizer import Lemmatizer, LemmatizerType
from processor.nlp import computeSentimentScoresForNouns, computeSentimentScoresForAllWords, \
    computeSentimentScoresAfterTransformingObjectiveWords, computeGiniIndexScore
from processor.featureSelection import featureSelection
from BizrateDotCom import data_reader as biz_reader
from OrbitzDotCom import data_reader as orb_reader
from KbbDotCom import data_reader as kbb_reader

log = config.getLogger(__name__)


def _processHelper(fileId, filename, nTotalTasks):
    datasetName = re.match('.*/(.*).csv', filename).group(1)
    log.info('Processing dataset={dataset} on process={pid}. {fileId} of {nTotal}.'
             .format(dataset=datasetName, pid=os.getpid(), fileId=fileId, nTotal=nTotalTasks))
    if config.websiteName == 'BizrateDotCom':
        reviewData, posneg = biz_reader.readReviewData(filename)
    elif config.websiteName == 'OrbitzDotCom':
        reviewData, posneg = orb_reader.readReviewData(filename)
    elif config.websiteName == 'KbbDotCom':
        reviewData, posneg = kbb_reader.readReviewData(filename)
    else:
        reviewData, posneg = None, None
        log.error('Error: unsupported website data.')
        return

    lem = Lemmatizer(tagger, LemmatizerType.Vocabulary, config.POSDict, reviewData, stopwords)
    lem.lemmatize()
    lem.computeSentimentScores(batchSize=100)
    lem.removeUndesiredWords()

    if not os.path.exists(config.processedDataPath):
        log.info('{} does not exist and will be created.'.format(config.processedDataPath))
        os.makedirs(config.processedDataPath)
    computeSentimentScoresForAllWords(datasetName + '_', lem.cleanedByVocabulary, lem.scoredByVocabulary, posneg, stopwords)
    computeSentimentScoresForNouns(datasetName + '_', lem.cleanedByVocabulary, lem.scoredByVocabulary, posneg, stopwords)
    computeSentimentScoresAfterTransformingObjectiveWords(datasetName + '_', lem.cleanedByVocabulary,
                                                          lem.lemmatizedDataByVocabulary,
                                                          lem.scoredByVocabulary, posneg,
                                                          stopwords)

    # if config.websiteName == 'BizrateDotCom':
    #     if not os.path.exists(config.featureSelectedDataPath):
    #         log.info('{} does not exist and will be created.'.format(config.featureSelectedDataPath))
    #         os.makedirs(config.featureSelectedDataPath)
    #     featureSelection(datasetName)
    # elif config.websiteName == 'KbbDotCom':
    #     computeGiniIndexScore(datasetName + '_nouns_tfidf.csv')
    # else:
    #     pass
    log.info('----------------------------------------\n')


if __name__ == '__main__':
    log.info('Using tag model [{tagModel}]. Model path is [{modelPath}]. JAR path is [{jarPath}]'.format(
        tagModel=config.taggerConfig['currentUsedModel'],
        modelPath=config.taggerConfig['modelPath'][config.taggerConfig['currentUsedModel']],
        jarPath=config.taggerConfig['jarPath']))
    tagger = config.getTagger()

    reviewData, posneg = [], []
    if config.websiteName == 'BizrateDotCom':
        reviewFiles = [(config.splittedDataPath + x) for x in os.listdir(config.splittedDataPath)
                       if x.endswith('.csv') and '_split_' not in x]
    elif config.websiteName == 'KbbDotCom':
        reviewFiles = [(config.splittedDataPath + x) for x in os.listdir(config.splittedDataPath)
                       if x.endswith('.csv')]
    else:
        reviewFiles = []
    log.info('There are {} input datasets.'.format(len(reviewFiles)))

    stopwords = readStopwords(config.stopwordsPath)

    log.info('Using %d processes to process.' % config.numExecutors)
    pool = mp.Pool(processes=config.numExecutors)
    for idx, reviewFile in enumerate(reviewFiles):
        pool.apply_async(_processHelper, args=(idx, reviewFile, len(reviewFiles)))
    log.info('Waiting for subprocesses to complete...')
    pool.close()
    pool.join()
    log.info('All files are processed.')
