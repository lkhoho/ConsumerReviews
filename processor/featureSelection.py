import os
import operator
import pandas as pd
import numpy as np
from scipy.stats import chisquare
import common.config as config

log = config.getLogger(__name__)


def featureSelection(datasetName, numFeatures=200):
    log.info('Start feature selection on dataset {} with maximum of {} features.'.format(datasetName, numFeatures))
    files = [x for x in os.listdir(config.processedDataPath) if x.startswith(datasetName) and x.endswith('.csv')]
    assert len(files) == 9, 'Error: there should be 9 TFIDF files for each splitted dateset.'

    rawTFIDFFilename = list(filter(lambda x: x.endswith('raw_tfidf.csv'), files))[0]
    nounTFIDFFilename = list(filter(lambda x: x.endswith('nouns_tfidf.csv'), files))[0]
    label = 'posneg'

    log.info('Compute chiSquares for raw and noun TFIDF CSVs.')
    rawChiFeatures = _chiSquareFeatureSelector(config.processedDataPath + rawTFIDFFilename, label, numFeatures)
    nounChiFeatures = _chiSquareFeatureSelector(config.processedDataPath + nounTFIDFFilename, label, numFeatures)

    log.info('Compute frequency for raw and noun TFIDF CSVs.')
    rawFreqFeatures = _frequencyFeatureSelector(config.processedDataPath + rawTFIDFFilename, label, numFeatures)
    nounFreqFeatures = _frequencyFeatureSelector(config.processedDataPath + nounTFIDFFilename, label, numFeatures)

    for file in files:
        log.info('Perform feature selection on {}'.format(file))
        df = pd.read_csv(config.processedDataPath + file)

        if ('_raw_' in file) or ('_all_' in file):
            rawChiFeatures.append(label)
            result = df[rawChiFeatures]
            filename = file[:file.rfind('.')] + '_chi.csv'
            result.to_csv(config.featureSelectedDataPath + filename, index=False)
            log.info('Write {}'.format(filename))

            rawFreqFeatures.append(label)
            result = df[rawFreqFeatures]
            filename = file[:file.rfind('.')] + '_freq.csv'
            result.to_csv(config.featureSelectedDataPath + filename, index=False)
            log.info('Write {}'.format(filename))
        elif '_nouns_' in file:
            nounChiFeatures.append(label)
            result = df[nounChiFeatures]
            filename = file[:file.rfind('.')] + '_chi.csv'
            result.to_csv(config.featureSelectedDataPath + filename, index=False)
            log.info('Write {}'.format(filename))

            nounFreqFeatures.append(label)
            result = df[nounFreqFeatures]
            filename = file[:file.rfind('.')] + '_freq.csv'
            result.to_csv(config.featureSelectedDataPath + filename, index=False)
            log.info('Write {}'.format(filename))
        else:
            log.error("Error: unknown type of CSV file. Expected 'all', 'nouns' or 'raw' CSVs.")


def _chiSquareFeatureSelector(filename, label, numFeatures):
    df = pd.read_csv(filename)
    chiSquares = {}

    for feature in df:
        if feature == label:
            continue

        values = df[feature]
        posneg = df[label]

        nAppearPos = len(df[(values > 0.0) & (posneg == 1)])  # appear in positive category
        nNotAppearPos = len(df[(values == 0.0) & (posneg == 1)])  # not-appear in positive category
        nAppearNeg = len(df[(values > 0.0) & (posneg == 0)])  # appear in negative category
        nNotAppearNeg = len(df[(values == 0.0) & (posneg == 0)])  # not-appear in negative category

        # 2x2 matrix
        # nAppearPos   nNotAppearPos
        # nAppearNeg   nNotAppearNeg
        mat = np.array([[nAppearPos, nNotAppearPos], [nAppearNeg, nNotAppearNeg]])
        assert mat.sum(axis=0)[0] == len(df[values > 0.0]), \
            'feature={}, left={}, right={}'.format(feature, mat.sum(axis=0)[0], len(df[values > 0.0]))
        assert mat.sum(axis=0)[1] == len(df[values == 0.0]), \
            'feature={}, left={}, right={}'.format(feature, mat.sum(axis=0)[1], len(df[values == 0.0]))
        assert mat.sum(axis=1)[0] == len(df[posneg == 1]), \
            'feature={}, left={}, right={}'.format(feature, mat.sum(axis=1)[0], len(df[posneg == 1]))
        assert mat.sum(axis=1)[1] == len(df[posneg == 0]), \
            'feature={}, left={}, right={}'.format(feature, mat.sum(axis=1)[1], len(df[posneg == 0]))

        total = mat.sum()
        nAppearPosExp = mat.sum(axis=1)[0] * mat.sum(axis=0)[0] / total
        nNotAppearPosExp = mat.sum(axis=1)[0] * mat.sum(axis=0)[1] / total
        nAppearNegExp = mat.sum(axis=1)[1] * mat.sum(axis=0)[0] / total
        nNotAppearNegExp = mat.sum(axis=1)[1] * mat.sum(axis=0)[1] / total
        expected = [nAppearPosExp, nNotAppearPosExp, nAppearNegExp, nNotAppearNegExp]
        chiSquares[feature] = chisquare(mat.flatten().tolist(), expected)[0]

    sortedChiSquares = sorted(chiSquares.items(), key=operator.itemgetter(1), reverse=True)[:numFeatures]
    return [x[0] for x in sortedChiSquares]


def _frequencyFeatureSelector(filename, label, numFeatures):
    df = pd.read_csv(filename)
    frequency = {}

    for feature in df:
        if feature == label:
            continue

        values = df[feature]
        frequency[feature] = len(df[values > 0.0])

    sortedFrequency = sorted(frequency.items(), key=operator.itemgetter(1), reverse=True)[:numFeatures]
    return [x[0] for x in sortedFrequency]


if __name__ == '__main__':
    featureSelection('crutchfield_split_kurtosis_-3.0_0.0')
