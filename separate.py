import pandas as pd
from sklearn.model_selection import train_test_split


workDir = '/Users/keliu/Dropbox/text_review/program/'
tfidfFile = 'f 200.csv'
giniFile = 'f result.csv'
wordListFile = 'f selection.csv'
testSize = 0.3
threshWords = True


def threshWordsFromGini(dfReview, dfGini):
    labelPos, labelNeg = 'pos', 'neg'
    nPos = dfReview[dfReview['class'] == labelPos].shape[0]
    nNeg = dfReview[dfReview['class'] == labelNeg].shape[0]
    assert nPos + nNeg == dfReview.shape[0]
    giniThresh = (nPos / (nPos + nNeg)) ** 2 + (nNeg / (nPos + nNeg)) ** 2

    reviewPosNegRatio = nPos / nNeg
    df2 = dfGini[(dfGini['gini'] >= giniThresh) & (dfGini['num_pos'] / dfGini['num_neg'] >= reviewPosNegRatio)]

    filename = workDir + 'giniThreshed.csv'
    df2.to_csv(filename, index=False)
    print('Threshed words are saved at %s.' % filename)


if __name__ == '__main__':
    dfReview = pd.read_csv(workDir + tfidfFile)

    if threshWords:
        dfGini = pd.read_csv(workDir + giniFile)
        threshWordsFromGini(dfReview, dfGini)
    else:
        dfGini = pd.read_csv(workDir + wordListFile)
        dfTrain, dfTest = train_test_split(dfReview, test_size=testSize)
        dfTrain.to_csv(workDir + 'train' + str(int((1.0 - testSize) * 100)) + 'p.csv', index=False)
        dfTest.to_csv(workDir + 'test' + str(int(testSize * 100)) + 'p.csv', index=False)

        dfTestCopy1 = dfTest.copy()
        giniWords = set()
        for _, row in dfGini.iterrows():
            word = row[0][:-2]  # get clean word (not ended with _0 or _1)
            giniWords.add(word)

            isAppeared = int(row[0][-1]) == 1
            if isAppeared:
                tfidfSum = sum(dfTrain[word])
                tfidfCounter = len(dfTrain[word][dfTrain[word] > 0])
                tfidfAvg = tfidfSum / tfidfCounter
                dfTestCopy1.loc[:, word] = tfidfAvg
            else:
                dfTestCopy1.loc[:, word] = 0.0

        dfTrainCopy = dfTrain.copy()
        dfTestCopy2 = dfTest.copy()
        for word in giniWords:
            dfTrainCopy = dfTrainCopy.drop(columns=[word])
            dfTestCopy2 = dfTestCopy2.drop(columns=[word])

        dfTrainCopy.to_csv(workDir + 'trainRemoved.csv', index=False)
        dfTestCopy1.to_csv(workDir + 'testSubstitute.csv', index=False)
        dfTestCopy2.to_csv(workDir + 'testRemoved.csv', index=False)

        dfTest['class'] = 'pos'
        dfTestCopy1['class'] = 'pos'
        dfTestCopy2['class'] = 'pos'
        dfTest.to_csv(workDir + 'testClassChanged.csv', index=False)
        dfTestCopy1.to_csv(workDir + 'testSubstituteClassChanged.csv', index=False)
        dfTestCopy2.to_csv(workDir + 'testRemovedClassChanged.csv', index=False)

    print('Done!')
