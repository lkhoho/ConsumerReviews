#%%
import re
import pandas as pd
import jieba
from sklearn.feature_extraction.text import TfidfVectorizer


df = pd.read_csv('/mnt/c/Users/lkhoho/Desktop/xcar_post_nonempty_content.csv')
contents = df['content'].dropna()
print('Raw content size=%d' % len(contents))
filter_mask = (contents.str.len() > 0) & \
              (~contents.str.contains('.*更多精彩尽在.*')) & \
              (~contents.str.contains('.*附件.*'))
contents = contents[filter_mask][:5000]
print('Filtered content size=%d' % len(contents))

with open('/mnt/c/Users/lkhoho/Desktop/stop_words.txt') as fp:
    stopwords = fp.read()
stopwords = stopwords.splitlines()
print('Read %d words in stopwords.' % len(stopwords))

#%%
for content in contents[:5]:
    print(content)
    print('<<>><<>>')
    print('/'.join(jieba.cut(content)))
    print('-----------------\n')


#%%
tokenized = []
for content in contents:
    cutted = list(jieba.cut(content))
    cutted = filter(lambda word: len(word) > 0 and re.search(r'.*[\d_]+.*', word) is None, cutted)
    tokenized.append(' '.join(cutted))

vector = TfidfVectorizer(stop_words=stopwords)
tfidf = vector.fit_transform(tokenized)
with open('/mnt/c/Users/lkhoho/Desktop/xcar_post_features.txt', 'w') as fp:
    for feature in vector.get_feature_names():
        fp.write(feature)
        fp.write('\n')
df_tfidf = pd.DataFrame(tfidf.toarray(), columns=vector.get_feature_names())
print('TFIDF matrix shape=%s' % str(df_tfidf.shape))
df_tfidf.to_csv('/mnt/c/Users/lkhoho/Desktop/xcar_post_tfidf.csv', index=False, encoding='utf-8', sep=',')
print('Done!')
