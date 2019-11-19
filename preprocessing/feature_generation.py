import os
import logging
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from airflow.models import Variable


def compute_TFIDF(upstream_task: str,
                  text_field: str,
                  label: str,
                  include_index=False,
                  **context):
    """
    Compute TFIDF matrix from :texts:.
    :param text: Name of texts column that  TFIDF matrix is calculated from.
    :param label: Name of target variable column. (e.g. positive/negative).
    :return: File name of TFIDF matrix with labels as the last column (if not None).
    """

    task_instance = context['ti']
    working_dir = Variable.get('working_dir')
    os.makedirs(working_dir, exist_ok=True)
    logging.info('upstream_task=' + upstream_task)
    filename = task_instance.xcom_pull(task_ids=upstream_task)
    logging.info('Filename=' + filename)
    df = pd.read_csv(os.sep.join([working_dir, context['ds_nodash'], filename]))
    logging.info('Dataframe shape={}'.format(df.shape))
    vectorizer = TfidfVectorizer()
    texts = [''.join(text) for text in df[text_field]]
    tfidf = vectorizer.fit_transform(texts)
    df_result = pd.DataFrame(data=tfidf.toarray(), columns=vectorizer.get_feature_names())
    if label is not None:
        df_result[label] = df[label]
    logging.info('TFIDF shape={}'.format(df_result.shape))

    pos = filename.rfind('.')
    filename = filename[:pos] + '__tfidf.csv'
    save_path = os.sep.join([working_dir, context['ds_nodash']])
    os.makedirs(save_path, exist_ok=True)
    df_result.to_csv(save_path + os.sep + filename, index=include_index)

    return filename
