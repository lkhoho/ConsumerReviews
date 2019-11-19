import sys
import os
import yaml
from nltk.corpus import stopwords
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

PROJ_ROOT = os.getenv('$CONSUMER_REVIEWS_HOME', '/Users/keliu/Developer/python/ConsumerReviews')
WF_ROOT = os.sep.join([PROJ_ROOT, 'workflows'])
sys.path.append(PROJ_ROOT)

from preprocessing.setup import setup
from preprocessing.balance_samples import RebalanceMode, rebalance_reviews
from preprocessing.lemmatization_new import LemmatizationMode, lemmatize
from preprocessing.feature_generation import compute_TFIDF
from preprocessing.feature_selection import FeatureRankingMode, feature_ranking


with open(WF_ROOT + os.sep + 'expedia_jp_workflow.yaml') as fp:
    try:
        config = yaml.safe_load(fp)
    except yaml.YAMLError as exc:
        print(exc)

stopwords_set = set(stopwords.words('english'))
with open(config['lemmatization']['stopwords']) as fp:
    for line in fp.readlines():
        stopwords_set.add(line.strip())

default_args = {
    'owner': 'keliu',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 20),
    'email': ['lkhoho@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    # 'schedule_interval': 'none'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

working_dir = Variable.get('working_dir', default_var=None)
if working_dir is None:
    Variable.set("working_dir", config['common']['working_dir'])

dag_jp = DAG('expedia_reviews_jp', default_args=default_args, schedule_interval=timedelta(1))

dag_start_task = DummyOperator(task_id='start', dag=dag_jp)

setup_task = PythonOperator(task_id='setup', 
                            dag=dag_jp,
                            provide_context=True,
                            python_callable=setup,
                            op_kwargs={
                                'data_pathname': config['setup']['data_pathname'],
                            })

lemmatize_task = PythonOperator(task_id='lemmatization',
                                dag=dag_jp,
                                provide_context=True,
                                python_callable=lemmatize,
                                op_kwargs={
                                    'upstream_task': 'setup',
                                    'text_field': config['lemmatization']['text_field_name'],
                                    'stopwords': stopwords_set,
                                    'config': {
                                        'pos_tagger': {
                                            'model': PROJ_ROOT + os.sep +
                                                    config['lemmatization']['pos_tagger']['model'],
                                            'jar': PROJ_ROOT + os.sep + config['lemmatization']['pos_tagger']['jar'],
                                            'java_options': config['lemmatization']['pos_tagger']['java_options'],
                                            'mode': eval(config['lemmatization']['mode']),
                                            'pos_mapping': config['lemmatization']['pos_tagger']['pos_mapping'],
                                            'invalid_pos': config['lemmatization']['pos_tagger']['invalid_pos']
                                        },
                                    },
                                    'include_index': config['lemmatization']['include_index'],
                                })

feature_generation_task = PythonOperator(task_id='feature_generation',
                                         dag=dag_jp,
                                         provide_context=True,
                                         python_callable=compute_TFIDF,
                                         op_kwargs={
                                             'upstream_task': 'lemmatization',
                                             'text_field': config['feature_generation']['text_field_name'],
                                             'label': config['feature_generation']['label_field_name'],
                                             'include_index': config['feature_generation']['include_index'],
                                         })

dag_start_task >> setup_task >> lemmatize_task >> feature_generation_task
