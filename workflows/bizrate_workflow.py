import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

sys.path.append('/Users/keliu/Developer/python/ConsumerReviews')
from preprocessing.io import predefined_sql, fetch_by_store
from preprocessing.curated_attributes import predefined_curated_attributes, compute_curated_attributes
from preprocessing.split import predefined_range_definitions, split_dataset
from preprocessing.lemmatization import LemmatizationMode, sentence_sep, lemmatize, compute_sentiment_score, \
    clean_undesired
from preprocessing.nlp import process_all, process_nouns, transform_objective
from preprocessing.feature_selection import FeatureSelectionMode, feature_selection

default_args = {
    'owner': 'keliu',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 1),
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

dag = DAG('bizrate_reviews', default_args=default_args, schedule_interval=timedelta(1))
working_dir = str(Path.home()) + os.path.sep + 'consumer_reviews_working'
tagger_jar_dir = os.getenv('WSHOME') + os.path.sep.join(['', 'lib', 'stanford-postagger', ''])
tagger_model_dir = os.getenv('WSHOME') + os.path.sep.join(['', 'lib', 'stanford-postagger', 'models', ''])

# stores = ['MidwayUSA', 'Overstock.com']
# stores = ['FragranceNet.com']
stores = ['MidwayUSA']
# lemmatization_modes = [LemmatizationMode.VOCABULARY, LemmatizationMode.PARAGRAPH]
lemmatization_modes = [LemmatizationMode.VOCABULARY]
label_name = 'POSNEG'
feature_selection_modes = [FeatureSelectionMode.FREQUENCY, FeatureSelectionMode.CHI_SQUARE]
num_selected_features = [10, 25, 50, 100, 200, 500, 1000]

dag_start_task = DummyOperator(task_id='start', dag=dag)

for store in stores:
    fetch_task = PythonOperator(task_id='fetch__{}'.format(store), dag=dag, provide_context=True,
                                python_callable=fetch_by_store,
                                op_kwargs={
                                    'conn_id': 'consumer_reviews_default',
                                    'store': store,
                                    'sql_str': predefined_sql['all_fields_filtered_by_store'],
                                    'working_dir': working_dir,
                                    'include_index': False
                                })

    curate_task = PythonOperator(task_id='curate__{}'.format(store), dag=dag, provide_context=True,
                                 python_callable=compute_curated_attributes,
                                 op_kwargs={
                                     'attributes_to_calculate': predefined_curated_attributes,
                                     'store': store,
                                     'working_dir': working_dir,
                                     'include_index': False
                                 })

    split_task = PythonOperator(task_id='split__{}'.format(store), dag=dag, provide_context=True,
                                python_callable=split_dataset,
                                op_kwargs={
                                    'range_definition': predefined_range_definitions,
                                    'store': store,
                                    'working_dir': working_dir,
                                    'include_index': False
                                })

    lemmatize_start_task = DummyOperator(task_id='lemmatize_start__{}'.format(store), dag=dag)
    lemmatize_end_task = DummyOperator(task_id='lemmatize_end__{}'.format(store), dag=dag)

    dag_start_task >> fetch_task >> curate_task >> split_task
    fetch_task >> lemmatize_start_task

    for mode in lemmatization_modes:
        lemmatize_task = PythonOperator(task_id='lemmatize__{}_{}'.format(store, str(mode)), dag=dag,
                                        provide_context=True, python_callable=lemmatize,
                                        op_kwargs={
                                            'tagger_config': {
                                                'jar_path': tagger_jar_dir + 'stanford-postagger.jar',
                                                'model_path': {
                                                    'left3words': tagger_model_dir + 'english-left3words-distsim.tagger',
                                                    'bidirectional':
                                                        tagger_model_dir + 'english-bidirectional-distsim.tagger'
                                                },
                                                'model': 'left3words'  # or 'bidirectional' (slower but more accurate)
                                            },
                                            'part_of_speech': 'default',
                                            'lemmatizer': 'default',
                                            'mode': mode,
                                            'stopwords': 'default',
                                            'store': store,
                                            'working_dir': working_dir
                                        })

        score_task = PythonOperator(task_id='score__{}_{}'.format(store, str(mode)), dag=dag, provide_context=True,
                                    python_callable=compute_sentiment_score,
                                    op_kwargs={
                                        'mode': mode,
                                        'store': store,
                                        'working_dir': working_dir
                                    })

        clean_task = PythonOperator(task_id='clean__{}_{}'.format(store, str(mode)), dag=dag, provide_context=True,
                                    python_callable=clean_undesired,
                                    op_kwargs={
                                        'mode': mode,
                                        'store': store,
                                        'working_dir': working_dir
                                    })

        lemmatize_start_task >> lemmatize_task >> score_task >> lemmatize_end_task
        lemmatize_task >> clean_task >> lemmatize_end_task

    nlp_start_task = DummyOperator(task_id='nlp_start__{}'.format(store), dag=dag)
    nlp_end_task = DummyOperator(task_id='nlp_end__{}'.format(store), dag=dag)

    [split_task, lemmatize_end_task] >> nlp_start_task

    for mode in lemmatization_modes:
        nlp_all_words_task = PythonOperator(task_id='process_all_words__{}_{}'.format(store, str(mode)), dag=dag,
                                            provide_context=True, python_callable=process_all,
                                            op_kwargs={
                                                'positive_label_threshold': 9,
                                                'label_name': label_name,
                                                'stopwords': 'default',
                                                'mode': mode,
                                                'store': store,
                                                'working_dir': working_dir,
                                                'include_index': False
                                            })
        nlp_start_task >> nlp_all_words_task >> nlp_end_task

        nlp_nouns_task = PythonOperator(task_id='process_nouns__{}_{}'.format(store, str(mode)), dag=dag,
                                        provide_context=True, python_callable=process_nouns,
                                        op_kwargs={
                                            'positive_label_threshold': 9,
                                            'label_name': label_name,
                                            'stopwords': 'default',
                                            'mode': mode,
                                            'store': store,
                                            'working_dir': working_dir,
                                            'include_index': False
                                        })
        nlp_start_task >> nlp_nouns_task >> nlp_end_task

        nlp_transform_task = PythonOperator(task_id='transform_objective__{}_{}'.format(store, str(mode)), dag=dag,
                                            provide_context=True, python_callable=transform_objective,
                                            op_kwargs={
                                                'positive_label_threshold': 9,
                                                'label_name': label_name,
                                                'stopwords': 'default',
                                                'sep': sentence_sep,
                                                'mode': mode,
                                                'store': store,
                                                'working_dir': working_dir,
                                                'include_index': False
                                            })
        nlp_start_task >> nlp_transform_task >> nlp_end_task

    feature_start_task = DummyOperator(task_id='feature_start__{}'.format(store), dag=dag)
    feature_end_task = DummyOperator(task_id='feature_end__{}'.format(store), dag=dag)

    nlp_end_task >> feature_start_task

    for lem_mode in lemmatization_modes:
        for feature_mode in feature_selection_modes:
            feature_selection_task = PythonOperator(
                task_id='feature_selection__{}_{}_{}'.format(store, str(lem_mode), str(feature_mode)), dag=dag,
                provide_context=True, python_callable=feature_selection,
                op_kwargs={
                    'lem_mode': lem_mode,
                    'feature_mode': feature_mode,
                    'num_features': num_selected_features,
                    'label_name': label_name,
                    'store': store,
                    'working_dir': working_dir,
                    'include_index': False
                })
            feature_start_task >> feature_selection_task >> feature_end_task
