import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

sys.path.append('/Users/keliu/Developer/python/ConsumerReviews')
from preprocessing.io import fetch_bizrate_all_fields_by_store
from preprocessing.balance_samples import over_sampling_reviews
from preprocessing.curated_attributes import site_experience_ratings, CuratedAttribute, compute_curated_attributes
from preprocessing.split import predefined_range_definitions, split_dataset
from preprocessing.lemmatization import lemmatize, compute_sentiment_score, clean_undesired
from preprocessing.nlp import process_all, process_nouns, transform_objective
from preprocessing.feature_selection import feature_selection
from configs import CommonConfig, LemmatizationConfig, FeatureSelectionConfig


bizrate_args = {
    'common': CommonConfig,
    'curated_attributes': [
        CuratedAttribute(name='variance', depends_on=site_experience_ratings, output_name='VARIANCE', as_label=False),
        CuratedAttribute(name='skewness', depends_on=site_experience_ratings, output_name='SKEWNESS', as_label=False),
        CuratedAttribute(name='kurtosis', depends_on=site_experience_ratings, output_name='KURTOSIS', as_label=False),
        CuratedAttribute(name='range', depends_on=site_experience_ratings, output_name='RANGE', as_label=False),
        CuratedAttribute(name='shop_again', depends_on='SHOP_AGAIN', output_name='SHOP_AGAIN_POSNEG',
                         positive_threshold=9, as_label=True),
        CuratedAttribute(name='to_recommend', depends_on='TO_RECOMMEND', output_name='TO_RECOMMEND_POSNEG',
                         positive_threshold=9, as_label=True),
        CuratedAttribute(name='satisfaction', depends_on='SATISFACTION', output_name='SATISFACTION_POSNEG',
                         positive_threshold=9, as_label=True)
    ],
    'lemmatization': LemmatizationConfig,
    'stores': ['MidwayUSA'],
    'label_name': 'POSNEG',
    'feature_selection': FeatureSelectionConfig,
}

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

dag_start_task = DummyOperator(task_id='start', dag=dag)
for store in bizrate_args['stores']:
    fetch_task = PythonOperator(task_id='fetch__{}'.format(store), dag=dag, provide_context=True,
                                python_callable=fetch_bizrate_all_fields_by_store,
                                op_kwargs={
                                    'db_conn_id': 'consumer_reviews_default',
                                    'store': store,
                                    'working_dir': bizrate_args['common'].working_dir,
                                    'include_index': False
                                })

    curate_task = PythonOperator(task_id='curate__{}'.format(store), dag=dag, provide_context=True,
                                 python_callable=compute_curated_attributes,
                                 op_kwargs={
                                     'attributes': bizrate_args['curated_attributes'],
                                     'store': store,
                                     'working_dir': bizrate_args['common'].working_dir,
                                     'include_index': False
                                 })

    over_sampling_task = PythonOperator(task_id='over_sampling__{}'.format(store), dag=dag, provide_context=True,
                                        python_callable=over_sampling_reviews,
                                        op_kwargs={
                                            'target_fields': [attrib.output_name for attrib in list(
                                                filter(lambda x: x.as_label, bizrate_args['curated_attributes'])
                                            )],
                                            'store': store,
                                            'working_dir': bizrate_args['common'].working_dir,
                                            'include_index': False
                                        })

    # split_task = PythonOperator(task_id='split__{}'.format(store), dag=dag, provide_context=True,
    #                             python_callable=split_dataset,
    #                             op_kwargs={
    #                                 'range_definition': predefined_range_definitions,
    #                                 'store': store,
    #                                 'working_dir': working_dir,
    #                                 'include_index': False
    #                             })

    lemmatize_start_task = DummyOperator(task_id='lemmatize_start__{}'.format(store), dag=dag)
    lemmatize_end_task = DummyOperator(task_id='lemmatize_end__{}'.format(store), dag=dag)

    dag_start_task >> fetch_task >> curate_task >> over_sampling_task  # >> split_task
    fetch_task >> lemmatize_start_task

    for mode in bizrate_args['lemmatization'].modes:
        lemmatize_task = PythonOperator(task_id='lemmatize__{}_{}'.format(store, str(mode)), dag=dag,
                                        provide_context=True, python_callable=lemmatize,
                                        op_kwargs={
                                            'lemmatization_config': bizrate_args['lemmatization'],
                                            'stopwords': bizrate_args['common'].get_stopwords(),
                                            'mode': mode,
                                            'store': store,
                                            'working_dir': bizrate_args['common'].working_dir
                                        })

        score_task = PythonOperator(task_id='score__{}_{}'.format(store, str(mode)), dag=dag, provide_context=True,
                                    python_callable=compute_sentiment_score,
                                    op_kwargs={
                                        'sentence_sep': bizrate_args['lemmatization'].sentence_sep,
                                        'mode': mode,
                                        'store': store,
                                        'working_dir': bizrate_args['common'].working_dir
                                    })

        clean_task = PythonOperator(task_id='clean__{}_{}'.format(store, str(mode)), dag=dag, provide_context=True,
                                    python_callable=clean_undesired,
                                    op_kwargs={
                                        'sentence_sep': bizrate_args['lemmatization'].sentence_sep,
                                        'mode': mode,
                                        'store': store,
                                        'working_dir': bizrate_args['common'].working_dir
                                    })

        lemmatize_start_task >> lemmatize_task >> score_task >> lemmatize_end_task
        lemmatize_task >> clean_task >> lemmatize_end_task

    nlp_start_task = DummyOperator(task_id='nlp_start__{}'.format(store), dag=dag)
    nlp_end_task = DummyOperator(task_id='nlp_end__{}'.format(store), dag=dag)

    # [split_task, lemmatize_end_task] >> nlp_start_task
    lemmatize_end_task >> nlp_start_task

    for mode in bizrate_args['lemmatization'].modes:
        nlp_all_words_task = PythonOperator(task_id='process_all_words__{}_{}'.format(store, str(mode)), dag=dag,
                                            provide_context=True, python_callable=process_all,
                                            op_kwargs={
                                                'stopwords': bizrate_args['common'].get_stopwords(),
                                                'mode': mode,
                                                'store': store,
                                                'working_dir': bizrate_args['common'].working_dir,
                                                'include_index': False
                                            })
        nlp_start_task >> nlp_all_words_task >> nlp_end_task

        nlp_nouns_task = PythonOperator(task_id='process_nouns__{}_{}'.format(store, str(mode)), dag=dag,
                                        provide_context=True, python_callable=process_nouns,
                                        op_kwargs={
                                            'stopwords': bizrate_args['common'].get_stopwords(),
                                            'mode': mode,
                                            'store': store,
                                            'working_dir': bizrate_args['common'].working_dir,
                                            'include_index': False
                                        })
        nlp_start_task >> nlp_nouns_task >> nlp_end_task

        nlp_transform_task = PythonOperator(task_id='transform_objective__{}_{}'.format(store, str(mode)), dag=dag,
                                            provide_context=True, python_callable=transform_objective,
                                            op_kwargs={
                                                'stopwords': bizrate_args['common'].get_stopwords(),
                                                'mode': mode,
                                                'sep': bizrate_args['lemmatization'].sentence_sep,
                                                'store': store,
                                                'working_dir': bizrate_args['common'].working_dir,
                                                'include_index': False
                                            })
        nlp_start_task >> nlp_transform_task >> nlp_end_task

    feature_start_task = DummyOperator(task_id='feature_start__{}'.format(store), dag=dag)
    feature_end_task = DummyOperator(task_id='feature_end__{}'.format(store), dag=dag)

    nlp_end_task >> feature_start_task

    for lem_mode in bizrate_args['lemmatization'].modes:
        for feature_mode in bizrate_args['feature_selection'].modes:
            feature_selection_task = PythonOperator(
                task_id='feature_selection__{}_{}_{}'.format(store, str(lem_mode), str(feature_mode)), dag=dag,
                provide_context=True, python_callable=feature_selection,
                op_kwargs={
                    'lem_mode': lem_mode,
                    'feature_mode': feature_mode,
                    'num_features': bizrate_args['feature_selection'].num_features,
                    'store': store,
                    'working_dir': bizrate_args['common'].working_dir,
                    'include_index': False
                })
            feature_start_task >> feature_selection_task >> feature_end_task
