import os
import logging
from airflow.hooks.mysql_hook import MySqlHook

sql_fields = {
    'bizrate_all': '''
        `BIZRATE_REVIEW`.*,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.SHIPPING_OPTIONS AS SITE_SHIPPING_OPTIONS,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.SHIPPING_CHARGES AS SITE_SHIPPING_CHARGES,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.EASE_OF_FINDING AS SITE_EASE_OF_FINDING,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.PRICE_RELATIVE_TO_OTHER AS SITE_PRICE_RELATIVE_TO_OTHER,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.CLARITY_PRODUCT_INFO AS SITE_CLARITY_PRODUCT_INFO,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.CHARGES_STATED_CLEARLY AS SITE_CHARGES_STATED_CLEARLY,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.SITE_DESIGN AS SITE_SITE_DESIGN,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.PRODUCT_SELECTION AS SITE_PRODUCT_SELECTION,
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.CHECKOUT_SATISFACTION AS SITE_CHECKOUT_SATISFACTION,
        `BIZRATE_AFTER_PURCHASE_RATINGS`.PRODUCT_AVAILABILITY AS AFTER_PRODUCT_AVAILABILITY,
        `BIZRATE_AFTER_PURCHASE_RATINGS`.ONTIME_DELIVERY AS AFTER_ONTIME_DELIVERY,
        `BIZRATE_AFTER_PURCHASE_RATINGS`.PRODUCT_MET_EXPECTATIONS AS AFTER_PRODUCT_MET_EXPECTATIONS,
        `BIZRATE_AFTER_PURCHASE_RATINGS`.ORDER_TRACKING AS AFTER_ORDER_TRACKING,
        `BIZRATE_AFTER_PURCHASE_RATINGS`.RETURN_PROCESS AS AFTER_RETURN_PROCESS,
        `BIZRATE_AFTER_PURCHASE_RATINGS`.CUSTOMER_SUPPORT AS AFTER_CUSTOMER_SUPPORT
        FROM `BIZRATE_REVIEW` INNER JOIN `BIZRATE_SITE_EXPERIENCE_RATINGS`
        ON `BIZRATE_REVIEW`.PID = `BIZRATE_SITE_EXPERIENCE_RATINGS`.FK_REVIEW_PID 
        INNER JOIN `BIZRATE_AFTER_PURCHASE_RATINGS`
        ON `BIZRATE_REVIEW`.PID = `BIZRATE_AFTER_PURCHASE_RATINGS`.FK_REVIEW_PID
        '''
}

sql_conditions = {
    'bizrate_has_all_site_experience_ratings': '''
        `BIZRATE_SITE_EXPERIENCE_RATINGS`.SHIPPING_OPTIONS IS NOT NULL 
        AND `BIZRATE_SITE_EXPERIENCE_RATINGS`.SHIPPING_CHARGES IS NOT NULL 
        AND `BIZRATE_SITE_EXPERIENCE_RATINGS`.EASE_OF_FINDING IS NOT NULL 
        AND `BIZRATE_SITE_EXPERIENCE_RATINGS`.PRICE_RELATIVE_TO_OTHER IS NOT NULL 
        AND `BIZRATE_SITE_EXPERIENCE_RATINGS`.CLARITY_PRODUCT_INFO IS NOT NULL 
        AND `BIZRATE_SITE_EXPERIENCE_RATINGS`.CHARGES_STATED_CLEARLY IS NOT NULL 
        AND `BIZRATE_SITE_EXPERIENCE_RATINGS`.SITE_DESIGN IS NOT NULL 
        AND `BIZRATE_SITE_EXPERIENCE_RATINGS`.PRODUCT_SELECTION IS NOT NULL 
        AND `BIZRATE_SITE_EXPERIENCE_RATINGS`.CHECKOUT_SATISFACTION IS NOT NULL
        ''',
    'bizrate_has_before_purchase_review': '''
        `BIZRATE_REVIEW`.BEFORE_REVIEW IS NOT NULL
        ''',
    'bizrate_has_all_overall_scores': '''
        `BIZRATE_REVIEW`.SHOP_AGAIN IS NOT NULL
        AND `BIZRATE_REVIEW`.TO_RECOMMEND IS NOT NULL
        AND `BIZRATE_REVIEW`.SATISFACTION IS NOT NULL
        ''',
    'bizrate_parameter_review_store': '''
        `BIZRATE_REVIEW`.STORE=%s
        '''
}


def fetch_bizrate_all_fields_by_store(db_conn_id, store, working_dir, include_index, **context):
    """
    Fetch Bizrate reviews filtered by given store name, site experience ratings, and after purchase ratings that have
    complete scores and site experience ratings.

    Keyword argument:
    store -- given store name
    """

    conn_id = db_conn_id
    exec_date = context['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    mysql_hook = MySqlHook(conn_id)
    if not os.path.exists(working_dir):
        os.makedirs(working_dir)
    sql_str = 'SELECT ' + sql_fields['bizrate_all'] \
              + ' WHERE ' + sql_conditions['bizrate_has_all_site_experience_ratings'] \
              + ' AND ' + sql_conditions['bizrate_has_before_purchase_review'] \
              + ' AND ' + sql_conditions['bizrate_has_all_overall_scores'] \
              + ' AND ' + sql_conditions['bizrate_parameter_review_store']
    logging.info('Execute SQL query: {}'.format(sql_str % store))
    df = mysql_hook.get_pandas_df(sql=sql_str, parameters=(store,))
    file_name = store + '.csv'
    path_file = working_dir + os.sep + file_name
    df.to_csv(path_file, index=include_index)
    logging.info('Save dataframe to ' + path_file)
    return {
        'input_files': [None],
        'output_files': [file_name]
    }
