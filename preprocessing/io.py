import os
import re
from airflow.hooks.mysql_hook import MySqlHook

predefined_sql = {
    "all_fields_filtered_by_store": """
SELECT REVIEW.*, 
SITE.SHIPPING_OPTIONS AS SITE_SHIPPING_OPTIONS,
SITE.SHIPPING_CHARGES AS SITE_SHIPPING_CHARGES,
SITE.EASE_OF_FINDING AS SITE_EASE_OF_FINDING,
SITE.PRICE_RELATIVE_TO_OTHER AS SITE_PRICE_RELATIVE_TO_OTHER,
SITE.CLARITY_PRODUCT_INFO AS SITE_CLARITY_PRODUCT_INFO,
SITE.CHARGES_STATED_CLEARLY AS SITE_CHARGES_STATED_CLEARLY,
SITE.SITE_DESIGN AS SITE_SITE_DESIGN,
SITE.PRODUCT_SELECTION AS SITE_PRODUCT_SELECTION,
SITE.CHECKOUT_SATISFACTION AS SITE_CHECKOUT_SATISFACTION,
AFTER.PRODUCT_AVAILABILITY AS AFTER_PRODUCT_AVAILABILITY, 
AFTER.ONTIME_DELIVERY AS AFTER_ONTIME_DELIVERY, 
AFTER.PRODUCT_MET_EXPECTATIONS AS AFTER_PRODUCT_MET_EXPECTATIONS, 
AFTER.ORDER_TRACKING AS AFTER_ORDER_TRACKING, 
AFTER.RETURN_PROCESS AS AFTER_RETURN_PROCESS,
AFTER.CUSTOMER_SUPPORT AS AFTER_CUSTOMER_SUPPORT
FROM BIZRATE_REVIEW REVIEW INNER JOIN BIZRATE_SITE_EXPERIENCE_RATINGS SITE ON REVIEW.PID = SITE.FK_REVIEW_PID 
INNER JOIN BIZRATE_AFTER_PURCHASE_RATINGS AFTER on REVIEW.PID = AFTER.FK_REVIEW_PID
WHERE SITE.SHIPPING_OPTIONS IS NOT NULL AND SITE.SHIPPING_CHARGES IS NOT NULL 
AND SITE.EASE_OF_FINDING IS NOT NULL AND SITE.PRICE_RELATIVE_TO_OTHER IS NOT NULL 
AND SITE.CLARITY_PRODUCT_INFO IS NOT NULL AND SITE.CHARGES_STATED_CLEARLY IS NOT NULL 
AND SITE.SITE_DESIGN IS NOT NULL AND SITE.PRODUCT_SELECTION IS NOT NULL 
AND SITE.CHECKOUT_SATISFACTION AND REVIEW.STORE=%s
AND REVIEW.BEFORE_REVIEW IS NOT NULL
"""
}


def fetch_by_store(conn_id, store, sql_str, working_dir, include_index, **kwargs):
    conn_id = conn_id
    exec_date = kwargs['execution_date'].strftime('%Y%m%d')
    working_dir += os.path.sep + exec_date
    db = MySqlHook(conn_id)
    # sql = _parse_parms_to_sql(kwargs)
    norm_stores = []
    if not os.path.exists(working_dir):
        os.makedirs(working_dir)
    df = db.get_pandas_df(sql=sql_str, parameters=(store,))
    store = _norm_store(store)
    norm_stores.append(norm_stores)
    file_name = store + '__original__.csv'
    df.to_csv(working_dir + os.sep + file_name, index=include_index)


def _norm_store(store):
    return re.sub("['&, ]", '', store)


def _parse_parms_to_sql(kwargs: dict) -> str:
    # query_fields = ', '.join(kwargs['sql_fields'])
    #
    # if kwargs['sql_join'] is None:
    #     join_sql = None
    # elif kwargs['sql_join'] == 'inner':
    #     join_sql = 'INNER JOIN'
    # elif kwargs['sql_join'] == 'left':
    #     join_sql = 'LEFT JOIN'
    # elif kwargs['sql_join'] == 'right':
    #     join_sql = 'RIGHT JOIN'
    # elif kwargs['sql_join'] == 'outer':
    #     join_sql = 'OUTER JOIN'
    #
    # fks = kwargs['sql_fks']
    #
    # tbl_sql = kwargs['sql_tables'][0]
    # if len(kwargs['sql_tables']) > 1:
    #     for tbl in kwargs['sql_tables'][1:]
    #
    # return 'SELECT ' + query_fields + ' FROM ' +
    pass



