import os
import logging
from shutil import copy
from airflow.models import Variable


def setup(data_pathname: str,
          **context):
    working_dir = Variable.get('working_dir')
    os.makedirs(working_dir + os.sep + context['ds_nodash'], exist_ok=True)
    logging.info('Working dir=' + working_dir)
    logging.info('Data pathname=' + data_pathname)
    pos = data_pathname.rfind('/')
    filename = data_pathname[pos+1:]
    logging.info('Data filename=' + filename)
    save_pathname = os.sep.join([working_dir, context['ds_nodash'], filename])
    copy(data_pathname, save_pathname)
    logging.info('Data file is copied to ' + save_pathname)

    return filename
