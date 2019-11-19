from shutil import copy


def setup(data_pathname: str):
    working_dir = Variable.get('working_dir')
    os.makedirs(working_dir, exist_ok=True)
    logging.info('Working dir=' + working_dir)
    logging.info('Data pathname=' + data_pathname)
    pos = filename.rfind('/')
    filename = filename[pos:]
    save_pathname = os.sep.join([working_dir, context['ds_nodash'], filename])
    copy(data_pathname, save_pathname)
    logging.info('Data file is copied to ' + save_pathname)

    return filename
