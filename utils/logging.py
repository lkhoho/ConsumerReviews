import logging


def get_logger(name, **formats):
    fmt = formats.get('format', '%(levelname)s [%(module)s] - %(asctime)s: %(message)s')
    date_format = formats.get('date_format', '%m/%d/%Y %I:%M:%S %p')
    formatter = logging.Formatter(fmt=fmt, datefmt=date_format)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    if logger.handlers is None:
        logger.addHandler(handler)
    return logger
