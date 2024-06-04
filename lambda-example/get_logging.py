import logging
import traceback


def get_logging():
    try:
        # output log format
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(message)s')
        for handler in logger.handlers:
            handler.setFormatter(formatter)
        return logging
    except Exception as e:
        traceback.print_exc()
