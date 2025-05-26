import logging
from os.path import join, abspath, exists, dirname
from os import makedirs


BASE_DIR = dirname(abspath(__file__))
LOG_FOLDER = join(BASE_DIR, "..", "log")

if not exists(LOG_FOLDER):
    makedirs(LOG_FOLDER)

def create_custom_logger(name_logger: str = "BDD") -> logging.Logger:
    """
    Créer le logger au format bdd
    :param name_logger: nom du logger
    :return: Logger
    """
    logger = logging.getLogger(name_logger)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        logger_file = join(LOG_FOLDER, f"{name_logger}.log")
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        fh = logging.FileHandler(logger_file)
        fh.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        ch.setFormatter(formatter)
        fh.setFormatter(formatter)

        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger

QUERY_LOG = create_custom_logger("QUERY")
MANAGER_LOG = create_custom_logger("MANAGER")
