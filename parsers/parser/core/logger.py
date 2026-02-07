import logging
import sys


def setup_logger():
    logger = logging.getLogger("rentcheck")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Дата Время | Уровень | Сообщение
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.propagate = False

    return logger