import logging
import sys
import time


class RelativeTimeFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()

    def format(self, record):
        # сколько секунд прошло с момента создания логгера
        elapsed_seconds = record.created - self.start_time
        elapsed_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_seconds))
        record.elapsed = elapsed_time
        return super().format(record)


def setup_logger():
    logger = logging.getLogger("rentcheck")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = RelativeTimeFormatter("[%(elapsed)s] %(levelname)s | %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.propagate = False

    return logger
