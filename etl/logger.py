import logging
import sys
import os

class StageFilter(logging.Filter):
    """Добавляет поле stage в запись лога, если не передан — ставим '-'"""
    def filter(self, record):
        if not hasattr(record, "stage"):
            record.stage = "-"
        return True


def setup_logging(log_file="logs/etl.log"):
    # Создаем папку для логов, если нет
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(stage)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    stage_filter = StageFilter()

    # Консольный вывод
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    console.addFilter(stage_filter)

    # Файл
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.addFilter(stage_filter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger

def get_stage_logger(stage_name: str):
    """Возвращает LoggerAdapter с привязанным stage"""
    logger = logging.getLogger("etl")
    return logging.LoggerAdapter(logger, {"stage": stage_name})