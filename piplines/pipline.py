from etl.logger import setup_logging, get_stage_logger
from etl.extract import extract_data
from etl.config import load_cfg, validate_cfg
import pandas as pd
import logging
from etl.transform import transform
from etl.load import load_data, get_bd_connection

logger = get_stage_logger("pipline")

def run_pipline():
    setup_logging()
    logger.info("Запуск ETL пайплайна")

    cfg = load_cfg("configs/config.json")
    cfg = validate_cfg(cfg)
    data = extract_data(cfg)
    df = transform(data)
    conn = get_bd_connection(cfg)

    load_data(conn, "main_table", df)
    logger.info("ETL пайплайн завершен")
    print(df)
    conn.close()