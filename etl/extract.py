
from etl.api import process_cfg_for_api, get_paginated_data
from etl.logger import get_stage_logger

import pandas as pd
import sys

logger = get_stage_logger("extract")

def extract_data(raw_cfg:dict) -> list:
    processed_cfg = process_cfg_for_api(raw_cfg)
    logger.info(f"НАЧИНАЕМ ЗАГРУЖАТЬ ДАННЫЕ!!!")

    url_temp = processed_cfg["url"]
    full_data = []

    for indicator in processed_cfg["indicators"]:
        processed_cfg["url"] = url_temp + indicator
        full_data.extend(get_paginated_data(processed_cfg))
        processed_cfg["url"] = url_temp
    # df = pd.DataFrame(data=full_data)
    if not full_data:
        logger.info(f"Пусто!!! {processed_cfg}")
        sys.exit("Не получено данных для данного набора параметров. ОСТАНОВКА!!!")
    return full_data