from copy import deepcopy
from etl.logger import get_stage_logger

import re
import json
import os

DEFAULT_CFG = {
    "base_url": "https://api.worldbank.org/v2",
    "per_page": 500,
    "retries": 3,
    "timeout": 10,
    "pause": 2,
    # если нужно выбрать все страны пишем просто "all"
    # оставь пустым [] если разрез стран вообще не нужен
    "countries": ["CHN"],
    # если нужно выбрать все индикаторы пишем просто "all" но лучше не надо
    # оставь пустым [] если разрез индексов вообще не нужен
    "indicators": ["NY.GDP.PCAP.CD"],
    # диапазон - "date_st:date_end"
    # конкретная дата - "date", перечисление дат "date_1,date_2,..."
    # все даты пишем "all"
    "date_interval": ["2020:2024"],
    "host": "aws-1-eu-west-1.pooler.supabase.com",
    "port": 6543,
    "user": "postgres.uuzwbewejtdochhwkaig",
    "dbname": "postgres",
    "password": "?YhPqvr5Ej3M7Jc",
}

logger = get_stage_logger("extract")

def validate_cfg(cfg:dict) -> dict:
    """
    Функция, которая осуществляет валидацию значений словаря-config, и возвращает словарь-config с валидированными знач.
    Его можно получить в функции load_cfg которая подгрузит параметры из нужного файла
    или ввести вручную словарь необходимых параметров, или даже пустой словарь, функция заменит на значения по умолчанию
    те можно использовать отдельно
    функция делает вызовы по api WB более робастными и отрабатывает некоторые опечатки,
    однако стоит все же следовать инструкциям по заполненbю config.json
    """

    logger.info(f"Валидация config-файла")
    validated_cfg = cfg

    # base_url
    if not isinstance(cfg.get("base_url"), str) or not cfg["base_url"].startswith("http"):
        logger.info("Некорректный base_url. Заменяем на значение по умолчанию")
        validated_cfg["base_url"] = deepcopy(DEFAULT_CFG["base_url"])
    else:
        validated_cfg["base_url"] = cfg["base_url"].rstrip("/")

    # per_page: int > 0
    try:
        per_page = int(cfg.get("per_page", 0))
        if per_page <= 0:
            raise ValueError
        validated_cfg["per_page"] = per_page
    except (ValueError, TypeError):
        logger.info("Некорректный per_page. Заменяем на значение по умолчанию")
        validated_cfg["per_page"] = deepcopy(DEFAULT_CFG["per_page"])

    # timeout, pause, retries: int >= 0
    for key in ("timeout", "pause", "retries"):
        try:
            val = int(cfg.get(key, -1))
            if val < 0:
                raise ValueError
            validated_cfg[key] = val
        except (ValueError, TypeError):
            logger.info(f"Некорректный {key}. Заменяем на значение по умолчанию")
            validated_cfg[key] = deepcopy(DEFAULT_CFG[key])

    # countries: list[str] | "all" | []
    # indicators: list[str] | "all" | []
    for key in ("indicators", "countries"):
        val = cfg.get(key, [])
        validated_cfg[key] = []

        if isinstance(val, str):
            val = [val]

        if isinstance(val, list):
            temp_list = []
            has_invalid = False

            for item in val:
                if not isinstance(item, str):
                    has_invalid = True
                    continue
                item = item.strip()
                if not item:
                    continue
                if item.lower() == "all":
                    validated_cfg[key] = ["all"]
                    break
                if key == "countries":
                    temp_list.append(item.upper())
                else:
                    temp_list.append(item)

            else:
                if not temp_list and has_invalid:
                    logger.info(f"Некорректный формат {key}. Заменяем на значение по умолчанию")
                    validated_cfg[key] = deepcopy(DEFAULT_CFG[key])
                elif validated_cfg[key] != ["all"]:
                    validated_cfg[key] = temp_list
        else:
            logger.info(f"Некорректный формат {key}. Заменяем на значение по умолчанию")
            validated_cfg[key] = deepcopy(DEFAULT_CFG[key])
    # Для цикла
    #if not validated_cfg["indicators"]:
    #    validated_cfg["indicators"] = [""]

    # date_interval: List[str] | []
    di = cfg.get("date_interval", "")
    validated_cfg["date_interval"] = []
    if isinstance(di, int):
        di = str(di)
    if isinstance(di, str):
        di = [di]
    if isinstance(di, list):
        for date in di:
            if isinstance(date, int):
                date = str(date)
            if isinstance(date, str):
                date = re.sub(r"\s+", "", date)
                if date.lower() == "all":
                    validated_cfg["date_interval"] = []
                    break
                if not date:
                    continue

                parts = [p for p in date.split(",") if p]
                for val in parts:
                    if not val:
                        continue
                    if ":" in val:
                        first_last = val.split(":")
                        if len(first_last) == 2 and all(p.isdigit() and len(p) == 4 for p in first_last):
                            validated_cfg["date_interval"].append(val)
                            continue
                    elif val.isdigit() and len(val) == 4:
                        validated_cfg["date_interval"].append(val)
    else:
        logger.info(f"Некорректный формат date_interval. Заменяем на значение по умолчанию")
        validated_cfg["date_interval"] = deepcopy(DEFAULT_CFG["date_interval"])
    logger.info(f"Валидация прошла успешно")
    return validated_cfg

def load_cfg(path: str = "config.json") -> dict:
    '''
    Данная функция выгружает конфигурацию из файла, при его отсутствии создает файл с дефолтными настройками.
    Возвращает валидированную конфигурацию (обязательные ключи добавляются со значениями по умолчанию)
    Валидация значений осуществляется в отдельной функции validate_cfg
    '''
    logger.info(f"Загрузка config-файла")
    if not os.path.exists(path):
        logger.info(f"config-файла {path} не существует, по умолчанию создан файл с дефолтными настройками")
        with open(path, 'w', encoding="utf-8") as f:
            json.dump(DEFAULT_CFG, f, indent=4)
        return deepcopy(DEFAULT_CFG)

    try:
        with open(path, 'r', encoding="utf-8") as f:
            cfg = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.info(f"Ошибка чтения config-файла {e}, по умолчанию используем дефолтные настройки")
        return deepcopy(DEFAULT_CFG)

    for key, value in DEFAULT_CFG.items():
        if key not in cfg:
            logger.info(f"Добавлен недостающий ключ {key} с дефолтным значением")
            cfg[key] = deepcopy(value)
    logger.info(f"config-файл {path} успешно загружен.")
    return cfg