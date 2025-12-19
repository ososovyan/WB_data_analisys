
from copy import deepcopy
from typing import Tuple

import requests as rq
from etl.logger import get_stage_logger
import time

logger = get_stage_logger("extract")

def safe_request(processed_cfg:dict) -> Tuple[dict, list[dict]]:
    '''
    Возможно не самая оптимальная и красивая функция в которой происходит прямой запрос к бд
    в целом к любой бд - она универсальная
    '''
    url_endpoint = processed_cfg["url"]
    params = processed_cfg["params"]
    timeout = processed_cfg["timeout"]
    pause = processed_cfg["pause"]
    retries = processed_cfg["retries"]

    for i in range(1, 1 + retries):
        logger.info(f"GET-запрос попытка {i}/{retries}, {url_endpoint}, {params}")
        # Ловим ошибки
        try:
            response = rq.get(url_endpoint, params=params, timeout=timeout)
            response.raise_for_status()
        except rq.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)

            if status == 429:
                logger.info(f"Слишком много запросов (429). ПОВТОРЯЕМ попытку через {pause} сек...")
            elif status and 400 <= status < 500:
                logger.info(f"Клиентская ошибка {e.response.status_code}. ОСТАНОВКА!!!")
                break
            else:
                logger.info(f"HTTPError {status}. ПОВТОРЯЕМ попытку через {pause} сек...")
            if i < retries:
                time.sleep(pause)
            continue
        except (rq.exceptions.Timeout, rq.exceptions.ConnectionError) as e:
            logger.info(f"Ошибка соединения: {e}. Попытка {i}/{retries}. ПОВТОРЯЕМ попытку через {pause} сек...")
            if i < retries:
                time.sleep(pause)
            continue
        except Exception as e:
            logger.info(f"Неизвестная ошибка запроса: {e}")
            raise
        # Если вдруг файл пустой или с ним что-то
        try:
            data = response.json()
        except ValueError:
            logger.info("Ответ не является корректным JSON. ПОВТОРЯЕМ попытку...")
            if i < retries:
                time.sleep(pause)
            continue

        if not data:
            logger.info("JSON пустой. ПОВТОРЯЕМ попытку...")
            if i < retries:
                time.sleep(pause)
            continue

        metadata, data_list = data[0], data[1]

        if not isinstance(metadata, dict):
            logger.info(f"Некорректный формат данных metadata: {type(metadata)}")
            if i < retries:
                time.sleep(pause)
            continue

        if not isinstance(data_list, list):
            logger.info(f"Некорректный формат данных data_list: {type(data_list)}")
            if i < retries:
                time.sleep(pause)
            continue


        return metadata, data_list

    logger.info(f"GET-запрос не удался после {i} попыток")
    raise RuntimeError(f"GET-запрос не удался после {i} попыток, {processed_cfg}")

def get_paginated_data(processed_cfg:dict,
                       first_page: int = 1,
                       last_page: int | None = None) -> list[dict]:
    '''
    Это тоже универсальная функция, которая отрабатывает пагинацию.
    Есть опция выбирать страницы (она условна мало ли, по умолчанию это всегда с 1 по последнюю страницу
    при этом логика внутри исключает ошибки вызванные некорректным выбором параметров first_page и last_page
    '''
    full_data = []
    local_processed_cfg = deepcopy(processed_cfg)

    if first_page < 1:
        logger.warning(f"Некорректный first_page={first_page}, используем 1")
        first_page = 1
    page = first_page

    while True:
        local_processed_cfg['params']['page'] = page
        logger.info(f"Страница номер {page}")

        metadata, data_list = safe_request(local_processed_cfg)
## Отработать
        if data_list:
            full_data.extend(data_list)
        else:
            logger.info(f"Страница номер {page} пустая, пропускаем")

        total_pages = metadata.get("pages", page)
        if last_page is not None:
            total_pages = min(total_pages, last_page)
        '''
        # Универсальная проверка отрабатывающая много исключений (некорректно большое first_page)однако, функция не дает 
        причину break - возможность выставить номера страниц, нужна условно вызов будет проиисходить внутри другой функции
        '''
        logger.info(f"Отработали {page} страниц из {total_pages}, всего строкк ")
        if page >= total_pages:
            logger.info("Достигнут конец выбранного диапазона страниц")
            break
        page += 1
    logger.info(f"Запрос успешно отработал!!!")
    return full_data

def process_cfg_for_api(raw_cfg:dict) -> dict:
    '''
    Не совсем обязательный блок его можно было реализовать как на этапе валидации, так изначально строить config
    таким способом.
    Аргументы
    - есть предположения что в config будут передаваться другие данные, чтобы не передавать лишнего в функции
    для запросов отдельно здесь собирается словарь только из того что нам потребуется
    - Хочется максимально гибко и автоматически совершать запрос к примеру когда нужно получить
    список стран без индикаторов, или только индикаторы -- здесь происходит сборка url endpoint
    - Плюс в конфиг более структура более удобна для пользователя, здесь сборка более удобна для работы с ней
    также задаются параметры которые на данном этапе (и в целом) не будет опции выбирать
    '''
    processed_cfg = {
        'url': '',
        'params': {
            "format": "json",
            "date": raw_cfg["date_interval"],
            "per_page": raw_cfg["per_page"],
            "page": 1
        },
        'timeout': raw_cfg["timeout"],
        'pause': raw_cfg["pause"],
        'retries': raw_cfg["retries"],
        'indicators': [""]
    }
    temp_url = raw_cfg["base_url"]
    if raw_cfg["countries"]:
        temp_url += "/country/" + ';'.join(raw_cfg["countries"])

    if raw_cfg["indicators"]:
        temp_url += "/indicator/"
        processed_cfg["indicators"] = raw_cfg["indicators"]

    processed_cfg["url"] = temp_url
    return processed_cfg