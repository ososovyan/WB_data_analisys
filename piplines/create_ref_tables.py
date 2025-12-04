from etl.logger import setup_logging, get_stage_logger
from etl.extract import extract_data
from etl.config import load_cfg, validate_cfg
import pandas as pd
from etl.transform import transform, normalize_reference_from_key
from etl.load import load_data, get_bd_connection


logger = get_stage_logger("pipline")

def create_ref_tables_con():
    setup_logging()
    logger.info("Запуск пайплайна для создания/обновления справочных таблиц для стран")

    #-----------Countries----------------
    cfg = load_cfg("configs/config_ref_tables_countries.json")
    # По сути валидация не обязательна
    cfg = validate_cfg(cfg)
    data = extract_data(cfg)
    df = pd.json_normalize(data)

    # Страны содержат столицы, агрегированные регионы нет
    df_c = df[df["capitalCity"] != ""]
    df_region = normalize_reference_from_key(df_c, "region")
    df_adminregion = normalize_reference_from_key(df_c, "adminregion")
    df_income_level= normalize_reference_from_key(df_c, "incomeLevel")
    df_lending_type = normalize_reference_from_key(df_c, "lendingType")
    df_country = normalize_reference_from_key(df_c, "country")
    # Однако можно делать запрос с использованием кода агрегированного региона и использовать
    # возможно данные использовать дальше
    df_r = df[df["capitalCity"] == ""]
    df_agr_region = normalize_reference_from_key(df_r[["id", "iso2Code", "name"]], "")

    conn = get_bd_connection(cfg)
    load_data(conn, "region", df_region)
    load_data(conn, "adminregion", df_adminregion)
    load_data(conn, "income_level", df_income_level)
    load_data(conn, "lending_type", df_lending_type)
    load_data(conn, "country", df_country)
    logger.info("Пайплайн - Создание/ обновление справочных таблиц для стран окончено")

def create_ref_tables_ind():
    setup_logging()
    cfg = load_cfg("configs/config_ref_tables_indicators.json")
    logger.info("ПЗапуск пайплайна для создания/обновления справочных таблиц для индикаторов")
    #-------------------Indicators-------------------
    # По сути валидация не обязательна
    cfg = validate_cfg(cfg)
    data = extract_data(cfg)
    df = pd.json_normalize(data)
    df_indicator = normalize_reference_from_key(df, "indicator")
    df_source = normalize_reference_from_key(df, "source")
    conn = get_bd_connection(cfg)
    load_data(conn, "source", df_source)
    load_data(conn, "indicator", df_indicator)
    logger.info("Пайплайн - Создание/ обновление справочных таблиц для индикаторов окончено")
