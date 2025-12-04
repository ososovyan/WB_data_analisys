import re
import pandas as pd
from etl.logger import get_stage_logger

def transform(data: list) -> pd.DataFrame:
    df = pd.json_normalize(data)
    df.info()
    df = normalize_reference_from_key(df)
    return df

def to_snake(name: str) -> str:
    name = name.replace(".", "_")
    name = re.sub(r'([0-9])([a-zA-Z])', r'\1_\2', name)
    #name = re.sub(r'([a-zA-Z])([0-9])', r'\1_\2', name)  # буква→цифра
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name)
    name = name.lower()
    name = re.sub(r'_+', '_', name)

    return name


def process_columns_for_special_keys(columns: list, key: str) -> list:
    new_column_list = []
    for col in columns:
        if "." not in col or col.endswith(".id"):
            new_column_list.append(col)
    # Костыль убираем топики все
    try:
        new_column_list.remove("topics")
    except ValueError:
        pass
    return new_column_list


def normalize_reference_from_key(df: pd.DataFrame, key: str = None) -> pd.DataFrame:
    """
    Универсальный обработчик справочных таблиц для WorldBank.
    Key нужен для создания справочных таблиц
    Сохраняет key в названии, заменяет точку на '_', конвертирует camelCase → snake_case.
    """
    special_keys = ["country", "indicator"]
    if key in special_keys:
        cols = process_columns_for_special_keys(df.columns.tolist(), key)
    elif key is None:
        #Хардкод того что при в запросе country_id не iso3 a iso2
        df.rename(columns={
            "countryiso3code": "country.id",
            "country.id": "country.iso2_code"
        }, inplace=True)
        cols = ["country.id", "indicator.id", "date", "value", "obs_status", "unit"]
    else:
        cols = [col for col in df.columns if col.startswith(f"{key}.")]
    if not cols:
        raise ValueError(f"Колонки с ключом '{key}' не найдены.")

    ref_df = df[cols].drop_duplicates().copy().reset_index(drop=True)
    new_columns = {col: to_snake(col) for col in cols}
    ref_df.rename(columns=new_columns, inplace=True)
    ref_df = ref_df.mask(ref_df.isna() | (ref_df == ""), "N/F")
    return ref_df