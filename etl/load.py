import psycopg2
from psycopg2 import sql
from etl.logger import get_stage_logger

logger = get_stage_logger("load")

def get_bd_connection(cfg:dict):
    logger.info("Подключение к БД создано")
    return psycopg2.connect(
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=cfg["port"]
    )

def get_existing_tables(conn, schema = 'public') -> dict:
    '''
    Данная функция делает запрос к Postgress бд и возвращает словарь
    ключи которого названия таблиц - значения названия pk столбцов
    эта функция нужная при создании автоматических связей столбцов вновь созданной таблицы
    '''

    query = """
           SELECT 
               t.table_name,
               kcu.column_name AS primary_key
           FROM information_schema.tables t
           JOIN information_schema.table_constraints tc 
               ON t.table_name = tc.table_name AND tc.constraint_type = 'PRIMARY KEY'
           JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name
           WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE';
       """

    existing_tables = {}
    with conn.cursor() as cur:
        cur.execute(query, [schema])
        for table_name, pk_column in cur.fetchall():
            existing_tables[table_name] = pk_column
    logger.info("В данный момент в нашей схеме существуют след таблицы и их PK")
    return existing_tables

def create_table(conn, table_name, df, schema = 'public'):

    '''
    Данная функция в первую очередь создает таблицы новые с заданным названием (если такая существует то не создает)
    Процесс автоматизированный на вход получает pd.df будущее название таблицы, название схемы.
    Из pd.df мы получаем названия столбцов таблицы, а также тип данных
    По кусочкам мы собираем sql запрос
    Также предусмотрено автоматическое создание pk и fk
    Если мы встречаем столбец с названием id это pk, если таких не встретили, создаем сами новый столбец
    который и будет новым pk
    Также мы пытаемся найти связи с другими таблицами, предположение если есть столбец формаата.
    {table_name}_id - то есть таблица table_name pk, которой связан с нашим столбцом для нахождения актуаальных
    справочных таблиц пользуемся функцией get_existing_tables
    ***Также мможет сразу сделаем индексами все fk для оптимизации работы дальнейших обращений к нашей бд
    '''
    logger.info(f"Создаем таблицу {table_name}")
    existing_tables = get_existing_tables(conn, schema)
    columns = []
    primary_keys = []
    foreign_keys = []

    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            pg_type = "INTEGER"
        elif "float" in str(dtype):
            pg_type = "NUMERIC"
        elif "bool" in str(dtype):
            pg_type = "BOOLEAN"
        else:
            pg_type = "TEXT"

        if col == 'id' or col == f"{table_name}_id":
            primary_keys.append(col)

        for ref_table, ref_pk in existing_tables.items():

            if col.lower() == f"{ref_table.lower()}_id":
                foreign_keys.append(
                    sql.SQL("FOREIGN KEY ({col}) REFERENCES {ref_table}({ref_pk})").format(
                        col=sql.Identifier(col),
                        ref_table=sql.Identifier(ref_table),
                        ref_pk=sql.Identifier(ref_pk)
                    )
                )

        columns.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type)))

    logger.info(f"Найдено {len(primary_keys)}, {len(foreign_keys)}")
    if not primary_keys:
        columns.insert(0, sql.SQL("id SERIAL PRIMARY KEY"))
        pk_sql = sql.SQL("")
        logger.info(f"Создаем PRIMARY KEY")
    else:
        pk_sql = sql.SQL(", PRIMARY KEY ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, primary_keys))
        )

    fk_sql = sql.SQL(", ").join(foreign_keys)

    query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ( {}{}{} );").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns),
        pk_sql,
        sql.SQL(", ") + fk_sql if foreign_keys else sql.SQL("")
    )
    #logger.info(f"Итоговый Sql запрос {query}")
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()
        logger.info(f"Таблица {table_name} есть - создана, или уже существовала")

def load_data(conn, table_name, df, schema = 'public'):
    '''
    Функция загружает в Postgress БД таблицу с названием {table_name} из pd.df
    используем множественную вставку executemany()
    Предварительно с помощью метода get_existing_tables() проверяем есь ли такая таблица в бд
    есмли есть то мы ее не создаем

    '''
    logger.info(f"Выполняем заполнение таблицы {table_name} ")
    columns = list(df.columns)
    values = list(df.values)
    existing_tables = get_existing_tables(conn, schema)
    if not existing_tables.get(table_name, []):
        logger.info(f"Таблицы {table_name} не существует")
        create_table(conn, table_name, df)

    existing_tables = get_existing_tables(conn, schema)
    pk = existing_tables[table_name]
    fields = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)

    update_assignments = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))

        for col in columns
        if col != pk
    )

    insert_query = sql.SQL("""
    INSERT INTO {schema}.{table} ({fields})
    VALUES ({placeholders})
    ON CONFLICT ({pk}) 
    DO UPDATE SET {updates};
    """).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table_name),
        placeholders=placeholders,
        fields=fields,
        pk=sql.Identifier(pk),
        updates=update_assignments
    )
    with conn.cursor() as cur:
        cur.executemany(insert_query, values)
        conn.commit()
        logger.info(f"Таблица {table_name}успешно заполнена")

# def create_table(conn ,cfg:dict, key:str = None):
#
#
#     cur = conn.cursor()
#
#     if key == 'countries':
#         cur.execute(f"""
#             CREATE TABLE IF NOT EXISTS {key} (
#                 id VARCHAR PRIMARY KEY
#                 , iso2_code VARCHAR
#                 , name TEXT
#                 , capital_city TEXT
#                 , longitude FLOAT
#                 , latitude FLOAT
#                 , region_id VARCHAR REFERENCES regions(id)
#                 , adminregion_id VARCHAR REFERENCES admin_regions(id)
#                 , income_level_id VARCHAR REFERENCES income_levels(id)
#                 , lending_type_id VARCHAR REFERENCES lending_types(id)
#
#             );
#         """)
#     elif key == 'indicators':
#         pass
#     elif key is None:
#         pass
#     else:
#         cur.execute(f"""
#             CREATE TABLE IF NOT EXISTS {key} (
#                 id VARCHAR PRIMARY KEY
#                 , iso2_code VARCHAR
#                 , value TEXT
#
#             );
#         """)
#
#     conn.commit()
#     cur.close()

