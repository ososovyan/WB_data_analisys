/*DROP MATERIALIZED VIEW IF EXISTS mv_main_table_proc;

CREATE MATERIALIZED VIEW mv_main_table_proc AS
WITH template AS (
  SELECT m.country_id
    , c.name AS country_name
    , m.indicator_id
    , i.name AS indicator_name
    , CASE
      WHEN m.value = 'N/F' THEN NULL
      ELSE m.value::numeric
    END AS value
    , c.region_id 
    , r.region_value AS region_name
    , c.income_level_id 
    , il.income_level_value AS income_level_name
    , m.date::int AS year
    , MAKE_DATE(m.date::int, 1, 1) AS year_dt
    , (m.date::int / 10) * 10 AS decade
  
  FROM public.main_table AS m
  LEFT JOIN public.country AS c ON c.id = m.country_id
  LEFT JOIN public.indicator AS i ON i.id = m.indicator_id
  LEFT JOIN public.income_level AS il ON c.income_level_id = il.income_level_id
  LEFT JOIN public.region AS r ON c.region_id = r.region_id
), 
interpolation_preparation AS (
  -- В Posrgesql нет возможности использовать IGNORE NULLS чтобы пропускать строки null
  -- При этм четко нужно определять начало интервала и конец для интеполции, которые не null
  SELECT *
    , LAST_VALUE(CASE WHEN value IS NOT NULL THEN value END) OVER search_prev_notnull AS prev_value
    , LAST_VALUE(CASE WHEN value IS NOT NULL THEN year END) OVER search_prev_notnull AS prev_year
    , FIRST_VALUE(CASE WHEN value IS NOT NULL THEN value END) OVER search_next_notnull AS next_value
    , FIRST_VALUE(CASE WHEN value IS NOT NULL THEN year END) OVER search_next_notnull AS next_year
  FROM template
  WINDOW search_prev_notnull AS (
    PARTITION BY country_id, indicator_id
    ORDER BY year
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ),
  search_next_notnull AS (
    PARTITION BY country_id, indicator_id
    ORDER BY year
    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
  )
), 
template_interpolation AS (
  SELECT *
    , CASE
      WHEN value IS NOT NULL THEN value
      WHEN prev_value IS NOT NULL AND next_value IS NOT NULL AND next_year > prev_year
      THEN prev_value + (next_value - prev_value) * (year - prev_year) / (next_year - prev_year)
      ELSE NULL
    END AS value_filled
  FROM interpolation_preparation
),
calculation AS (
  -- ДЛя более глубокого анализа найдем доплнительные парметры
  -- измемнеие показателя (абсолютное и относительное) и волатильность на плече в пять лет
  SELECT *
    , value_filled - LAG(value_filled) OVER search_next AS del_val
    , (value_filled - LAG(value_filled) OVER search_next) / NULLIF(LAG(value_filled) OVER search_next, 0) AS prcnt_del
    , STDDEV(value) OVER (
      PARTITION BY country_id, indicator_id
      ORDER BY year
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
      ) AS vol_5
  FROM template_interpolation
  WINDOW search_next AS (
    PARTITION BY country_id, indicator_id
    ORDER BY year
  )
)
SELECT *
FROM calculation;

CREATE INDEX ON mv_main_table_proc (indicator_id);
CREATE INDEX ON mv_main_table_proc (country_id);
CREATE INDEX ON mv_main_table_proc (year);*/

CREATE OR REPLACE VIEW v_global AS
SELECT
    indicator_id
    , indicator_name
    , year
    , year_dt
    , AVG(value_filled) AS mean_value
    , STDDEV(value_filled) AS std_value
    , MIN(value_filled) AS min_value
    , MAX(value_filled) AS max_value
FROM mv_main_table_proc
GROUP BY indicator_id, indicator_name, year, year_dt;

CREATE OR REPLACE VIEW v_region AS
SELECT
    region_id
    , region_name
    , indicator_id
    , indicator_name
    , year
    , year_dt
    , AVG(value_filled) AS mean_value
    , STDDEV(value_filled) AS std_value
FROM mv_main_table_proc
GROUP BY region_id, region_name, indicator_id, indicator_name, year, year_dt;

CREATE OR REPLACE VIEW v_income_level AS
SELECT
    income_level_id
    , income_level_name
    , indicator_id
    , indicator_name
    , year
    , year_dt
    , AVG(value_filled) AS mean_value
    , STDDEV(value_filled) AS std_value
FROM mv_main_table_proc
GROUP BY income_level_id, income_level_name, indicator_id, indicator_name, year, year_dt;

CREATE OR REPLACE VIEW v_country_rank AS
SELECT country_id
    , country_name
    , indicator_id
    , indicator_name
    , year
    , value_filled
    , RANK() OVER (
        PARTITION BY indicator_id, year
        ORDER BY value_filled DESC
    ) AS rank_desc
    , RANK() OVER (
        PARTITION BY indicator_id, year
        ORDER BY value_filled ASC
    ) AS rank_asc
    , COUNT(*) OVER (
    PARTITION BY indicator_id, year
  ) AS countries_in_year
FROM mv_main_table_proc
WHERE value IS NOT NULL;
