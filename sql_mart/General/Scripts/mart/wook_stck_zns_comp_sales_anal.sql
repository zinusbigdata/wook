/*
 * ZNS-2837 : 아마존 Top4 경쟁사 ASP 분석 마트
 */

-- [ 01. Top 경쟁사 Asin 속성 마스터] --------------------------------------------------------
CREATE OR REPLACE TABLE tmp.stck_zns_comp_sales_anal_mst AS
SELECT
    *
FROM
    tmp.stck_zns_comp_sales_anal_mst
QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY load_datetime desc) = 1;


-- [ 02. 마트 생성하기] --------------------------------------------------------------------

CREATE OR REPLACE TABLE wook.stck_zns_comp_sales_anal
    OPTIONS (
        DESCRIPTION = "egohome, novilla, fdw, zinus 25년 이후 sales, price 분석용 테이블"
    )
AS
WITH cte_meta         AS (
    SELECT
        *
    FROM
        (
            SELECT DISTINCT
                TRIM(a.asin) AS asin
                , TRIM(a.zinus_sku) AS zinus_sku
                , IF(a.asin = 'B0B6FQZMJ4', '5', REGEXP_EXTRACT(LOWER(TRIM(inch_color)), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')) AS inch
                , TRIM(size) AS size
                , if(a.financial_category = 'Foam Mattresses', 'Foam Mattress', a.financial_category) as category
                , 1 AS ord
            FROM
                meta.amz_zinus_master_pdt_pi_add_new_col a
            WHERE
                a.financial_category in ('Foam Mattresses', 'Spring Mattresses')

            UNION ALL

            SELECT DISTINCT
                a.asin
                , cast(null as string) AS zinus_sku
                , profile as inch
                , size_adj as size
                , a.category
                , 2 AS ord
            FROM
                meta.amazon_mattress_master a  -- 경쟁사 

        )
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY asin ORDER BY category is null, ord) = 1
)
, cte_event_day AS (
        SELECT
            UPPER(TRIM(asin)) AS asin
            , metric
            , DATE(event_ts_utc) AS event_date
            , value_num
        FROM tmp.stck_zns_comp_sales_anal_hist_events
        WHERE
            metric = 'list_price'

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(asin)), metric, DATE(event_ts_utc)
            ORDER BY event_ts_utc DESC
        ) = 1
    )
, cte_list_price AS (
    SELECT
        asin
        , metric
        , event_date
        , LEAD(event_date) OVER (
            PARTITION BY asin, metric
            ORDER BY event_date
            ) AS next_event_date
        , value_num
    FROM cte_event_day
)
, cte_with_meta as (
        SELECT
            a.RetailerSku AS asin
            , FORMAT_DATE('%Y-%m', a.WeekEnding) as yr_month

            , ANY_VALUE(b.zinus_sku) AS zinus_sku

            , MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')) as brand

            , SUM(a.RetailSales) AS sales
            , SUM(a.UnitsSold) AS units
            , AVG(RetailPrice) AS avg_retail_price
            , AVG(listprice.value_num) AS avg_list_price

            , ANY_VALUE(b.category) AS category
            , ANY_VALUE(b.size) AS size
            , ANY_VALUE(b.inch) AS inch

            , LOWER(
                    REGEXP_REPLACE(
                            NORMALIZE(ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)],
                                      NFKC),
                            r'[^[:print:]]',
                            ' '
                    )
              ) AS title
        FROM
            stck.atlas_sales_all a

                JOIN cte_meta b
                    ON a.RetailerSku = b.asin
                left join cte_list_price listprice
                    ON a.RetailerSku = listprice.asin
                        AND a.WeekEnding BETWEEN listprice.event_date AND COALESCE(DATE_SUB(listprice.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
        WHERE
            REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') IN ( 'FDW', 'EGOHOME', 'NOVILLA', 'ZINUS' )
            AND b.category IS NOT NULL

            AND a.WeekEnding >= '2022-01-01'
        GROUP BY 1, 2

    )
SELECT DISTINCT
    a.asin
    , COALESCE(a.zinus_sku, b.model) as sku
    , a.brand
    , yr_month

    , CONCAT(
            SUBSTR(yr_month, 1, 4),
            '-Q',
            CAST(DIV(CAST(SUBSTR(yr_month, 6, 2) AS INT64) - 1, 3) + 1 AS STRING)
      ) AS yr_quarter

    , a.title

    , ROW_NUMBER() OVER (PARTITION BY category, yr_month ORDER BY sales DESC) as yr_month_sales_rank
    , sales
    , units
    , avg_retail_price
    , avg_list_price
    , category
    , COALESCE(nullif(inch, 'OTHERS'), REGEXP_EXTRACT(LOWER(a.title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)'), 'OTHERS' ) as inch

    ,     CASE
              WHEN a.asin in ('B00X6L6DCO', 'B00X6LCL3O') THEN 'Twin'
              WHEN a.asin = 'B0765C7YPX' THEN 'King'
              WHEN LOWER(a.size) IN ('ck', 'california king') THEN 'Cal King'
              WHEN LOWER(a.size) = '12 inch queen medium firm' THEN 'Queen'
              WHEN LOWER(a.size) IN ('k', 'king (u.s. standard)', 'king') THEN 'King'
              WHEN LOWER(a.size) IN ('t', 'twin', 'twin (75*38)') THEN 'Twin'
              WHEN LOWER(a.size) IN ('f', 'full', 'full (75*54)') THEN 'Full'
              WHEN LOWER(a.size) IN ('s', 'single') THEN 'Single'
              WHEN LOWER(a.size) IN ('txl', 'twin-xl', 'twin xl') THEN 'Twin XL'
              WHEN LOWER(a.size) = 'sq' THEN 'Short Queen'
              WHEN LOWER(a.size) IN ('q', 'queen (u.s. standard)', 'queen') THEN 'Queen'
              WHEN LOWER(a.size) = 'nt' THEN 'Narrow Twin'
              WHEN REGEXP_CONTAINS(a.title, r'\b(cal(?:ifornia)?\s*king|cal\s*king)\b') THEN 'Cal King'
              WHEN REGEXP_CONTAINS(a.title, r'\bshort\s*queen\b') THEN 'Short Queen'
              WHEN REGEXP_CONTAINS(a.title, r'\bnarrow\s*twin\b') THEN 'Narrow Twin'
              WHEN REGEXP_CONTAINS(a.title, r'\btwin[\s-]*xl\b') THEN 'Twin XL'
              WHEN REGEXP_CONTAINS(a.title, r'\bqueen\b|\bqeen\b') THEN 'Queen'
              WHEN REGEXP_CONTAINS(a.title, r'\bking\b') THEN 'King'
              WHEN REGEXP_CONTAINS(a.title, r'\bfull\b') THEN 'Full'
              WHEN REGEXP_CONTAINS(a.title, r'\btwin\b') THEN 'Twin'
              WHEN REGEXP_CONTAINS(a.title, r'\bsingle\b') THEN 'Single'
              ELSE a.size
          END AS size
FROM
    cte_with_meta a
         left join tmp.stck_zns_comp_sales_anal_mst b
            on a.asin = b.asin
;
