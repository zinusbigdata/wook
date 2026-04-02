-- CREATE OR REPLACE TABLE tmp.stck_zinus_sales_analysis AS
CREATE OR REPLACE TABLE wook.stck_zinus_sales_analysis
    OPTIONS (
        DESCRIPTION = "23년 이후 zinus mattress asin 별 sales"
        )
AS
WITH cte_meta         AS (
        SELECT
            *
        FROM
            (
                -- pi Master
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

                -- GPT Mattress Master
                SELECT DISTINCT
                    a.asin
                    , cast(null as string) AS zinus_sku
                    , profile as inch
                    , size_adj as size
                    , a.category
                    , 2 AS ord
                FROM
                    meta.amazon_mattress_master a

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
            --     , (ARRAY_AGG(a.Brand)) as brand
            , MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')) as brand

            , SUM(a.RetailSales) AS sales
            , SUM(a.UnitsSold) AS units
            , AVG(RetailPrice) AS avg_retail_price
            , AVG(listprice.value_num) AS avg_list_price

            , ANY_VALUE(b.category) AS category
            , ANY_VALUE(b.size) AS size
            , ANY_VALUE(b.inch) AS inch
            --     , ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)] AS title
            , LOWER(
                    REGEXP_REPLACE(
                            NORMALIZE(ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)],
                                      NFKC), -- NBSP 등 정규화
                            r'[^[:print:]]',
                            ' '
                    )
              ) AS title
        FROM
            stck.atlas_sales_all a
                LEFT JOIN cte_meta b
                    ON a.RetailerSku = b.asin
                left join cte_list_price listprice
                    ON a.RetailerSku = listprice.asin
                AND a.WeekEnding BETWEEN listprice.event_date AND COALESCE(DATE_SUB(listprice.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
        WHERE
            REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') = 'ZINUS'
            AND b.category IS NOT NULL
            AND a.WeekEnding >= '2023-01-01'
        GROUP BY 1, 2
        -- HAVING ARRAY_LENGTH((ARRAY_AGG(a.Brand))) = 1
    )
SELECT DISTINCT
    a.asin
    , COALESCE(a.zinus_sku, b.model) as sku
    , a.brand
    , yr_month
    , a.title
    , ROW_NUMBER() OVER (PARTITION BY category, yr_month ORDER BY sales DESC) as yr_month_sales_rank
    , sales
    , units
    , avg_retail_price
    , avg_list_price
    , category
    , COALESCE(
            NULLIF(inch, 'OTHERS')
        , REGEXP_EXTRACT(LOWER(a.title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')
        , if(a.asin = 'B0D46LY1Y4', '10', null)
        , if(a.asin = 'B0F5X62491', '14', null)
        , 'OTHERS'
      ) as inch
    --     , size
    ,     CASE
              WHEN a.asin in ('B00X6L6DCO', 'B00X6LCL3O', 'B0C8S875C9', 'B0F5X62491') THEN 'Twin'
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


-- stackline base - 단일 asin (B0CKYZC93L) 검증 -----------------------------------------------------------------------------------------
with cte as (
    SELECT
        *
        , IF(WeekEnding BETWEEN '2024-09-01' AND '2025-02-28', 'Y', 'N') AS is_pre
        , IF(WeekEnding BETWEEN '2025-09-01' AND '2026-02-28', 'Y', 'N') AS is_cur
    FROM
        stck.atlas_sales_all
    WHERE
        RetailerSku = 'B0CKYZC93L'
        AND WeekEnding >= '2024-09-01'
)
SELECT
    sum(if(is_pre='Y',RetailSales, 0))
    , sum(if(is_cur='Y',RetailSales, 0))
FROM
    cte
;
-- f0_,f1_
-- 4994433.180000001
-- 8040122.5200000005


select DISTINCT yr_month from wook.stck_zinus_sales_analysis;

-- 23년 이후 stackline data 에 inch, size 마스터 보완한 table base - 최종 검증 쿼리 ------------------------------------------------------------------------------------------------------
WITH
    cte AS (
        SELECT
            *
            , IF(yr_month BETWEEN '2024-09' AND '2025-02', 'Y', 'N') AS is_pre
            , IF(yr_month BETWEEN '2025-09' AND '2026-02', 'Y', 'N') AS is_cur
        FROM
            wook.stck_zinus_sales_analysis
        WHERE
            brand = 'ZINUS'
            AND inch = '12'
            AND size = 'Queen'
        -- ORDER BY
        --     yr_month
    )
    , cte_grp as (
        SELECT
            asin
            , inch
            , size
            , title
            , CAST(SUM(IF(is_pre = 'Y', sales, 0)) AS NUMERIC) AS before_sales
            , CAST(SUM(IF(is_cur = 'Y', sales, 0)) AS NUMERIC) AS after_sales
        FROM
            cte
        GROUP BY 1, 2, 3, 4
    )
SELECT
    *
    , after_sales - before_sales AS delta
FROM
    cte_grp
;