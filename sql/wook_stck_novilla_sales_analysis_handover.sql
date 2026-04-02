CREATE OR REPLACE TABLE wook.stck_novilla_sales_analysis
    OPTIONS (
        DESCRIPTION = "23년 이후 novilla sales 데이터 추출, seller type (1p,3p) 추가"
    )
AS
WITH cte_meta AS (
    SELECT DISTINCT
        a.asin,
        CAST(NULL AS STRING) AS zinus_sku,
        profile AS inch,
        size_adj AS size,
        a.category,
        2 AS ord
    FROM meta.amazon_mattress_master a
),
cte_event_day AS (
    SELECT
        UPPER(TRIM(asin)) AS asin,
        metric,
        DATE(event_ts_utc) AS event_date,
        value_num
    FROM tmp.stck_zns_comp_sales_anal_hist_events
    WHERE metric = 'list_price'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY UPPER(TRIM(asin)), metric, DATE(event_ts_utc)
        ORDER BY event_ts_utc DESC
    ) = 1
),
cte_list_price AS (
    SELECT
        asin,
        metric,
        event_date,
        LEAD(event_date) OVER (PARTITION BY asin, metric ORDER BY event_date) AS next_event_date,
        value_num
    FROM cte_event_day
),
cte_seller_src AS (
    SELECT
        UPPER(TRIM(asin)) AS asin,
        DATE(event_ts_utc) AS event_date,
        NULLIF(buyBoxSellerId, 'None') AS buyBoxSellerId,
        NULLIF(buyBoxIsAmazon, 'None') AS buyBoxIsAmazon
    FROM tmp.zns2706_novillla_seller
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY UPPER(TRIM(asin)), DATE(event_ts_utc)
        ORDER BY event_ts_utc DESC
    ) = 1
),
cte_seller AS (
    SELECT
        asin,
        event_date,
        LEAD(event_date) OVER (PARTITION BY asin ORDER BY event_date) AS next_event_date,
        buyBoxSellerId,
        buyBoxIsAmazon,
        IF(buyBoxSellerId = 'ATVPDKIKX0DER' OR buyBoxIsAmazon = 'True', '1p', '3p') AS seller_type
    FROM cte_seller_src
),
cte_with_meta AS (
    SELECT
        a.RetailerSku AS asin,
        seller.seller_type,
        FORMAT_DATE('%Y-%m', a.WeekEnding) AS yr_month,
        ANY_VALUE(b.zinus_sku) AS zinus_sku,
        MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]', '')) AS brand,
        SUM(a.RetailSales) AS sales,
        SUM(a.UnitsSold) AS units,
        AVG(RetailPrice) AS avg_retail_price,
        AVG(listprice.value_num) AS avg_list_price,
        ANY_VALUE(b.category) AS category,
        ANY_VALUE(b.size) AS size,
        ANY_VALUE(b.inch) AS inch,
        LOWER(REGEXP_REPLACE(
            NORMALIZE(ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)], NFKC),
            r'[^[:print:]]',
            ' '
        )) AS title
    FROM stck.atlas_sales_all a
    LEFT JOIN cte_meta b
      ON a.RetailerSku = b.asin
    LEFT JOIN cte_list_price listprice
      ON a.RetailerSku = listprice.asin
     AND a.WeekEnding BETWEEN listprice.event_date
                         AND COALESCE(DATE_SUB(listprice.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
    LEFT JOIN cte_seller seller
      ON a.RetailerSku = seller.asin
     AND a.WeekEnding BETWEEN seller.event_date
                         AND COALESCE(DATE_SUB(seller.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
    WHERE REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]', '') = 'NOVILLA'
      AND b.category IS NOT NULL
      AND a.WeekEnding >= '2023-01-01'
    GROUP BY 1, 2, 3
)
SELECT DISTINCT
    a.asin,
    a.seller_type,
    COALESCE(a.zinus_sku, b.model) AS sku,
    a.brand,
    yr_month,
    a.title,
    ROW_NUMBER() OVER (PARTITION BY category, yr_month ORDER BY sales DESC) AS yr_month_sales_rank,
    sales,
    units,
    avg_retail_price,
    avg_list_price,
    category,
    COALESCE(
        NULLIF(inch, 'OTHERS'),
        REGEXP_EXTRACT(LOWER(a.title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)'),
        IF(a.asin = 'B0D46LY1Y4', '10', NULL),
        IF(a.asin = 'B0F5X62491', '14', NULL),
        'OTHERS'
    ) AS inch,
    CASE
        WHEN a.asin IN ('B00X6L6DCO', 'B00X6LCL3O', 'B0C8S875C9', 'B0F5X62491') THEN 'Twin'
        WHEN a.asin = 'B0765C7YPX' THEN 'King'
        WHEN a.asin = 'B0D46LY1Y4' THEN 'Queen'
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
FROM cte_with_meta a
LEFT JOIN tmp.stck_zns_comp_sales_anal_mst b
  ON a.asin = b.asin
;
