/*
    ZNS-3036: Stackline의 Tentpole 분석에 대해 Peter의 후속 요청 건
 */

--- [01. ZINUS 점유율 계산하기 ] --------------------------------------------------
WITH base AS (
  SELECT EXTRACT(YEAR FROM WeekEnding)                      AS yr
       , EXTRACT(QUARTER FROM WeekEnding)                   AS qtr
       , EXTRACT(MONTH FROM WeekEnding)                     AS mo
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')  AS brand
       , ROUND(SUM(RetailSales), 0)                         AS sales
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
        AND WeekEnding BETWEEN '2024-01-01' and '2026-06-30'
  GROUP BY 1, 2, 3, 4
)

-- ===== Monthly =====
, monthly AS (
  SELECT yr, mo, brand, SUM(sales) AS sales
  FROM base
  GROUP BY 1, 2, 3
)
, monthly_ranked AS (
  SELECT 'MONTHLY'   AS period_type
       , yr
       , mo          AS period
       , brand
       , sales
       , SUM(sales) OVER (PARTITION BY yr, mo) AS total_sales
  FROM monthly
)
, monthly_final AS (
  SELECT period_type, yr, period, brand, sales, total_sales
       , ROUND(SAFE_DIVIDE(sales, total_sales) * 100, 2) AS zinus_share_pct
  FROM monthly_ranked
  WHERE brand = 'ZINUS'
)
--SELECT * FROM monthly_final

-- ===== Quarterly =====
, quarterly AS (
  SELECT yr, qtr, brand, SUM(sales) AS sales
  FROM base
  GROUP BY 1, 2, 3
)
, quarterly_ranked AS (
  SELECT 'QUARTERLY' AS period_type
       , yr
       , qtr         AS period
       , brand
       , sales
       , SUM(sales) OVER (PARTITION BY yr, qtr) AS total_sales
  FROM quarterly
)
, quarterly_final AS (
  SELECT period_type, yr, period, brand, sales, total_sales
       , ROUND(SAFE_DIVIDE(sales, total_sales) * 100, 2) AS zinus_share_pct
  FROM quarterly_ranked
  WHERE brand = 'ZINUS'
)

SELECT * FROM monthly_final
UNION ALL
SELECT * FROM quarterly_final
ORDER BY period_type, yr, period
;

SELECT DISTINCT WeekEnding
FROM stck.atlas_sales_all
where WeekEnding >= '2026-01-01'
ORDER BY 1 DESC
;



--- [02. EGOHOME 매출 볼륨 - 2026.6.21-27(Amazon Prime Day) ] --------------------------------------------------

WITH base AS (
  SELECT WeekEnding
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')  AS brand
       , SUM(RetailSales)                                   AS sales
       , SUM(UnitsSold)                                     AS units
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
        AND WeekEnding IN ('2026-06-27', '2025-07-12')
  GROUP BY 1, 2
)
, ranked AS (
  SELECT WeekEnding, brand, sales, units
       , SUM(sales) OVER (PARTITION BY WeekEnding) AS total_sales
  FROM base
)
, final AS (
  SELECT WeekEnding, brand
       , ROUND(sales, 0)                                     AS sales
       , units
       , ROUND(SAFE_DIVIDE(sales, units), 2)                 AS asp
       , ROUND(total_sales, 0)                                AS total_sales
       , ROUND(SAFE_DIVIDE(sales, total_sales) * 100, 2)      AS share_pct
  FROM ranked
  WHERE brand IN ('EGOHOME', 'ZINUS')
)
SELECT * FROM final
ORDER BY WeekEnding DESC, brand
;
SELECT cur.WeekEnding
     , cur.brand
     , cur.sales
     , cur.units
     , cur.asp
     , cur.share_pct
     , py.WeekEnding                                         AS WeekEnding_ly
     , py.sales                                               AS sales_ly
     , py.units                                               AS units_ly
     , py.asp                                                 AS asp_ly
     , py.share_pct                                          AS share_pct_ly
     , ROUND(cur.share_pct - py.share_pct, 2)                 AS share_pct_yoy_diff
FROM final cur
LEFT JOIN final py
  ON cur.brand = py.brand
 AND py.WeekEnding = '2025-07-12'
WHERE cur.WeekEnding = '2026-06-27'
ORDER BY cur.brand
;


--- [021. ZINUS와 EGOHOME 비교 ] -------------------------------------------------

WITH weekly_brand AS (
  SELECT WeekEnding
       , EXTRACT(ISOWEEK FROM WeekEnding) AS wk
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')  AS brand
       , ROUND(SUM(RetailSales), 0)                         AS sales
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
        AND WeekEnding BETWEEN '2026-01-01' and '2026-06-30'
  GROUP BY 1,2,3
)
, weekly_ranked AS (
    SELECT *
        , SUM(sales) OVER (PARTITION BY wk) AS total_sales
    FROM weekly_brand
)
SELECT *
   , ROUND(SAFE_DIVIDE(sales, total_sales) * 100, 2) AS share_pct
FROM weekly_ranked
WHERE brand in ('EGOHOME', 'ZINUS')
ORDER BY wk desc, brand;
;

--- [022. 좀더 분석해 보기 : YoY 비교 ] -------------------------------------------------

WITH base AS (
  SELECT WeekEnding
       , EXTRACT(YEAR FROM WeekEnding)                      AS yr
       , EXTRACT(ISOWEEK FROM WeekEnding)                   AS wk
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')  AS brand
       , SUM(RetailSales)                                   AS sales
       , SUM(UnitsSold)                                     AS units
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
        AND WeekEnding BETWEEN '2025-01-01' AND '2026-06-30'
  GROUP BY 1, 2, 3, 4
)
, ranked AS (
  SELECT WeekEnding, yr, wk, brand, sales, units
       , SUM(sales) OVER (PARTITION BY yr, wk) AS total_sales
  FROM base
)
, final AS (
  SELECT WeekEnding, yr, wk, brand
       , ROUND(sales, 0)                                     AS sales
       , units
       , ROUND(SAFE_DIVIDE(sales, units), 2)                 AS asp
       , ROUND(total_sales, 0)                                AS total_sales
       , ROUND(SAFE_DIVIDE(sales, total_sales) * 100, 2)      AS share_pct
  FROM ranked
  WHERE brand IN ('EGOHOME', 'ZINUS')
)
SELECT cur.WeekEnding
     , cur.yr
     , cur.wk
     , cur.brand
     , cur.sales
     , cur.units
     , cur.asp
     , cur.share_pct
     , py.WeekEnding                                         AS WeekEnding_ly
     , py.sales                                               AS sales_ly
     , py.units                                               AS units_ly
     , py.asp                                                 AS asp_ly
     , py.share_pct                                          AS share_pct_ly
     , ROUND(cur.share_pct - py.share_pct, 2)                 AS share_pct_yoy_diff
FROM final cur
LEFT JOIN final py
  ON cur.wk    = py.wk
 AND cur.brand = py.brand
 AND cur.yr    = py.yr + 1
WHERE cur.yr = 2026
ORDER BY cur.wk DESC, cur.brand
;
