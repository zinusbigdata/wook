/*
 * ZNS-2940 : Amazon에서 Walmart로 이동한 Brand들을 추적하기
 */

--[ 전 brand 활동 시퀀스 + ASIN 수 + 총매출 + 소멸 연도 ]
CREATE OR REPLACE TABLE wook.tmp_amz_disappeared_brands AS
WITH brand_year AS (
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0) AS sales
       , COUNT(DISTINCT RetailerSku)       AS asin_cnt
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
  GROUP BY 1, 2
)
--SELECT count(DISTINCT brand) FROM brand_year
, max_yr AS (   -- 데이터 전체의 최신 연도 (진행 중 연도 판별용)
  SELECT MAX(yr) AS data_max_yr FROM brand_year
)
, yr_seq AS (
  SELECT brand
       , STRING_AGG(CAST(yr AS STRING), ',' ORDER BY yr) AS active_years_list
       , MIN(yr) AS first_yr
       , MAX(yr) AS last_yr
       , COUNT(DISTINCT yr)         AS active_years_cnt
       , SUM(sales)                 AS total_sales
       , SUM(asin_cnt)              AS total_asin_cnt   -- 연도별 ASIN 수 합 (활동량 지표)
  FROM brand_year
  GROUP BY brand
)
SELECT s.brand
     , s.active_years_list
     , s.first_yr
     , s.last_yr
     , (s.last_yr - s.first_yr + 1)                       AS span_years
     , s.active_years_cnt
     , (s.last_yr - s.first_yr + 1) - s.active_years_cnt  AS missing_years
     , s.total_asin_cnt
     , s.total_sales
     -- 마지막 활동 연도 기준 소멸 연도: 마지막 활동이 진행중(최신) 연도면 아직 살아있음 → NULL
     , CASE WHEN s.last_yr < m.data_max_yr
            THEN s.last_yr + 1
            ELSE NULL
       END                                                AS disappeared_yr
FROM yr_seq s
CROSS JOIN max_yr m
ORDER BY missing_years DESC, total_sales DESC
;


--[ Amazon 소멸 brand의 Walmart 이동 분석 : v2 ]
WITH wmt_brand_year AS (   -- Walmart: 연도별 brand 매출/ASIN
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0)  AS sales
       , COUNT(DISTINCT RetailerSku) AS asin_cnt
  FROM stck.wmt_atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
  GROUP BY 1, 2
)
, wmt_seq AS (   -- Walmart: brand별 활동 시퀀스 요약
  SELECT brand
       , STRING_AGG(CAST(yr AS STRING), ',' ORDER BY yr) AS wmt_active_years_list
       , MIN(yr) AS wmt_first_yr
       , MAX(yr) AS wmt_last_yr
       , SUM(sales)    AS wmt_total_sales
       , SUM(asin_cnt) AS wmt_total_asin_cnt
  FROM wmt_brand_year
  GROUP BY brand
)
SELECT
    a.brand
  , a.disappeared_yr                       -- Amazon 소멸 연도 (NULL이면 아직 활동중)
  , a.last_yr        AS amz_last_yr         -- Amazon 마지막 활동 연도
  , a.total_sales    AS amz_total_sales
  , a.total_asin_cnt AS amz_total_asin_cnt
  -- Walmart 측 요약
  , w.wmt_active_years_list
  , w.wmt_first_yr
  , w.wmt_last_yr
  , w.wmt_total_sales
  , w.wmt_total_asin_cnt
  -- Amazon 소멸 시점(기준연도) 전후 Walmart 매출 분리 집계
  , wb.wmt_sales_before                     -- 기준연도 이전 Walmart 매출
  , wb.wmt_sales_after                      -- 기준연도 이후(당해 포함) Walmart 매출
  -- 이동 판정
  , CASE
      WHEN w.brand IS NULL                              THEN 'AMZ_ONLY'      			-- 월마트에 없음
      WHEN a.disappeared_yr IS NULL                     THEN 'ACTIVE_BOTH_CHANNELS'   	-- 아마존 활동중 + 월마트 병존
      WHEN IFNULL(wb.wmt_sales_after, 0)  > 0
       AND IFNULL(wb.wmt_sales_before, 0) = 0           THEN 'MIGRATED_TO_WMT'        	-- 소멸 후 Walmart 신규 등장
      WHEN IFNULL(wb.wmt_sales_after, 0)  > 0           THEN 'AMZ_GONE_WMT_SURVIVES'   	-- 아마존 소멸 + 월마트 생존
      ELSE 'GONE_BOTH_CHANNELS'                                                     	-- 아마존 소멸 + 월마트 소멸
    END AS migration_status
  -- 이동 강도: 소멸후 Walmart / 소멸전 Walmart (참고용)
  , SAFE_DIVIDE(wb.wmt_sales_after, NULLIF(wb.wmt_sales_before, 0)) AS wmt_after_before_ratio
FROM wook.tmp_amz_disappeared_brands a
LEFT JOIN wmt_seq w
  ON w.brand = a.brand
-- 기준연도(disappeared_yr) 전후 Walmart 매출 분리
LEFT JOIN (
  SELECT y.brand
       , SUM(CASE WHEN y.yr <  d.disappeared_yr THEN y.sales ELSE 0 END) AS wmt_sales_before
       , SUM(CASE WHEN y.yr >= d.disappeared_yr THEN y.sales ELSE 0 END) AS wmt_sales_after
  FROM wmt_brand_year y
  JOIN wook.tmp_amz_disappeared_brands d
    ON d.brand = y.brand
   AND d.disappeared_yr IS NOT NULL          -- 소멸 연도가 있는 brand만 전후 분리 의미 있음
  GROUP BY y.brand
) wb
  ON wb.brand = a.brand
ORDER BY
    (w.brand IS NULL)                        -- Walmart 있는 brand 먼저
  , a.disappeared_yr
  , a.total_sales DESC
;



-- [ Walmart ]
WITH wmt_brand_yr AS (
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0)  AS sales
       , COUNT(DISTINCT RetailerSku) AS asin_cnt
  FROM stck.wmt_atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
  GROUP BY 1, 2
)
SELECT DISTINCT brand
FROM wmt_brand_yr 
  
  

-- 이전 버전들 ---


--[ 사라졌다가 다시 들어오는 brand들 ]
WITH brand_year AS (
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0) AS sales
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
  GROUP BY 1, 2
)
, yr_seq AS (
  SELECT brand
       , STRING_AGG(CAST(yr AS STRING), ',' ORDER BY yr) AS active_years_list
       , MIN(yr) AS first_yr
       , MAX(yr) AS last_yr
       , COUNT(DISTINCT yr) AS active_years_cnt
  FROM brand_year
  GROUP BY brand
)
-- gap이 있으면(연속이어야 할 연도 수 > 실제 활동 연도 수) 사라졌다 재등장한 brand
SELECT brand
     , active_years_list
     , first_yr
     , last_yr
     , (last_yr - first_yr + 1) AS span_years
     , active_years_cnt
     , (last_yr - first_yr + 1) - active_years_cnt AS missing_years
FROM yr_seq
WHERE (last_yr - first_yr + 1) > active_years_cnt   -- 시퀀스에 공백 존재
ORDER BY missing_years DESC, brand
;


-- [ Amazon에서 Walmart로 이동한 Brand 분석 ]
WITH amz_brand_year AS (   -- Amazon: 연도별 brand 매출
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0) AS sales
       , SUM(UnitsSold) AS units
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses'
    AND RetailSales > 0
  GROUP BY 1, 2
)
, wmt_brand_year AS (   -- Walmart: 연도별 brand 매출
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0) AS sales
       , SUM(UnitsSold) AS units
  FROM stck.wmt_atlas_sales_all
  WHERE SubCategory = 'Mattresses'
    AND RetailSales > 0
  GROUP BY 1, 2
)
-- ── Amazon에서 연도별 소멸 브랜드 식별 ──
, lost_all AS (
  SELECT brand, 2021 AS gone_since FROM (
    SELECT brand FROM amz_brand_year WHERE yr <= 2020
    EXCEPT DISTINCT SELECT brand FROM amz_brand_year WHERE yr >= 2021)
  UNION ALL
  SELECT brand, 2022 FROM (
    SELECT brand FROM amz_brand_year WHERE yr <= 2021
    EXCEPT DISTINCT SELECT brand FROM amz_brand_year WHERE yr >= 2022)
  UNION ALL
  SELECT brand, 2023 FROM (
    SELECT brand FROM amz_brand_year WHERE yr <= 2022
    EXCEPT DISTINCT SELECT brand FROM amz_brand_year WHERE yr >= 2023)
  UNION ALL
  SELECT brand, 2024 FROM (
    SELECT brand FROM amz_brand_year WHERE yr <= 2023
    EXCEPT DISTINCT SELECT brand FROM amz_brand_year WHERE yr >= 2024)
  UNION ALL
  SELECT brand, 2025 FROM (
    SELECT brand FROM amz_brand_year WHERE yr <= 2024
    EXCEPT DISTINCT SELECT brand FROM amz_brand_year WHERE yr >= 2025)
  UNION ALL
  SELECT brand, 2026 FROM (
    SELECT brand FROM amz_brand_year WHERE yr <= 2025
    EXCEPT DISTINCT SELECT brand FROM amz_brand_year WHERE yr >= 2026)
)
, lost_first AS (   -- Amazon 최초 소멸 연도
  SELECT brand, MIN(gone_since) AS amz_gone_since
  FROM lost_all
  GROUP BY brand
)
-- ── Amazon 소멸 직전 실적 ──
, amz_before AS (
  SELECT l.brand
       , l.amz_gone_since
       , SUM(a.sales)  AS amz_sales_before_gone
       , MAX(a.yr)     AS amz_last_active_year
  FROM lost_first l
  JOIN amz_brand_year a
    ON a.brand = l.brand
   AND a.yr < l.amz_gone_since
  GROUP BY l.brand, l.amz_gone_since
)
-- ── Walmart 측 집계: 소멸 이전 vs 소멸 이후 ──
, wmt_agg AS (
  SELECT b.brand
       , SUM(CASE WHEN w.yr <  b.amz_gone_since THEN w.sales ELSE 0 END) AS wmt_sales_before
       , SUM(CASE WHEN w.yr >= b.amz_gone_since THEN w.sales ELSE 0 END) AS wmt_sales_after
       , MAX(w.yr)                                                       AS wmt_last_active_year
       , MAX(CASE WHEN w.yr >= b.amz_gone_since THEN w.yr END)           AS wmt_last_year_after
  FROM amz_before b
  JOIN wmt_brand_year w
    ON w.brand = b.brand
  GROUP BY b.brand
)
SELECT b.amz_gone_since
     , b.brand
     , b.amz_last_active_year
     , b.amz_sales_before_gone
     , IFNULL(w.wmt_sales_before, 0) AS wmt_sales_before
     , IFNULL(w.wmt_sales_after,  0) AS wmt_sales_after
     , w.wmt_last_active_year
     -- 이동 판정
     , CASE
         WHEN w.brand IS NULL                       THEN 'NO_WALMART'        -- 월마트에 아예 없음
         WHEN IFNULL(w.wmt_sales_after, 0) > 0
          AND IFNULL(w.wmt_sales_before,0) = 0       THEN 'MIGRATED'          -- 소멸 후 신규 등장 → 이동 강한 신호
         WHEN IFNULL(w.wmt_sales_after, 0) > 0       THEN 'SURVIVED_BOTH'     -- 양쪽 존재 → 잔존
         ELSE 'WMT_ALSO_GONE'                                                -- 월마트도 그 전에 사라짐
       END AS migration_status
FROM amz_before b
LEFT JOIN wmt_agg w
  ON w.brand = b.brand
ORDER BY b.amz_gone_since, b.amz_sales_before_gone DESC
;


-- [Amazon에서 사라진 brand 분석]

WITH brand_year AS (   -- 연도별 활동 brand + 매출 집계
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0) AS sales
       , SUM(UnitsSold) AS units
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses'
    AND RetailSales > 0
  GROUP BY 1, 2
)
, wmt_brand_year AS (
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0) AS sales
       , SUM(UnitsSold) AS units
  FROM stck.wmt_atlas_sales_all
  WHERE SubCategory = 'Mattresses'
    AND RetailSales > 0
  GROUP BY 1, 2
) 
, lost_2021 AS (
  SELECT brand FROM brand_year WHERE yr <= 2020
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2021
)
, lost_2022 AS (
  SELECT brand FROM brand_year WHERE yr <= 2021
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2022
)
, lost_2023 AS (
  SELECT brand FROM brand_year WHERE yr <= 2022
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2023
)
, lost_2024 AS (
  SELECT brand FROM brand_year WHERE yr <= 2023
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2024
)
, lost_2025 AS (
  SELECT brand FROM brand_year WHERE yr <= 2024
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2025
)
, lost_2026 AS (
  SELECT brand FROM brand_year WHERE yr <= 2025
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2026
)
, lost_all AS (
  SELECT brand, 2021 AS gone_since FROM lost_2021
  UNION ALL
  SELECT brand, 2022 FROM lost_2022
  UNION ALL
  SELECT brand, 2023 FROM lost_2023
  UNION ALL
  SELECT brand, 2024 FROM lost_2024
  UNION ALL
  SELECT brand, 2025 FROM lost_2025
  UNION ALL
  SELECT brand, 2026 FROM lost_2026
)
, lost_first AS (   -- brand별 최초 소멸 연도만
  SELECT brand, MIN(gone_since) AS gone_since
  FROM lost_all
  GROUP BY brand
)
SELECT l.gone_since
     , l.brand
     , SUM(yr_data.sales) AS sales_before_gone
     , MAX(yr_data.yr)    AS last_active_year
FROM lost_first l
JOIN brand_year yr_data
  ON yr_data.brand = l.brand
 AND yr_data.yr < l.gone_since
GROUP BY l.gone_since, l.brand
ORDER BY l.gone_since, sales_before_gone DESC
;


--[ Walmart Brand 들 조사하기 ]







-- [ 2024년 이전 존재했다가 2025년 사라졌다가 2026년 다시 등장한 Brand들 찾기 ] 
-- [ 결과 : 5개 브랜드 뿐 ]

WITH brand_year AS (
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0) AS sales
       , SUM(UnitsSold) AS units
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses'
    AND RetailSales > 0
  GROUP BY 1, 2
)
, before_2024 AS (
  SELECT DISTINCT brand FROM brand_year WHERE yr <= 2023
)
, in_2025 AS (
  SELECT DISTINCT brand FROM brand_year WHERE yr = 2025
)
, in_2026 AS (
  SELECT DISTINCT brand FROM brand_year WHERE yr = 2026
)
, comeback AS (
  (
    SELECT brand FROM before_2024
    INTERSECT DISTINCT
    SELECT brand FROM in_2026
  )
  EXCEPT DISTINCT
  SELECT brand FROM in_2025
)
SELECT yr_data.brand
     , yr_data.yr
     , yr_data.sales
     , yr_data.units
FROM brand_year yr_data
JOIN comeback USING (brand)
ORDER BY yr_data.brand, yr_data.yr;




-- [ Brand별 test ]

SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
	, ROUND(SUM(RetailSales), 0) AS sales
FROM stck.atlas_sales_all 
  WHERE SubCategory = 'Mattresses'
    AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') = '1INCH'
GROUP BY 1 
ORDER BY 1 ; 





WITH brand_year AS (   -- 연도별 활동 brand + 매출 집계
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
       , ROUND(SUM(RetailSales), 0) AS sales
       , SUM(UnitsSold) AS units
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses'
    AND RetailSales > 0
  GROUP BY 1, 2
)
-- 1) ~2023 존재, 2024~ 소멸
, lost_2024 AS (
  SELECT brand FROM brand_year WHERE yr <= 2023
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2024
)
-- 2) ~2024 존재, 2025~ 소멸
, lost_2025 AS (
  SELECT brand FROM brand_year WHERE yr <= 2024
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2025
)
-- 3) ~2025 존재, 2026~ 소멸
, lost_2026 AS (
  SELECT brand FROM brand_year WHERE yr <= 2025
  EXCEPT DISTINCT
  SELECT brand FROM brand_year WHERE yr >= 2026
)
SELECT brand
     , SUM(sales) AS sales_until_2025      -- 소멸 전 누적 매출
     , MAX(yr)    AS last_active_year       -- 마지막 활동 연도
FROM brand_year 
JOIN lost_2026 USING (brand)
WHERE yr <= 2025
GROUP BY brand
ORDER BY sales_until_2025 DESC;





SELECT '2024년 이후 소멸' AS cohort, brand FROM lost_2024
UNION ALL
SELECT '2025년 이후 소멸', brand FROM lost_2025
UNION ALL
SELECT '2026년 이후 소멸', brand FROM lost_2026
ORDER BY cohort, brand;




WITH amz_brand_2025 AS (
	SELECT DISTINCT REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
		, round(sum(RetailSales), 0) AS sales
		, sum(UnitsSold) AS units 
		, rank() OVER (ORDER BY sum(RetailSales) DESC) AS rnk 
	FROM stck.atlas_sales_all 
	WHERE SubCategory = 'Mattresses'
		AND EXTRACT(YEAR FROM WeekEnding) = 2025 AND RetailSales > 0
	GROUP BY 1
	ORDER BY 2 DESC 
)
, amz_brand_2026 AS (
	SELECT DISTINCT REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
		, round(sum(RetailSales), 0) AS sales
		, sum(UnitsSold) AS units 
		, rank() OVER (ORDER BY sum(RetailSales) DESC) AS rnk 
	FROM stck.atlas_sales_all 
	WHERE SubCategory = 'Mattresses'
		AND EXTRACT(YEAR FROM WeekEnding) = 2026 AND RetailSales > 0
	GROUP BY 1
	ORDER BY 2 DESC 
)







SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
	, round(sum(RetailSales), 0) AS sales
	, sum(UnitsSold) AS units 
FROM stck.wmt_atlas_sales_all 
WHERE SubCategory = 'Mattresses' 
GROUP BY 1 
ORDER BY 1 
	
	
	
SELECT EXTRACT(YEAR FROM WeekEnding) AS yr
	, round(sum(RetailSales), 0) AS sales
	, sum(UnitsSold) AS units 
FROM stck.atlas_sales_all 
WHERE SubCategory = 'Mattresses' 
GROUP BY 1 
ORDER BY 1 
	
	
	
	
	

