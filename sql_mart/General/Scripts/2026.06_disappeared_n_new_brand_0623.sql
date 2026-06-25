/*
 * ZNS-2940 : Amazon에서 Walmart로 이동한 Brand들을 추적하기
 */

--[ I. Amazon 매트리스 brand 연도별 통계 + 소멸 코호트 차트용 데이터 ]
--CREATE OR REPLACE TABLE wook.tmp_amz_brand_yearly_stats AS

-- 1. brand_year: (연도, brand)별 매출 집계 테이블
WITH brand_year AS (
  SELECT EXTRACT(YEAR FROM WeekEnding)                      AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')  AS brand
       , ROUND(SUM(RetailSales), 0)                         AS sales
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
  GROUP BY 1, 2
)
--SELECT * FROM brand_year WHERE  brand='BESTCOMFORT'

-- brand별 최초/마지막 활동 연도 + 누적 매출 (소멸 판정 및 누적매출용)
, seq AS (
  SELECT brand
       , MIN(yr)     AS first_yr
       , MAX(yr)     AS last_yr
       , SUM(sales)  AS cum_sales   -- 소멸 전까지 brand 누적 매출
  FROM brand_year
  GROUP BY brand
)

-- 데이터 최신 연도 (진행 중 연도 → 소멸 판정 제외용)
, max_yr AS (
  SELECT MAX(yr) AS data_max_yr FROM brand_year
)

-- 2 & 3. 연도별 distinct brand 수 + 신생 brand 수
, yearly AS (
  SELECT byr.yr
       , COUNT(DISTINCT byr.brand)                                   AS total_brands
       , COUNT(DISTINCT IF(s.first_yr = byr.yr, byr.brand, NULL))    AS new_brands
  FROM brand_year byr
  JOIN seq s USING (brand)
  GROUP BY byr.yr
)

-- 4. 연도별 소멸 brand 수 + 소멸 brand 누적 매출
--    소멸 연도(gone_since) = 마지막 활동 연도 + 1, 단 진행 중 연도는 제외
, gone AS (
  SELECT s.last_yr + 1          AS yr
       , COUNT(*)               AS disappeared_brands
       , SUM(s.cum_sales)       AS disappeared_cum_sales
  FROM seq s
  CROSS JOIN max_yr m
  WHERE s.last_yr < m.data_max_yr
  GROUP BY 1
)

-- 최종: 차트 계산에 필요한 모든 지표 결합
SELECT y.yr
     , y.total_brands                                            -- 그 해 활동 brand 수
     , y.new_brands                                              -- 그 해 신생 brand 수
     , COALESCE(g.disappeared_brands, 0)     AS disappeared_brands     -- 그 해 소멸 brand 수
     , COALESCE(g.disappeared_cum_sales, 0)  AS disappeared_cum_sales  -- 소멸 brand 누적 매출
     , ROUND(SAFE_DIVIDE(g.disappeared_cum_sales, g.disappeared_brands), 0)
                                             AS disappeared_avg_sales  -- 소멸 brand 평균 매출
FROM yearly y
LEFT JOIN gone g USING (yr)
ORDER BY y.yr
;



-- 2024 활동집합과 2025 활동집합의 차집합 분석
WITH brand_year AS (
  SELECT EXTRACT(YEAR FROM WeekEnding) AS yr,
         REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
  FROM stck.atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
  GROUP BY 1, 2
),
a24 AS (SELECT brand FROM brand_year WHERE yr = 2024),
a25 AS (SELECT brand FROM brand_year WHERE yr = 2025),
seq AS (SELECT brand, MIN(yr) as first_yr FROM brand_year GROUP BY brand)
--SELECT brand FROM a24 WHERE brand NOT IN (SELECT brand FROM a25)

SELECT
  (SELECT COUNT(*) FROM a24) AS active_2024,
  (SELECT COUNT(*) FROM a25) AS active_2025,
  -- 2024엔 있고 2025엔 없는 brand (= 진짜 이탈)
  (SELECT COUNT(*) FROM a24 WHERE brand NOT IN (SELECT brand FROM a25)) AS left_24_to_25,
  -- 2025엔 있고 2024엔 없는 brand (= 유입: 신생 + 재진입)
  (SELECT COUNT(*) FROM a25 WHERE brand NOT IN (SELECT brand FROM a24)) AS entered_25,
  -- 그중 신생(2025 first)
  (SELECT COUNT(*) FROM a25 JOIN seq USING(brand)
     WHERE seq.first_yr = 2025 AND a25.brand NOT IN (SELECT brand FROM a24)) AS new_25,
  -- 그중 재진입(2025 이전 first, 2024 미활동)
  (SELECT COUNT(*) FROM a25 JOIN seq USING(brand)
     WHERE seq.first_yr < 2025 AND a25.brand NOT IN (SELECT brand FROM a24)) AS reentry_25
;


--[ II. Walmart 매트리스 brand 연도별 통계 + 소멸 코호트 차트용 데이터 ]
--CREATE OR REPLACE TABLE wook.tmp_amz_brand_yearly_stats AS

-- 1. brand_year: (연도, brand)별 매출 집계 테이블
WITH brand_year AS (
  SELECT EXTRACT(YEAR FROM WeekEnding)                      AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')  AS brand
       , ROUND(SUM(RetailSales), 0)                         AS sales
  FROM stck.wmt_atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
  GROUP BY 1, 2
)
--SELECT yr, count(DISTINCT brand) FROM brand_year GROUP BY 1 ORDER BY 1
-- brand별 최초/마지막 활동 연도 + 누적 매출 (소멸 판정 및 누적매출용)
, seq AS (
  SELECT brand
       , MIN(yr)     AS first_yr
       , MAX(yr)     AS last_yr
       , SUM(sales)  AS cum_sales   -- 소멸 전까지 brand 누적 매출
  FROM brand_year
  GROUP BY brand
)

-- 데이터 최신 연도 (진행 중 연도 → 소멸 판정 제외용)
, max_yr AS (
  SELECT MAX(yr) AS data_max_yr FROM brand_year
)

-- 2 & 3. 연도별 distinct brand 수 + 신생 brand 수
, yearly AS (
  SELECT byr.yr
       , COUNT(DISTINCT byr.brand)                                   AS total_brands
       , COUNT(DISTINCT IF(s.first_yr = byr.yr, byr.brand, NULL))    AS new_brands
  FROM brand_year byr
  JOIN seq s USING (brand)
  GROUP BY byr.yr
)

-- 4. 연도별 소멸 brand 수 + 소멸 brand 누적 매출
--    소멸 연도(gone_since) = 마지막 활동 연도 + 1, 단 진행 중 연도는 제외
, gone AS (
  SELECT s.last_yr + 1          AS yr
       , COUNT(*)               AS disappeared_brands
       , SUM(s.cum_sales)       AS disappeared_cum_sales
  FROM seq s
  CROSS JOIN max_yr m
  WHERE s.last_yr < m.data_max_yr
  GROUP BY 1
)

-- 최종: 차트 계산에 필요한 모든 지표 결합
SELECT y.yr
     , y.total_brands                                            -- 그 해 활동 brand 수
     , y.new_brands                                              -- 그 해 신생 brand 수
     , COALESCE(g.disappeared_brands, 0)     AS disappeared_brands     -- 그 해 소멸 brand 수
     , COALESCE(g.disappeared_cum_sales, 0)  AS disappeared_cum_sales  -- 소멸 brand 누적 매출
     , ROUND(SAFE_DIVIDE(g.disappeared_cum_sales, g.disappeared_brands), 0)
                                             AS disappeared_avg_sales  -- 소멸 brand 평균 매출
FROM yearly y
LEFT JOIN gone g USING (yr)
ORDER BY y.yr
;



-- [ Walmart Brand Family 매출 ] ------------------------------
WITH brand_year AS (
  SELECT EXTRACT(YEAR FROM WeekEnding)                      AS yr
       , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')  AS brand
       , ROUND(SUM(RetailSales), 0)                         AS sales
  FROM stck.wmt_atlas_sales_all
  WHERE SubCategory = 'Mattresses' AND RetailSales > 0
  GROUP BY 1, 2
)
--SELECT yr, sum(sales) FROM brand_year GROUP BY 1 ORDER BY 1
, brand_year_fam AS (
  SELECT by_.yr
       , by_.brand
       , by_.sales
       , COALESCE(fm.FAMILY_NAME, by_.brand) AS brand_family
  FROM brand_year AS by_
  LEFT JOIN `meta.brand_family_mapping` AS fm
         ON by_.brand = REGEXP_REPLACE(UPPER(fm.BRAND_UPPER), r'[^[:print:]]', '')
)
SELECT yr, brand_family, sum(sales)
FROM brand_year_fam
WHERE brand_family IN ('ZINUS','NOVILLA FAMILY','MLILY FAMILY','FDW DIRECT')    
GROUP BY 1,2 ORDER BY 1,2 



-- ============================================================
-- UPC prefix가 같은 Walmart 매트리스 브랜드 군집 추출
--   - GS1 회사 prefix(UPC 앞 8자리)를 공유하면 동일 등록 주체로 추정
--   - 매출 $1M 이상 brand만 / 한 prefix에 2개 이상 묶이면 family 후보
-- ============================================================
WITH src AS (
  SELECT
    UPPER(TRIM(Brand)) AS brand,
    LPAD(CAST(CAST(upc AS INT64) AS STRING), 12, '0') AS upc12,
    RetailSales AS sales
  FROM `market-analysis-project-91130.stck.wmt_atlas_sales_all`
  WHERE LOWER(SubCategory) LIKE '%mattress%'
    AND upc IS NOT NULL AND upc > 0
    AND EXTRACT(YEAR FROM WeekEnding) BETWEEN 2022 AND 2026
),
-- 브랜드별 총매출 → $1M 이상만 유지
brand_sales AS (
  SELECT brand, SUM(sales) AS brand_sales
  FROM src
  GROUP BY brand
  HAVING SUM(sales) >= 1e6
),
-- brand × prefix(8자리) 매핑 ($1M 이상 brand로 한정)
brand_prefix AS (
  SELECT DISTINCT SUBSTR(s.upc12, 1, 8) AS prefix8, s.brand
  FROM src s
  JOIN brand_sales bs USING (brand)
),
-- prefix별 군집 집계
prefix_cluster AS (
  SELECT
    bp.prefix8,
    COUNT(DISTINCT bp.brand) AS n_brands,
    ROUND(SUM(bs.brand_sales) / 1e6, 1) AS cluster_sales_m,
    STRING_AGG(
      bp.brand || ' ($' || CAST(ROUND(bs.brand_sales/1e6, 1) AS STRING) || 'M)',
      ' | ' ORDER BY bs.brand_sales DESC
    ) AS brands_in_cluster
  FROM brand_prefix bp
  JOIN brand_sales bs USING (brand)
  GROUP BY bp.prefix8
)
-- 2개 이상 brand가 묶이는 prefix = family 후보
SELECT
  prefix8 AS upc_prefix,
  n_brands,
  cluster_sales_m,
  brands_in_cluster
FROM prefix_cluster
WHERE n_brands >= 2
ORDER BY n_brands DESC, cluster_sales_m DESC


-- [MLILY FAMILY에 SYNTHOSPACE 브랜드 추가하기]

INSERT INTO meta.brand_family_mapping (BRAND_UPPER, FAMILY_NAME)
VALUES ('SYNTHOSPACE', 'MLILY FAMILY');




