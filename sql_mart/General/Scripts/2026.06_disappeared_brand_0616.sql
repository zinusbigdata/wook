
/*
 * Amazon에서 Walmart로 이동한 Brand들을 추적하기
 */

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
)
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
	
	
	
	
	

