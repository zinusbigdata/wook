/*
 * Amazon Market에서 분기별 ASP 추이 분석 
 * 마이클 요청 : Mattress, Beds, Bed Frames 마켓 전체 --> EU 마켓 --> 경쟁 Brand별도 추가하기 
 */


--# 2026.04.14, Amazon Mattress Market Analysis 

--# 100. 미국 전체
--# Amazon US에서 Beds, Bed Frames, Mattresses에서 뷴기별 평균 가격 추이
--# 데이터 소스 : stck.atlas_sales_all 

SELECT
    SubCategory,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    --count(*),
    count(DISTINCT RetailerSku) AS asin_cn
FROM stck.atlas_sales_all 
WHERE SubCategory IN ('Mattresses', 'Beds', 'Bed Frames')
  AND RetailPrice > 0
GROUP BY SubCategory, yr_qt
ORDER BY SubCategory, yr_qt
;

--# 110. 미국 경쟁사
--# 111. Mattress 경쟁사

--SELECT DISTINCT brand FROM wook.stck_zns_comp_sales_anal

SELECT brand, yr_quarter,
    ROUND(AVG(avg_retail_price), 1) AS avg_price,
    ROUND(SUM(avg_retail_price * units) / NULLIF(SUM(units), 0), 1) AS weighted_avg_price,
FROM wook.stck_zns_comp_sales_anal 
GROUP BY 1,2
ORDER BY 1,2
	



-- # 2026.04.15
-- # Stackline 데이터 바로 활용하기 
-- # Mattress, Beds, Bed Frames 경쟁사들의 분기별 평균 가격 추이 분석


/*
 * 각 개 버전 : Mattress
 */

WITH cte_stck_matt_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Mattresses'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('FDW', 'EGOHOME', 'NOVILLA', 'ZINUS')
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
)
SELECT brand,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    count(DISTINCT RetailerSku),
    'Mattress' AS category
FROM cte_stck_matt_comp
GROUP BY 1, 2
ORDER BY 1,2
;

/*
 * 각 개 버번 : Bed Frames
 * Bed Frame : NEW JETO, AMAZON BASICS, SVEN & SON, Zinus Family
 */

WITH cte_stck_bedframes_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
		, CASE
		    WHEN REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('ZINUS', 'MELLOW', 'BEST PRICE MATTRESS', 'BPM')
		        THEN 'ZINUS FAM'
		    ELSE REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')
		  END AS brand_fam
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Bed Frames'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN (
			'ZINUS', 'MELLOW', 'BEST PRICE MATTRESS',
			'NEW JETO', 'AMAZONBASICS', 'SVEN & SON')
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
)
SELECT brand_fam,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    'Bed Frames' AS category
FROM cte_stck_bedframes_comp
GROUP BY 1,2
ORDER BY 1,2
;

/*
 * 각 개 버번 : Beds 
 * Beds : AMOLIFE FAMILY, ROLANSTAR, Zinus Family, YAHEETECH
 */
WITH cte_stck_beds_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
		, CASE
		    WHEN REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('ZINUS', 'MELLOW', 'BEST PRICE MATTRESS', 'BPM')
		        THEN 'ZINUS FAM'
		    WHEN REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('SHA CERLIN','ALLEWIE','AMOLIFE','AMERLIFE','LIKIMIO','EINFACH','IMUSEE','KEALIVE')
		        THEN 'AMOLIFE FAM'
		    ELSE REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')
		  END AS brand_fam
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Beds'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN (
			'ZINUS', 'MELLOW', 'BEST PRICE MATTRESS',
			'YAHEETECH', 'ROLANSTAR',
			'SHA CERLIN','ALLEWIE','AMOLIFE','AMERLIFE','LIKIMIO','EINFACH','IMUSEE','KEALIVE'
			)
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
)
--SELECT DISTINCT brand_fam, brand FROM cte_stck_beds_comp ORDER BY 1,2
SELECT brand_fam,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    'Beds' AS category
FROM cte_stck_beds_comp
GROUP BY 1, 2
ORDER BY 1, 2
;





/*************************
 * 통합 버전 2 : Family 버전
 *************************/

WITH cte_stck_matt_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
		, REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand_fam
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Mattresses'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('FDW', 'EGOHOME', 'NOVILLA', 'ZINUS')
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
)
, cte_stck_bedframes_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
		, CASE
		    WHEN REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('ZINUS', 'MELLOW', 'BEST PRICE MATTRESS', 'BPM')
		        THEN 'ZINUS FAM'
		    ELSE REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')
		  END AS brand_fam
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Bed Frames'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN (
			'ZINUS', 'MELLOW', 'BEST PRICE MATTRESS',
			'NEW JETO', 'AMAZONBASICS', 'SVEN & SON')
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
)
, cte_stck_beds_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
		, CASE
		    WHEN REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('ZINUS', 'MELLOW', 'BEST PRICE MATTRESS', 'BPM')
		        THEN 'ZINUS FAM'
		    WHEN REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('SHA CERLIN','ALLEWIE','AMOLIFE','AMERLIFE','LIKIMIO','EINFACH','IMUSEE','KEALIVE')
		        THEN 'AMOLIFE FAM'
		    ELSE REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '')
		  END AS brand_fam
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Beds'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN (
			'ZINUS', 'MELLOW', 'BEST PRICE MATTRESS',
			'YAHEETECH', 'ROLANSTAR',
			'SHA CERLIN','ALLEWIE','AMOLIFE','AMERLIFE','LIKIMIO','EINFACH','IMUSEE','KEALIVE'
			)
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
)

SELECT brand_fam,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    'Mattress' AS category
FROM cte_stck_matt_comp
GROUP BY 1, 2

UNION ALL

SELECT brand_fam,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    'Bed Frames' AS category
FROM cte_stck_bedframes_comp
GROUP BY 1, 2

UNION ALL

SELECT brand_fam,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    'Beds' AS category
FROM cte_stck_beds_comp
GROUP BY 1, 2

ORDER BY 5, 1, 2
;











/********************
 * 통합 버전 1
 *******************/


WITH cte_stck_matt_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Mattresses'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN ('FDW', 'EGOHOME', 'NOVILLA', 'ZINUS')
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
),
cte_stck_bedframes_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Bed Frames'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN (
			'ZINUS','NEW JETO', 'AMAZONBASICS', 'SVEN & SON')
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
)
--SELECT DISTINCT brand FROM cte_stck_bedframes_comp
, cte_stck_beds_comp AS (
	SELECT
		REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') AS brand
	    , * EXCEPT (brand)
	FROM stck.atlas_sales_all
	WHERE SubCategory = 'Beds'
		AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') IN (
			'ZINUS','YAHEETECH', 'ROLANSTAR','AMERLIFE'
			)
		AND LENGTH(REGEXP_REPLACE(TRIM(RetailerSku), r'[^[:print:]]', '')) = 10
		AND RetailPrice > 0
		AND WeekEnding >= '2021-01-01'
)
--SELECT DISTINCT brand FROM cte_stck_beds_comp
SELECT brand,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    'Mattress' AS category
FROM cte_stck_matt_comp
GROUP BY 1, 2

UNION ALL

SELECT brand,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    'Bed Frames' AS category
FROM cte_stck_bedframes_comp
GROUP BY 1, 2

UNION ALL

SELECT brand,
    CONCAT(FORMAT_DATE('%Y', WeekEnding), '-', EXTRACT(QUARTER FROM WeekEnding)) AS yr_qt,
    ROUND(AVG(RetailPrice), 1) AS avg_price,
    ROUND(SUM(RetailPrice * UnitsSold) / NULLIF(SUM(UnitsSold), 0), 1) AS weighted_avg_price,
    'Beds' AS category
FROM cte_stck_beds_comp
GROUP BY 1, 2

ORDER BY 5, 1, 2
;

--

SELECT
    *
FROM
    stck.atlas_sales_all
WHERE
    subcategory = 'Beds'
    AND REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]', '') = 'AMAZONBASICS'
 



--# 02. 유럽 
--# Mattress, Non-mattress 분기별 평균 가격 추이

SELECT 
	TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) AS category, 
	FORMAT_TIMESTAMP('%Y-Q%Q', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', crawlTime)) AS yr_qt,
	--count(*),
	count(DISTINCT bestsellers_asin) AS asin_cnt,
	ROUND(AVG(bestsellers_price_value), 1) AS avg_price,
	'UK' AS country
FROM dw.rf_amzuk_bsr_all 
WHERE TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) IN ('Mattresses', 'Beds, Frames & Bases')
GROUP BY 1,2

UNION ALL

SELECT 
	TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) AS category, 
	FORMAT_TIMESTAMP('%Y-Q%Q', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', crawlTime)) AS yr_qt,
	--count(*),
	count(DISTINCT bestsellers_asin) AS asin_cnt,
	ROUND(AVG(bestsellers_price_value), 1) AS avg_price,
	'DE' AS country
FROM dw.rf_amzde_bsr_all 
WHERE TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) IN ('Mattresses', 'Beds, Frames & Bases')
GROUP BY 1,2

UNION ALL

SELECT 
	TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) AS category, 
	FORMAT_TIMESTAMP('%Y-Q%Q', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', crawlTime)) AS yr_qt,
	--count(*),
	count(DISTINCT bestsellers_asin) AS asin_cnt,
	ROUND(AVG(bestsellers_price_value), 1) AS avg_price,
	'FR' AS country
FROM dw.rf_amzfr_bsr_all 
WHERE TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) IN ('Mattress', 'Beds and bed frames')
GROUP BY 1,2
-- select distinct fullCategory from dw.rf_amzfr_bsr_all

UNION ALL

SELECT 
	TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) AS category, 
	FORMAT_TIMESTAMP('%Y-Q%Q', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', crawlTime)) AS yr_qt,
	--count(*),
	count(DISTINCT bestsellers_asin) AS asin_cnt,
	ROUND(AVG(bestsellers_price_value), 1) AS avg_price,
	'IT' AS country
FROM dw.rf_amzit_bsr_all 
WHERE TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) IN ('Mattresses', 'Beds, frames and bases')
GROUP BY 1,2
-- select distinct fullCategory from dw.rf_amzit_bsr_all


UNION ALL

SELECT 
	TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) AS category, 
	FORMAT_TIMESTAMP('%Y-Q%Q', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', crawlTime)) AS yr_qt,
	--count(*),
	count(DISTINCT bestsellers_asin) AS asin_cnt,
	ROUND(AVG(bestsellers_price_value), 1) AS avg_price,
	'ES' AS country
FROM dw.rf_amzes_bsr_all 
WHERE TRIM(REGEXP_EXTRACT(fullCategory, r'[^>]+$')) IN ('mattresses', 'Beds, structures and bases')
GROUP BY 1,2
-- select distinct fullCategory from dw.rf_amzes_bsr_all

ORDER BY 5,1,2




/*
 * EU BSR Top 20 제품 asin - 조희진 선임 data
 */

SELECT type, yr_quarter,
	ROUND(AVG(list_price), 1) AS avg_list_price,
	ROUND(AVG(buybox_price), 1) AS avg_bb_price,
FROM wook.uk_bsr_top20_asins_daily_price
WHERE year >= '2020'
GROUP BY 1,2
ORDER BY 1,2



/*
 * EU 경쟁사 가격 
 */

SELECT brand,
	yr_quarter,
	avg(avg_bb_price) AS avg_bb_price,
	avg(avg_bsr_price) AS avg_bsr_price
FROM wook.uk_mattress_bsr_price_analysis 
WHERE year >= '2021'
GROUP BY 1,2
ORDER BY 1,2



--# END 



