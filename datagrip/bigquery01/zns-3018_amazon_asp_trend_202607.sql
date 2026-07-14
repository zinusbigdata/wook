/*
    ZNS-3018: 아마존 시장에서 ASP 추이와 Top Brand들의 ASP 트랜드
 */

-- [ 1. Amazon Mattress : ASP of Top Brands ] ----------------------------------------------------------------------
CREATE OR REPLACE TABLE wook.tmp_amz_matt_asp AS
WITH monthly_matt_brand AS (
	SELECT
	    FORMAT_DATE('%Y-%m', WeekEnding) as yr_month
		-- RetailerSku AS asin
		--c.yr_month AS yr_month
	    , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]','') as brand

	    , ROUND(SUM(RetailSales), 0) AS sales
	    , SUM(UnitsSold) AS units
	    --, ROUND(AVG(RetailPrice), 1) AS avg_retail_price
	FROM
		stck.atlas_sales_all a
	    LEFT JOIN meta.wk_calendar_new c ON a.WeekEnding BETWEEN c.start_date AND c.end_date
	WHERE SubCategory='Mattresses'
		--WeekEnding >= '2024-01-01' AND SubCategory='Mattresses'
	GROUP BY 1,2
)
, with_share AS (
	SELECT *
		, ROUND(SAFE_DIVIDE(sales, units), 1) AS asp
		, ROUND(sales / NULLIF(SUM(sales) OVER (PARTITION BY yr_month), 0) * 100, 1) AS share_pct
		--, RANK() OVER (PARTITION BY yr_month ORDER BY sales DESC) AS sales_rank
	FROM monthly_matt_brand
)
SELECT *
FROM with_share
WHERE brand IN ('ZINUS','NOVILLA','EGOHOME','MLILY','FDW')

UNION ALL

SELECT yr_month
	, 'ALL' 		AS brand
	, SUM(sales) 	AS sales
	, SUM(units)	AS units
	--, NULL 			AS avg_retail_price
	, ROUND(SAFE_DIVIDE(SUM(sales), SUM(units)), 1) AS asp
	, NULL			AS share_pct
	--, NULL			AS sales_rank
FROM with_share
GROUP BY 1

ORDER BY yr_month desc, brand
;



--- [ 2. Amazon Beds : ASP of Top brands ] --------------------------------
CREATE OR REPLACE TABLE wook.tmp_amz_beds_asp AS
WITH monthly_beds_brand AS (
	SELECT
	    FORMAT_DATE('%Y-%m', WeekEnding) as yr_month
		-- RetailerSku AS asin
		--c.yr_month AS yr_month
	    , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]','') as brand

	    , ROUND(SUM(RetailSales), 0) AS sales
	    , SUM(UnitsSold) AS units
	    --, ROUND(AVG(RetailPrice), 1) AS avg_retail_price
	FROM
		stck.atlas_sales_all a
	    LEFT JOIN meta.wk_calendar_new c ON a.WeekEnding BETWEEN c.start_date AND c.end_date
	WHERE SubCategory='Beds'
		--WeekEnding >= '2024-01-01' AND SubCategory='Mattresses'
	GROUP BY 1,2
)
, with_beds_share AS (
	SELECT *
		, ROUND(SAFE_DIVIDE(sales, units), 1) AS asp
		, ROUND(sales / NULLIF(SUM(sales) OVER (PARTITION BY yr_month), 0) * 100, 1) AS share_pct
		--, RANK() OVER (PARTITION BY yr_month ORDER BY sales DESC) AS sales_rank
	FROM monthly_beds_brand
)
SELECT *
FROM with_beds_share
WHERE brand IN ('ZINUS','LIFEZONE','ALLEWIE','SHA CERLIN')

UNION ALL

SELECT yr_month
	, 'ALL' 		AS brand
	, SUM(sales) 	AS sales
	, SUM(units)	AS units
	--, NULL 			AS avg_retail_price
	, ROUND(SAFE_DIVIDE(SUM(sales), SUM(units)), 1) AS asp
	, NULL			AS share_pct
	--, NULL			AS sales_rank
FROM with_beds_share
GROUP BY 1

ORDER BY yr_month desc, brand
;


--- [ 3. Amazon Bed Frames : ASP of Top Brands ] --------------------------------
CREATE OR REPLACE TABLE wook.tmp_amz_frames_asp AS
WITH monthly_frames_brand AS (
	SELECT
	    FORMAT_DATE('%Y-%m', WeekEnding) as yr_month
		-- RetailerSku AS asin
		--c.yr_month AS yr_month
	    , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]','') as brand

	    , ROUND(SUM(RetailSales), 0) AS sales
	    , SUM(UnitsSold) AS units
	    --, ROUND(AVG(RetailPrice), 1) AS avg_retail_price
	FROM
		stck.atlas_sales_all a
	    LEFT JOIN meta.wk_calendar_new c ON a.WeekEnding BETWEEN c.start_date AND c.end_date
	WHERE SubCategory='Bed Frames'
		--WeekEnding >= '2024-01-01' AND SubCategory='Mattresses'
	GROUP BY 1,2
)
, with_frames_share AS (
	SELECT *
		, ROUND(SAFE_DIVIDE(sales, units), 1) AS asp
		, ROUND(sales / NULLIF(SUM(sales) OVER (PARTITION BY yr_month), 0) * 100, 1) AS share_pct
		--, RANK() OVER (PARTITION BY yr_month ORDER BY sales DESC) AS sales_rank
	FROM monthly_frames_brand
)
SELECT *
FROM with_frames_share
WHERE brand IN ('ZINUS','NEW JETO','AMAZONBASICS','HLIPHA')

UNION ALL

SELECT yr_month
	, 'ALL' 		AS brand
	, SUM(sales) 	AS sales
	, SUM(units)	AS units
	--, NULL 			AS avg_retail_price
	, ROUND(SAFE_DIVIDE(SUM(sales), SUM(units)), 1) AS asp
	, NULL			AS share_pct
	--, NULL			AS sales_rank
FROM with_frames_share
GROUP BY 1

ORDER BY yr_month desc, brand
;

---[ 04. Consolidate ] -----------------------------------------------------

SELECT 'Mattress' AS category
    , *
FROM wook.tmp_amz_matt_asp
UNION ALL
SELECT 'Beds' AS category
    , *
FROM wook.tmp_amz_beds_asp
UNION ALL
SELECT 'Bed Frames' AS category
    , *
FROM wook.tmp_amz_frames_asp
;

