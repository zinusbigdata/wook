/*
 * ZNS-2858: 아마존 Top 4 경쟁사 ASP + Traffic 분석
 */

--- [01. Amazon Mattress ASP - Foam Mattress Only] ---------------------
CREATE OR REPLACE TABLE wook.tmp_stck_asp_with_meta AS 
WITH with_meta AS (
    SELECT
        *
    FROM
        (

            SELECT DISTINCT
                TRIM(a.asin) AS asin
                , TRIM(a.zinus_sku) AS zinus_sku
                , IF(a.asin = 'B0B6FQZMJ4', '5', REGEXP_EXTRACT(LOWER(TRIM(inch_color)), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')) AS inch
                , TRIM(size) AS size
                --, if(a.financial_category = 'Foam Mattresses', 'Foam Mattress', a.financial_category) as category
                , CASE a.financial_category
				      WHEN 'Foam Mattresses'   THEN 'Foam Mattress'
					      WHEN 'Spring Mattresses' THEN 'Spring Mattress'
					      ELSE a.financial_category
					  END AS category 
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
, stck_with_meta AS (
        SELECT
            a.RetailerSku AS asin
            , FORMAT_DATE('%Y-%m', a.WeekEnding) as yr_month

            , ANY_VALUE(b.zinus_sku) AS zinus_sku

            , MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')) as brand

            , SUM(a.RetailSales) AS sales
            , SUM(a.UnitsSold) AS units
            , AVG(RetailPrice) AS avg_retail_price
            --, AVG(listprice.value_num) AS avg_list_price

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
            JOIN with_meta b ON a.RetailerSku = b.asin
        WHERE
            REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') IN ( 'FDW', 'EGOHOME', 'NOVILLA', 'ZINUS', 'MLILY' )
            --AND SubCategory = 'Mattresses'
            AND a.WeekEnding >= '2024-01-01'
        GROUP BY 1, 2
)
SELECT 
	yr_month, brand
    , ROUND(SUM(sales), 0) AS sales
    , SUM(units) AS units
	, ROUND(SAFE_DIVIDE(SUM(sales), SUM(units)), 1) AS asp
FROM stck_with_meta
WHERE category = 'Foam Mattress'
GROUP BY 1,2

/*UNION ALL 

SELECT yr_month
	, 'ALL'			AS brand
	, ROUND(SUM(sales), 0) 	AS sales
	, SUM(units)	AS units
	, ROUND(SAFE_DIVIDE(SUM(sales), SUM(units)), 1) AS asp
FROM stck_with_meta  
WHERE category = 'Foam Mattress'	-- Foam Mattress만 추출  
GROUP BY 1
*/
ORDER BY 1,2
;


--- [02. Stackline Traffic - Foam Mattress ] ------------------------------------------------
CREATE OR REPLACE TABLE wook.tmp_stck_traffic_with_meta AS 
WITH with_meta AS (
    SELECT
        *
    FROM
        (
            SELECT DISTINCT
                TRIM(a.asin) AS asin
                , TRIM(a.zinus_sku) AS zinus_sku
                , IF(a.asin = 'B0B6FQZMJ4', '5', REGEXP_EXTRACT(LOWER(TRIM(inch_color)), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')) AS inch
                , TRIM(size) AS size
                --, if(a.financial_category = 'Foam Mattresses', 'Foam Mattress', a.financial_category) as category
                , CASE a.financial_category
				      WHEN 'Foam Mattresses'   THEN 'Foam Mattress'
					      WHEN 'Spring Mattresses' THEN 'Spring Mattress'
					      ELSE a.financial_category
					  END AS category 
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
, traffic_with_meta AS (
	SELECT
		a.RetailerSku AS asin 
		, FORMAT_DATE('%Y-%m', a.WeekEnding) as yr_month
		
		, ANY_VALUE(b.zinus_sku) AS zinus_sku   
		, MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')) as brand

        , SUM(a.OrganicTraffic) AS organic
        , SUM(a.PaidTraffic) AS paid
        , SUM(a.TotalTraffic) AS traffic
        , SUM(a.PaidAdSpend) AS ad_spend
        --, AVG(listprice.value_num) AS avg_list_price

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
		stck.atals_traffic_pdt_all a 
		JOIN with_meta b ON a.RetailerSku = b.asin
    WHERE
	    REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') IN ( 'FDW', 'EGOHOME', 'NOVILLA', 'ZINUS', 'MLILY' )
	    --AND SubCategory = 'Mattresses'
	    AND a.WeekEnding >= '2024-01-01'
	GROUP BY 1, 2
)
--SELECT DISTINCT category FROM traffic_with_meta
SELECT yr_month, brand 
	, sum(organic) 	AS organic
	, sum(paid) 	AS paid  
	, sum(traffic) 	AS traffic  
	, round(sum(ad_spend), 0) AS ad_spend
FROM traffic_with_meta
WHERE category = 'Foam Mattress' 	-- Foam Mattress만 추출  
GROUP BY 1,2
ORDER BY 1
;



--- [03. 두개 테이블 합치기] -------------------------------------------------------
CREATE OR REPLACE TABLE wook.tmp_stck_asp_traffic AS 
SELECT
    COALESCE(s.yr_month, t.yr_month) AS yr_month
    , COALESCE(s.brand, t.brand)     AS brand

    -- ASP 측 지표
    , s.sales
    , s.units
    , s.asp

    -- Traffic 측 지표
    , t.organic
    , t.paid
    , t.traffic
    , t.ad_spend

    -- 파생 지표
    , ROUND(SAFE_DIVIDE(s.units,    t.traffic) * 100, 2) AS cvr_pct       -- 전환율
    , ROUND(SAFE_DIVIDE(t.ad_spend, s.sales)   * 100, 2) AS tacos_pct     -- 광고 매출 효율
    , ROUND(SAFE_DIVIDE(t.ad_spend, t.paid),         2) AS cpc            -- 유료 트래픽 1건당 비용
    , ROUND(SAFE_DIVIDE(t.paid,     t.traffic) * 100, 2) AS paid_share    -- 유료 트래픽 비중
    , ROUND(SAFE_DIVIDE(s.sales,    t.traffic),       2) AS rev_per_visit -- 방문당 매출
FROM
    wook.tmp_stck_asp_with_meta     s
    FULL OUTER JOIN wook.tmp_stck_traffic_with_meta t
        ON  s.yr_month = t.yr_month
        AND s.brand    = t.brand
WHERE
    COALESCE(s.brand, t.brand) <> 'ALL'   -- 양쪽의 ALL 행 모두 제외
ORDER BY 
    yr_month, brand
;


--- [ 04. 집계 ] ----------------------------------

-- 년도 별
SELECT 
	cast(substr(yr_month, 1, 4) AS int64) AS yr 
	, sum(sales) AS sales 
	, sum(units) AS units 
	, round(safe_divide(sum(sales), sum(units)), 2) AS asp
	, sum(traffic) AS traffic 
	, round(safe_divide(sum(units), sum(traffic))*100, 2) AS cvr_pct 
	, round(safe_divide(sum(ad_spend), sum(sales))*100, 2) AS tacos_pct
	, sum(ad_spend) AS ad_spend
FROM 
	wook.tmp_stck_asp_traffic 
WHERE 
	brand = 'MLILY'
GROUP by 1
ORDER BY 1
;

-- YTD 1월-4월
SELECT
    CAST(SUBSTR(yr_month, 1, 4) AS INT64) AS yr
    , SUM(sales) AS sales
    , SUM(units) AS units
    , ROUND(SAFE_DIVIDE(SUM(sales),    SUM(units)),          2) AS asp
    , SUM(traffic) AS traffic
    , ROUND(SAFE_DIVIDE(SUM(units),    SUM(traffic)) * 100,  2) AS cvr_pct
    , ROUND(SAFE_DIVIDE(SUM(ad_spend), SUM(sales))   * 100,  2) AS tacos_pct
    , SUM(ad_spend) AS ad_spend
FROM
    wook.tmp_stck_asp_traffic
WHERE
    brand = 'MLILY'
    AND CAST(SUBSTR(yr_month, 6, 2) AS INT64) BETWEEN 1 AND 4
GROUP BY 1
ORDER BY 1
;

-- corr
SELECT
    SUBSTR(yr_month, 1, 4) AS yr
    , CORR(asp, cvr_pct) AS asp_cvr_corr
    , CORR(asp, units) AS asp_units_corr
    , CORR(ad_spend, sales) AS ads_sales_corr
    , COUNT(*)         AS n_months
FROM
    wook.tmp_stck_asp_traffic
WHERE
    brand = 'NOVILLA'
   -- AND yr_month BETWEEN '2025-01' AND '2026-4'
GROUP BY 1

UNION ALL 

SELECT
	'ALL' AS yr
	, CORR(asp, cvr_pct) AS asp_cvr_corr
    , CORR(asp, units) AS asp_units_corr
    , CORR(ad_spend, sales) AS ads_sales_corr
    , COUNT(*)         AS n_months
FROM 
	wook.tmp_stck_asp_traffic
WHERE 
	brand = 'NOVILLA'
	
ORDER BY 1 
;	
	
	
	
	
	
	
	
	
	
	
	
	
