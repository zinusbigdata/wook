/*
 * Amazon Traffic 분석
 */

-- [01. stackline pdp traffice] ------------------------------------------------
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




-- # test #----------------------------------------
SELECT 
	--FORMAT_DATE('%Y-%m', WeekEnding) as yr_month
	--, RetailerSku AS asin,
	REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]','') as brand,
	--, sum(TotalTraffic) AS traffic
	--, sum(PaidAdSpend) AS ad_spend
	count(DISTINCT RetailerSku)
FROM 
	stck.atals_traffic_pdt_all a 
WHERE 	
	REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') IN ( 'FDW', 'EGOHOME', 'NOVILLA', 'ZINUS', 'MLILY')
	AND SubCategory = 'Mattresses'
	AND WeekEnding >= '2024-01-01'
GROUP BY 1





-- [02. VC glance views] -------------------------------------------------------

WITH weekly_agg AS (
	SELECT
		format_date('%G-%V', date) AS yr_wk,
		a.asin,
		sum(a.glance_views) AS gv,
		any_value(b.financial_category) AS category,
		any_value(b.new_collection) AS collection
	FROM vc.vc_traffic_daily a
		JOIN meta.amz_zinus_master_pdt_pi_add_new_col b
			ON a.asin=b.asin 
	GROUP BY 1,2
)
SELECT 
	yr_wk, collection, 
	sum(gv) AS gv
FROM
	weekly_agg 
GROUP BY 1,2
ORDER BY 1 DESC, 3 DESC  
;	