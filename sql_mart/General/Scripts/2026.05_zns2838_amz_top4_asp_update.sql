/*
 * ZNS-2838 : Amazon Top 4 브랜드의 ASP 보고 후 업데이트 버전 -> 월 기준
 */


-- [01. Amazon Mattress ASP - Foam Mattress Only] ----------------------------------------------------------------------
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

UNION ALL 

SELECT yr_month
	, 'ALL'			AS brand
	, ROUND(SUM(sales), 0) 	AS sales
	, SUM(units)	AS units
	, ROUND(SAFE_DIVIDE(SUM(sales), SUM(units)), 1) AS asp
FROM stck_with_meta  
WHERE category = 'Foam Mattress'	-- Foam Mattress만 추출  
GROUP BY 1

ORDER BY 1,2
;




-- [02.Amazon Mattress Top Brands] ----------------------------------------------------------------------
WITH monthly_matt_brand AS (
	SELECT
	    --FORMAT_DATE('%Y-%m', WeekEnding) as yr_month
		-- RetailerSku AS asin
		c.yr_month AS yr_month
	    , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]','') as brand
	
	    , ROUND(SUM(RetailSales), 0) AS sales
	    , SUM(UnitsSold) AS units
	    , ROUND(AVG(RetailPrice), 1) AS avg_retail_price   
	FROM 
		stck.atlas_sales_all a
	    LEFT JOIN meta.wk_calendar_new c ON a.WeekEnding BETWEEN c.start_date AND c.end_date   
	WHERE 
		WeekEnding >= '2024-01-01' AND SubCategory='Mattresses'  
	GROUP BY 1,2
)
, with_share AS (  
	SELECT *
		, ROUND(SAFE_DIVIDE(sales, units), 1) AS asp
		, ROUND(sales / NULLIF(SUM(sales) OVER (PARTITION BY yr_month), 0) * 100, 1) AS share_pct
		, RANK() OVER (PARTITION BY yr_month ORDER BY sales DESC) AS sales_rank
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
	, NULL 			AS avg_retail_price
	, ROUND(SAFE_DIVIDE(SUM(sales), SUM(units)), 1) AS asp
	, NULL			AS share_pct 
	, NULL			AS sales_rank
FROM with_share  
GROUP BY 1

ORDER BY yr_month desc, sales_rank
;



-- [03. Monthly Top 20 Brands] -------------------------------------------------------------
WITH monthly_matt_brand AS (
	SELECT
	    FORMAT_DATE('%Y-%m', WeekEnding) as yr_month
		-- RetailerSku AS asin
	    , REGEXP_REPLACE(UPPER(Brand), r'[^[:print:]]','') as brand
	
	    , ROUND(SUM(RetailSales), 0) AS sales
	    , SUM(UnitsSold) AS units
	    , ROUND(AVG(RetailPrice), 1) AS avg_retail_price   
	FROM
	    stck.atlas_sales_all
	WHERE 
		WeekEnding >= '2024-01-01' AND SubCategory='Mattresses'  
	GROUP BY 1,2
)
, with_share AS (  
	SELECT *
		, ROUND(SAFE_DIVIDE(sales, units), 1) AS asp
		, ROUND(sales / NULLIF(SUM(sales) OVER (PARTITION BY yr_month), 0) * 100, 1) AS share_pct
		, RANK() OVER (PARTITION BY yr_month ORDER BY sales DESC) AS sales_rank
	FROM monthly_matt_brand 
)
SELECT *
FROM with_share 
WHERE sales_rank <= 20
ORDER BY yr_month desc, sales_rank
;





