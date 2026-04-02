/*
 * Small-Box 출시 이후 Review 평점 변화 --> avg rank 추가하기 (Jade요구사항)
 * Written review & PDP review (대표 Asin)  
 * 분석 Mart : wook.review_sales_by_collection_category_new_collection 
 * Source table : tmp1.zns_f_cate_m_col_mst, vc.amz_vc_sales_monthly, rf_amz_pdt_zns_comp_daily, meta.amz_zns_cat_col_mst
 */

WITH cte_pdp_rank_day AS (
	SELECT asin
	  , DATE(crawlTime_utc) as date
	  --UPPER(TRIM(brand)) AS brand
	  , UPPER(TRIM(REGEXP_REPLACE(brand, r'[^[:print:]]', ''))) AS brand
	  , (SELECT MIN(cast(val as INT64)) FROM UNNEST([salesrank1, salesrank2, salesrank3, salesrank4]) AS val) AS least_sales_rank
	FROM dw.rf_amz_pdt_zns_comp_daily
	WHERE DATE(crawlTime_utc) BETWEEN '2024-01-01' AND '2025-12-31' 
		AND UPPER(TRIM(REGEXP_REPLACE(brand, r'[^[:print:]]', ''))) = 'ZINUS'
	  --brand IS NOT NULL AND UPPER(TRIM(brand)) <> 'NONE'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY asin, DATE(crawlTime_utc) ORDER BY crawlTime_utc DESC) = 1
)
--select distinct asin from cte_pdp_rank_day
, cte_pdp_rank_col AS (
	SELECT a.*
		, FORMAT_DATE('%yY_%QQ', a.date) AS yr_qt
	    , b.financial_category
	    , b.origin_collection
	    , b.main_collection
	    , b.new_collection
	--FROM (select distinct asin from cte_pdp_rank_day) a
	FROM cte_pdp_rank_day a
	LEFT JOIN meta.amz_zns_cat_col_mst b ON a.asin = b.asin
)
--select * from cte_tmp1 where new_collection is null 
, cte_pdp_rank_col_qt AS (
	SELECT --asin
		yr_qt
		, financial_category
		, new_collection
		, origin_collection
		, min(least_sales_rank) as min_rank
		, max(least_sales_rank) as max_rank
		, avg(least_sales_rank) as avg_rank
		, APPROX_QUANTILES(least_sales_rank, 2)[OFFSET(1)] AS median_rank
		, min(date) as start_date
		, max(date) as end_date
		, count(least_sales_rank) as cnt_rank
		, count(distinct asin) as cnt_asin
	FROM cte_pdp_rank_col
	WHERE origin_collection LIKE '%WonderBox%'
	GROUP BY 1,2,3,4
)
, cte_review_wonderbox AS (   	--> 27,726 --> 9448
	SELECT 
		a.* 
		, COALESCE(b.financial_category, a.financial_category, NULL) AS zinus_category 		
		, COALESCE(b.origin_collection, a.collection, NULL) AS origin_collection 
		--, b.origin_collection
		--, b.main_collection
		, b.new_collection
	FROM dw.amz_us_zinus_rvw a
	LEFT JOIN tmp1.zns_f_cate_m_col_mst b ON a.asin = b.asin
	WHERE review_date BETWEEN '2024-01-01' AND '2025-12-31'	
		AND b.origin_collection LIKE '%WonderBox%'
)
--select distinct asin from cte_review_wonderbox   
, cte_review_wonderbox_qt AS (
	SELECT  
		--CAST(FORMAT_DATE('%Y%m', review_date) AS INT64) AS yr_month
		FORMAT_DATE('%yY_%QQ', review_date) AS yr_qt
		, zinus_category
		, new_collection
		, origin_collection 
		, SUM(IF(rating < 3, 1, 0)) AS written_12_cnt
	    , COUNT(1) AS written_all_cnt
	    , SUM(IF(rating < 3, 1, 0)) / COUNT(1) AS written_12_ratio
	    , SUM(IF(rating = 1, 1, 0)) AS written_1_cnt
	    , SUM(IF(rating = 2, 1, 0)) AS written_2_cnt
	    , SUM(IF(rating = 3, 1, 0)) AS written_3_cnt
	    , SUM(IF(rating = 4, 1, 0)) AS written_4_cnt
	    , SUM(IF(rating = 5, 1, 0)) AS written_5_cnt
	    , (5*SUM(IF(rating = 5, 1, 0)) + 4*SUM(IF(rating = 4, 1, 0)) + 3*SUM(IF(rating = 3, 1, 0)) + 2*SUM(IF(rating = 2, 1, 0)) + 1*SUM(IF(rating = 1, 1, 0))) 
	    /COUNT(1) AS avg_written_rating
	FROM cte_review_wonderbox   
	GROUP BY 1,2,3,4
)
, cte_vc_sales AS (				--> 26582 
	SELECT a.*
		, FORMAT_DATE('%yY_%QQ', a.date) AS yr_qt
	    , b.financial_category
	    , b.origin_collection
	    , b.main_collection
	    , b.new_collection
	FROM vc.amz_vc_sales_monthly a
	LEFT JOIN tmp1.zns_f_cate_m_col_mst b ON a.asin = b.asin
	WHERE date BETWEEN '2024-01-01' AND '2025-12-31' 
)
, cte_vs_sales_qt AS (
	SELECT yr_qt
		, financial_category 
		, new_collection
		, origin_collection 		           
		, SUM(ordered_revenue) AS sales_amt
        , SUM(ordered_units) AS sales_qty
	FROM cte_vc_sales 
	GROUP BY 1,2,3,4
)
--select * from cte_vs_sales_qt
SELECT 
	a.*
	, b.sales_amt 
	, b.sales_qty 
	, c.avg_rank
	, c.median_rank
FROM cte_review_wonderbox_qt a
LEFT JOIN cte_vs_sales_qt b
	ON a.yr_qt = b.yr_qt AND a.zinus_category = b.financial_category 
		AND a.new_collection = b.new_collection AND a.origin_collection = b.origin_collection 
LEFT JOIN cte_pdp_rank_col_qt c
	ON a.yr_qt = c.yr_qt AND a.zinus_category = c.financial_category 
		AND a.new_collection = c.new_collection AND a.origin_collection = c.origin_collection  

-- END --
		

	
------------- test --------------
		
WITH cte_review_wonderbox AS (   	--> 27,726 --> 9448
	SELECT 
		a.* 
		, COALESCE(b.financial_category, a.financial_category, NULL) AS zinus_category 		
		, COALESCE(b.origin_collection, a.collection, NULL) AS origin_collection 
		--, b.origin_collection
		--, b.main_collection
		, b.new_collection
	FROM dw.amz_us_zinus_rvw a
	LEFT JOIN tmp1.zns_f_cate_m_col_mst b ON a.asin = b.asin
	WHERE review_date BETWEEN '2024-01-01' AND '2025-12-31'	
		AND b.origin_collection LIKE '%WonderBox%'
)		
select zinus_category, new_collection, origin_collection 
	, count(distinct asin)
from cte_review_wonderbox
group by 1,2,3
		
		
WITH cte_vc_sales AS (				--> 26582 
	SELECT a.*
		, FORMAT_DATE('%yY_%QQ', a.date) AS yr_qt
	    , b.financial_category
	    , b.origin_collection
	    , b.main_collection
	    , b.new_collection
	FROM vc.amz_vc_sales_monthly a
	LEFT JOIN tmp1.zns_f_cate_m_col_mst b ON a.asin = b.asin
	WHERE date BETWEEN '2024-01-01' AND '2025-12-31' 
)
select origin_collection, SUM(ordered_units) from cte_vc_sales
where financial_category = 'Foam Mattresses' and new_collection = 'FGM' and yr_qt = '25Y_4Q'
group by 1		
		
