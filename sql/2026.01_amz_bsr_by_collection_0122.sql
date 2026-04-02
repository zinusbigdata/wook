/*
 * BSR by Collection --> Mart 생성 SQL
 * input : rf_amz_bsr_all, rf_amz_bsr_hourly, amz_zinus_master_pdt_pi, global_sku_master
 * output : wook.mart_amz_bsr_by_collection
 * last updated : 1/23/2026
 */


-- 1. BSR fact table 만들기 
CREATE OR REPLACE TABLE wook.tmp_fact_amz_bsr_asin_brand AS
-- 24~25년 BSR data 뽑기 : 1,700,921 / 1,700,521개 / 962,978 rows
WITH cte_fct_rf_amz_basr AS (
	SELECT
    --fullCategory
    TRIM( 
      ARRAY_REVERSE(SPLIT(fullCategory, '>'))[OFFSET(0)] 
    ) AS bsr_ctgry
    , rank
    , asin
    , DATE(initialTime) AS bsr_date
    , FORMAT_DATE('%yY_%QQ', DATE(initialTime)) AS yr_qt
    , FORMAT_DATE('%Y', DATE(initialTime)) AS bsr_year
	FROM
	    rfapi.rf_amz_bsr_all
	    --rfapi.rf_amz_bsr_hourly
	WHERE
	    rank <= 100
	    AND DATE(initialTime) BETWEEN '2022-01-01' AND '2025-12-31'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY fullCategory, DATE (initialTime), rank ORDER BY initialTime DESC) = 1
)
-- PDP에서 asin과 brand 매핑 추출 --> 42,861 개 / 39,106
, cte_dim_pdt_asin_brand_map AS (
	SELECT
	  asin,
	  --UPPER(TRIM(brand)) AS brand
	  UPPER(TRIM(REGEXP_REPLACE(brand, r'[^[:print:]]', ''))) AS brand
	FROM dw.rf_amz_pdt_zns_comp_daily
	WHERE --DATE(crawlTime_utc) BETWEEN '2024-01-01' AND '2025-12-31'
	  --AND brand IS NOT NULL
	  brand IS NOT NULL AND UPPER(TRIM(brand)) <> 'NONE'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime_utc DESC) = 1
)
, cte_join_bsr_asin_brand AS (
	SELECT a.*
		, b.brand
	FROM cte_fct_rf_amz_basr a
	LEFT JOIN cte_dim_pdt_asin_brand_map b ON a.asin = b.asin 
) 
SELECT * FROM cte_join_bsr_asin_brand;

--select brand, count(*) from wook.tmp_fact_rf_amz_bsr_asin_brand 
--group by 1 order by 2 desc


-- 2. BSR fact에 dim 붙이기 
-- amz_bsr_zinus_asin_collection master 정리
-- 한팀장이 meta.amz_zinus_master_pdt_pi_add_new_col 새로 만듬. 
-- 기존 pi_master에 new_collection 붙여서...
CREATE OR REPLACE TABLE wook.mart_amz_bsr_by_zinus_col_cate AS
WITH cte_bsr_asin_zinus AS (
	SELECT DISTINCT asin from wook.tmp_fact_amz_bsr_asin_brand where brand='ZINUS'
	--> 883
) 
, cte_bsr_asin_zinus_col_cate AS (
	SELECT a.asin
		, b.new_collection 
		, b.financial_category
	FROM cte_bsr_asin_zinus a 
	LEFT JOIN  meta.amz_zinus_master_pdt_pi_add_new_col b on a.asin=b.asin
)
, cte_amz_bsr_zinus_asin_col_cate AS (
	SELECT a.*
		, b.new_collection
		, b.financial_category 
		, CASE WHEN a.bsr_ctgry = 'Mattresses' THEN 'Matt'
		 	   WHEN a.bsr_ctgry in ('Beds','Bed Frames','Box Springs','Mattress Toppers') THEN 'Non-Matt'
		 	   ELSE 'Others'
		  END AS ctgry_type
	FROM wook.tmp_fact_amz_bsr_asin_brand a
	LEFT JOIN cte_bsr_asin_zinus_col_cate b ON a.asin=b.asin
)

SELECT * 
	, IF(rank <= 20, TRUE, FALSE) AS is_top20
    , IF(rank BETWEEN 21 AND 100, TRUE, FALSE) AS is_top21_100
FROM cte_amz_bsr_zinus_asin_col_cate
WHERE ctgry_type <> 'Others';
--> 724,903

-- END --


/*
-- 3. Sales Fact Table 생성: Stackline Retail Sales 붙이기
--> 동적으로 top 20/100 변화 --> sales 집계는 어느 기준으로 ?? --> HOLD

--CREATE OR REPLACE TABLE wook.fct_amz_bsr_qt_sales AS
-- Matt
WITH cte_amz_bsr_qt_asins AS (
	SELECT ctgry_type, bsr_ctgry, financial_category, yr_qt, asin
	FROM wook.mart_amz_bsr_by_zinus_col_cate 
	WHERE ctgry_type='Matt' AND brand='ZINUS' AND is_top_20 is TRUE
	GROUP BY 1,2,3,4,5
	--> 859 
)
, cte_retail_sales_by_asin AS (
	SELECT
		FORMAT_DATE('%yY_%QQ', WeekEnding) AS yr_qt
		, RetailerSku AS asin
		, SUM(RetailSales) AS RetailSales
	FROM stck.atlas_sales_all 
	WHERE UPPER(TRIM(brand)) = 'ZINUS'
	GROUP BY 1,2
)
, cte_amz_bsr_qt_asins_sales AS (
	SELECT a.*
		, b.RetailSales
	FROM cte_amz_bsr_qt_asins a
	LEFT JOIN cte_retail_sales_by_asin b ON a.asin=b.asin AND a.yr_qt=b.yr_qt
)
, cte_amz_bsr_qt_asins_sales_mat AS (
	SELECT ctgry_type, bsr_ctgry
		, financial_category AS sub_ctgry
		, yr_qt
		, SUM(RetailSales)
	FROM cte_amz_bsr_qt_asins_sales
	GROUP BY 1,2,3,4
)
select * from cte_amz_bsr_qt_asins_sales_mat
order by 1,2,3,4

*/

--select distinct bsr_ctgry from wook.mart_amz_bsr_by_zinus_collection where ctgry_type='Others'
--select financial_category, count(distinct asin)  from wook.mart_amz_bsr_by_zinus_collection where ctgry_type='Matt' group by 1

/*
 *  Zinus Collections 개수, 점유율 분석하기
 *  Stackline Sales도 추가
 */


-- 1. Matt 부분 : Foam과 Spring으로 나눔
with cte_bsr_ctgry_cnt as (
	-- yr_qt별 top 20과 top 100 전체 count 계산 
	select ctgry_type, bsr_ctgry, yr_qt
		, sum(case when is_top20 then 1 else 0 end) as top20_cnt
		, sum(case when is_top21_100 then 1 else 0 end) as top21_100_cnt
		--, count(*) as top100_cnt
	from wook.mart_amz_bsr_by_zinus_col_cate
	group by 1,2,3
	--order by 2,1
)

, cte_mat_zinus_top20 as (
	-- quarter별 Zinus collectin 개수 (Top 20)
	-- select * from wook.mart_amz_bsr_by_zinus_col_cate where ctgry_type='Matt' and brand='ZINUS' and is_top20 is true
	--> 6,624 
	select 
		ctgry_type, bsr_ctgry, financial_category, yr_qt
		, count(*) as zinus_cnt
		, count(distinct asin) as zinus_asin_cnt
		, count(distinct new_collection) as znius_collection_cnt 
		, ARRAY_TO_STRING(ARRAY_AGG(DISTINCT new_collection ORDER BY new_collection), ',') as new_collection_list
	from wook.mart_amz_bsr_by_zinus_col_cate 
	where ctgry_type='Matt' and brand='ZINUS' and is_top20 is true 
	group by 1,2,3,4 	
	--order by 1,2,3,4
)
, cte_mat_zinus_top21_100 as (
	-- quarter별 Zinus collectin 개수 (Top 21 ~ 100)
	-- select * from wook.mart_amz_bsr_by_zinus_col_cate where ctgry_type='Matt' and brand='ZINUS' and is_top21_100 is true
	--> 8,703 
	select 
		ctgry_type, bsr_ctgry, financial_category, yr_qt
		, count(*) as zinus_cnt
		, count(distinct asin) as zinus_asin_cnt
		, count(distinct new_collection) as znius_collection_cnt 
		, ARRAY_TO_STRING(ARRAY_AGG(DISTINCT new_collection ORDER BY new_collection), ',') as new_collection_list
	from wook.mart_amz_bsr_by_zinus_col_cate 
	where ctgry_type='Matt' and brand='ZINUS' and is_top21_100 is true 
	group by 1,2,3,4 	
)	
--, cte_add_mat_zinus_top_share as (
select a.*
	, b.top20_cnt as tt_cnt
	, SAFE_DIVIDE(a.zinus_cnt, b.top20_cnt) as zinus_top20_share
	, 'Top 20' as bsr_range
from cte_mat_zinus_top20 a
left join cte_bsr_ctgry_cnt b on a.ctgry_type=b.ctgry_type and a.bsr_ctgry=b.bsr_ctgry and a.yr_qt=b.yr_qt

union all

select a.*
	, b.top21_100_cnt as tt_cnt 
	, SAFE_DIVIDE(a.zinus_cnt, b.top21_100_cnt) as zinus_top21_100_share
	, 'Top 21-100' as bsr_range
from cte_mat_zinus_top21_100 a
left join cte_bsr_ctgry_cnt b on a.ctgry_type=b.ctgry_type and a.bsr_ctgry=b.bsr_ctgry and a.yr_qt=b.yr_qt



-- 2. Non-Matt 부분 : bsr_ctgry를 그대로 사용

with cte_bsr_ctgry_cnt as (
	-- quarter별 Zinus Collection 점유율 (Top 20)
	select ctgry_type, bsr_ctgry, yr_qt
		, sum(case when is_top20 then 1 else 0 end) as top20_cnt
		, sum(case when is_top21_100 then 1 else 0 end) as top21_100_cnt
		--, count(*) as top100_cnt
	from wook.mart_amz_bsr_by_zinus_col_cate
	group by 1,2,3
	--order by 2,1
)
, cte_non_mat_zinus_top20 as (
	-- quarter별 Zinus collectin 개수 (Top 20)
	-- select * from wook.mart_amz_bsr_by_zinus_col_cate where ctgry_type='Non-Matt' and brand='ZINUS' and is_top20 is true
	--> 25,481 
	select 
		ctgry_type, bsr_ctgry, yr_qt
		, count(*) as zinus_cnt
		, count(distinct asin) as zinus_asin_cnt
		, count(distinct new_collection) as znius_collection_cnt 
	from wook.mart_amz_bsr_by_zinus_col_cate 
	where ctgry_type='Non-Matt' and brand='ZINUS' and is_top20 is true 
	group by 1,2,3	
	--order by 1,2,3,4
)
, cte_non_mat_zinus_top21_100 as (
	-- quarter별 Zinus collectin 개수 (Top 21 ~ 100)
	-- select * from wook.mart_amz_bsr_by_zinus_col_cate where ctgry_type='Non-Matt' and brand='ZINUS' and is_top21_100 is true
	--> 38,509 
	select 
		ctgry_type, bsr_ctgry, yr_qt
		, count(*) as zinus_cnt
		, count(distinct asin) as zinus_asin_cnt
		, count(distinct new_collection) as znius_collection_cnt 
	from wook.mart_amz_bsr_by_zinus_col_cate 
	where ctgry_type='Non-Matt' and brand='ZINUS' and is_top21_100 is true 
	group by 1,2,3	
)	
--, cte_add_mat_zinus_top_share as (
select a.*
	, b.top20_cnt as tt_cnt
	, SAFE_DIVIDE(a.zinus_cnt, b.top20_cnt) as zinus_top20_share
	, 'Top 20' as bsr_range
from cte_non_mat_zinus_top20 a
left join cte_bsr_ctgry_cnt b on a.ctgry_type=b.ctgry_type and a.bsr_ctgry=b.bsr_ctgry and a.yr_qt=b.yr_qt

union all

select a.*
	, b.top21_100_cnt as tt_cnt
	, SAFE_DIVIDE(a.zinus_cnt, b.top21_100_cnt) as zinus_top21_100_share
	, 'Top 21-100' as bsr_range
from cte_non_mat_zinus_top21_100 a
left join cte_bsr_ctgry_cnt b on a.ctgry_type=b.ctgry_type and a.bsr_ctgry=b.bsr_ctgry and a.yr_qt=b.yr_qt


-- END --


/*
 *  추가 분석 하기 : Collection별 qt별 노출 횟수 변화
 */ 

-- AMZ Mattress의 Collection별 분기별 BSR 노출 Count ?

select financial_category, new_collection, yr_qt
	, count(*) as bsr_cnt
	, 'Top 20' as bsr_level
from wook.mart_amz_bsr_by_zinus_col_cate
where ctgry_type='Matt' and brand='ZINUS' and is_top20 is true 
group by 1,2,3
--order by 1,2,3;

union all 

select financial_category, new_collection, yr_qt
	, count(*) as bsr_cnt
	, 'Top 21-100' as bsr_level
from wook.mart_amz_bsr_by_zinus_col_cate
where ctgry_type='Matt' and brand='ZINUS' and is_top21_100 is true 
group by 1,2,3
--order by 1,2,3;



--- 추가 분석 --

select a.new_collection
	, ARRAY_TO_STRING(ARRAY_AGG(DISTINCT b.product_description ORDER BY b.product_description), ',') AS product_description_list 
	, ARRAY_TO_STRING(ARRAY_AGG(DISTINCT b.collection ORDER BY b.collection), ',') AS collection_description_list 
from (
	select DISTINCT new_collection 
	from wook.mart_amz_bsr_by_zinus_col_cate 
	where ctgry_type='Matt' and financial_category = 'Spring Mattresses' and brand='ZINUS'
) a
left join meta.amz_zinus_master_pdt_pi_add_new_col b 
	on a.new_collection = b.new_collection 
group by 1
order by 1 


select DISTINCT new_collection 
from wook.mart_amz_bsr_by_zinus_col_cate 
where ctgry_type='Matt' and brand='ZINUS' and is_top21_100 is true 	
	



-- 데이터 검증
select *
from wook.mart_amz_bsr_by_zinus_col_cate
where ctgry_type='Matt' and brand='ZINUS' and is_top20 is true 
and yr_qt='22Y_1Q' and new_collection='FMS'

-- 


