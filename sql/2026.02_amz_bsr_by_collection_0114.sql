/*
 * BSR by Collection --> Mart 생성 SQL
 * input : rf_amz_bsr_hourly, amz_zinus_master_pdt_pi, global_sku_master
 * output : wook.mart_amz_bsr_by_collection
 * last updated : 1/20/2026
 */

CREATE OR REPLACE TABLE wook.tmp_fact_rf_amz_bsr_asin_brand AS
-- 1. 24~25년 BSR data 뽑기 : 962,978 rows
WITH fct_rf_amz_basr AS (
	SELECT
    --fullCategory
    TRIM( 
      ARRAY_REVERSE(SPLIT(fullCategory, '>'))[OFFSET(0)] 
    ) AS bsr_ctgry
    , bestsellers_rank AS rank
    , bestsellers_asin AS asin
    , DATE(initialTime) AS bsr_date
    , FORMAT_DATE('%yY_%QQ', DATE(initialTime)) AS yr_qt
    , FORMAT_DATE('%Y', DATE(initialTime)) AS bsr_year
	FROM
	    rfapi.rf_amz_bsr_hourly
	WHERE
	    bestsellers_rank <= 100
	    AND DATE(initialTime) BETWEEN '2024-01-01' AND '2025-12-31'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY fullCategory, DATE (initialTime), bestsellers_rank ORDER BY initialTime DESC) = 1
)
-- 2. PDP에서 asin과 brand 매핑 추출 --> 39,106
, dim_pdt_asin_brand_map AS (
	SELECT
	  asin,
	  --UPPER(TRIM(brand)) AS brand
	  UPPER(TRIM(REGEXP_REPLACE(brand, r'[^[:print:]]', ''))) AS brand
	FROM dw.rf_amz_pdt_zns_comp_daily
	WHERE DATE(crawlTime_utc) BETWEEN '2024-01-01' AND '2025-12-31'
	  AND brand IS NOT NULL
	  AND UPPER(TRIM(brand)) <> 'NONE'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY asin ORDER BY crawlTime_utc DESC) = 1
)
, cte_add_bsr_asin_brand AS (
	SELECT a.*
		, b.brand
	FROM fct_rf_amz_basr a
	LEFT JOIN dim_pdt_asin_brand_map b ON a.asin = b.asin 
) 
SELECT * FROM cte_add_bsr_asin_brand;

--select brand, count(*) from wook.tmp_fact_rf_amz_bsr_asin_brand 
--group by 1 order by 2 desc


-- 2. amz_bsr_zinus_asin_collection master 정리
CREATE OR REPLACE TABLE wook.tmp_dim_zinus_asin_sku_collection AS
WITH dim_amz_master AS ( 
	SELECT  asin, zinus_sku, collection FROM meta.amz_zinus_master_pdt_pi GROUP BY 1,2,3
	-- 2685
)
, dim_global_sku_master AS (
	-- 전체 map은 11,422 개, US거만 뽑아서 중복제거 : 4814 개
	--SELECT DISTINCT zinus_sku, collection_name FROM wook.global_sku_master WHERE sales_country='US' GROUP BY 1,2 
	SELECT zinus_sku, collection_name FROM wook.global_sku_master GROUP BY 1,2
)
, cte_asin_sku_collection AS ( 
	SELECT a.asin, a.zinus_sku, a.collection, b.collection_name
	FROM dim_amz_master a
	LEFT JOIN dim_global_sku_master b ON a.zinus_sku = b.zinus_sku 
--	LEFT JOIN dim_bsr_asin_zinus c ON a.asin = c.asin 
)
--select asin, count(*) from dim_asin_sku_collection group by 1 having count(*) > 1
--select * from dim_asin_sku_collection where asin in ('B006L9U4PK', 'B006L9WNL8', 'B006L9VANK', 'B006L9QN4G', 'B004TMURIK', 'B004TMV1IA', 'B004TN0IVK', 'B004TMI746')
, dim_bsr_asin_zinus AS (
	SELECT DISTINCT asin from wook.tmp_fact_rf_amz_bsr_asin_brand where brand='ZINUS'
)
, cte_add_bsr_asin_zinus_collection AS (
	SELECT a.asin, b.zinus_sku, b.collection, b.collection_name
	FROM dim_bsr_asin_zinus a
	LEFT JOIN cte_asin_sku_collection b ON a.asin = b.asin  
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY a.asin
		ORDER BY IF(b.collection_name = 'FMS', 1, 2), b.collection_name
	) = 1
)
--select * from cte_add_bsr_asin_zinus_collection;
, cte_add_missing_collection AS (
	SELECT 
		a.* 
		, COALESCE(b.glb_collection, a.collection_name) AS collection_adj
	FROM cte_add_bsr_asin_zinus_collection a
	LEFT JOIN (
                SELECT '2in MGT w WonderBox' AS pi_collection,	'MGT' AS glb_collection
                UNION ALL
                SELECT '3in MGT w WonderBox'	,	'MGT'
                UNION ALL
                SELECT 'Aidan Sling Chair, 2PK'	,	'Aidan'
                UNION ALL
                SELECT 'BIFD 5in'	,	'Cool Grey BIFD'
                UNION ALL
                SELECT 'Justina, 16in'	,	'Justina'
                UNION ALL
                SELECT 'Quinn Sleeper Sofa'	,	'Quinn'
                UNION ALL
                SELECT 'SB w Bamboo Slats'	,	'Bamboo Slat SB'
    ) b ON a.collection = b.pi_collection	
)  
SELECT * FROM cte_add_missing_collection;

-- 3. join
CREATE OR REPLACE TABLE wook.mart_amz_bsr_by_collection AS
SELECT a.*
	, b.collection_adj
	, CASE WHEN a.bsr_ctgry = 'Mattresses' THEN 'Matt'
	  	   ELSE 'Non-Matt'
	  END AS ctgry_type
FROM wook.tmp_fact_rf_amz_bsr_asin_brand a
LEFT JOIN wook.tmp_dim_zinus_asin_sku_collection b ON a.asin=b.asin ;


-- END --



/*
 *  Zinus Collections 개수, 점유율 분석하기
 */

-- 1. BSR top 20 
with cte_top20_tmp1 as (
	-- quarter별 Zinus collectin 개수 (Top 20)
	select ctgry_type, yr_qt
		, count(*) as zinus_cnt
		, count(distinct asin) as zinus_asin_cnt, 
		count(distinct collection_adj) as znius_collection_cnt 
	from wook.mart_amz_bsr_by_collection
	where brand='ZINUS' and rank <= 20 
	group by 1,2 
	--order by 2,1
)
, cte_top20_tmp2 as (
	-- quarter별 Zinus Collection 점유율 (Top 20)
	select ctgry_type, yr_qt, count(*) as qt_cnt
	from wook.mart_amz_bsr_by_collection where rank <= 20
	group by 1,2
	--order by 2,1
)
, cte_top20_tmp3 as (
	select a.*
		, b.qt_cnt 	--, c.zinus_cnt
		, SAFE_DIVIDE(a.zinus_cnt, b.qt_cnt) as zinus_share
	from cte_top20_tmp1 a
	left join cte_top20_tmp2 b on a.ctgry_type=b.ctgry_type and a.yr_qt=b.yr_qt
	--left join cte_tmp3 c on a.ctgry_type=c.ctgry_type and a.yr_qt=c.yr_qt
	order by 1,2
)
--select * from cte_top20_tmp3 
-- 2. BSR top 20 ~ 100 
, cte_top100_tmp4 as (
	-- quarter별 Zinus collectin 개수 (Top 20)
	select ctgry_type, yr_qt
		, count(*) as zinus_cnt
		, count(distinct asin) as zinus_asin_cnt, 
		count(distinct collection_adj) as zinus_collection_cnt 
	from wook.mart_amz_bsr_by_collection
	where brand='ZINUS' and rank between 21 and 100
	group by 1,2 
	--order by 2,1
)
, cte_top100_tmp5 as (
	-- quarter별 Zinus Collection 점유율 (Top 20)
	select ctgry_type, yr_qt, count(*) as qt_cnt
	from wook.mart_amz_bsr_by_collection where rank between 21 and 100
	group by 1,2
	--order by 2,1
)
, cte_top100_tmp6 as (
	select c.*
		, d.qt_cnt	--, c.zinus_cnt
		, SAFE_DIVIDE(c.zinus_cnt, d.qt_cnt) as zinus_share
	from cte_top100_tmp4 c
	left join cte_top100_tmp5 d on c.ctgry_type=d.ctgry_type and c.yr_qt=d.yr_qt
	order by 1, 2
)
select 'Top 20' as bsr_range
	, * from cte_top20_tmp3
union all
select 'Top 21~100' as bsr_range
	, * from cte_top100_tmp6
;


-- Zinus Collection List in Y25_Q4

select collection_adj, count(distinct asin) as asin_cnt
from wook.mart_amz_bsr_by_collection
where brand='ZINUS' and rank <= 20 and ctgry_type='Matt'
group by 1



