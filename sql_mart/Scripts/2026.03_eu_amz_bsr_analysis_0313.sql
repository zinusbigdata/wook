/*
 * EU BSR 분석 : 2025년 10월 이후 데이터 업데이트 후에 다시 실행 
 */

-- 1. Zinus 점유율 검증

with cte_tmp1 as (
	select *,
		extract(year from bsr_date) as yr
	from  wook.amz_eu_bsr_shr_daily_acc2
	where bsr_date >= '2023-01-01' and bsr_rank_range='Top 50'
)
--select distinct brand from cte_tmp1 
select country, bsr_ctgry, yr,
	count(*),
	sum(case when upper(brand_raw)='ZINUS' then 1 else 0 end) as zinus_row_cnt,
	sum(case when upper(brand_raw)='ZINUS' then 1 else 0 end) / count(*) as zinus_ratio
from cte_tmp1 
group by 1,2,3
;







select * from wook.amz_eu_bsr_shr_daily_acc2
where bsr_date >= '2026-01-01' and bsr_rank_range='Top 50' 


select count(*) from wook.amz_eu_bsr_shr_daily_acc2
where bsr_date >= '2023-01-01' and bsr_rank_range='Top 50' 
	and country ='UK' and bsr_ctgry in ('Beds, Frames & Bases', 'Mattresses')





---






--CREATE OR REPLACE TABLE tmp1.amz_eu_bsr_brand_list2 AS
WITH TMP1 AS (
    SELECT
        country
        , bsr_ctgry_label
        , brand
        , COUNT(asin) AS cnt
    FROM tmp1.amz_eu_bsr_shr_daily_acc
    WHERE
        bsr_date >= '2023-01-01'
        AND bsr_rank_range = 'Top 50'
    GROUP BY 1, 2, 3
)
SELECT
    country
    , bsr_ctgry_label
    , brand
FROM
    TMP1
QUALIFY
    RANK() OVER (PARTITION BY country, bsr_ctgry_label ORDER BY cnt DESC ) <= 10
;
