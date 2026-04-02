/***********************************************
 * Amazon Mattress Price 분석
 * tables : dw.amz_bsr_matt_top10_price
 ***********************************************/



-- 2026.03.23

select min(bsr_date), max(bsr_date) from dw.amz_bsr_matt_top10_price

select distinct brand from dw.amz_bsr_matt_top10_price



-- 2026.03.20 : price index 추이 , 월별
-- 경쟁사 4개와 가격 비교 : ZINUS, NOVILLA, EGOHOME, FDW



select brand, count(distinct asin) from dw.amz_bsr_matt_top10_price
where brand in ('ZINUS','NOVILLA','EGOHOME','FDW')
group by 1


select final_inch, final_size, count(*) from dw.amz_bsr_matt_top10_price
where bsr_date >= '2026-01-01' and brand='ZINUS'
group by 1,2 

select brand,
	FORMAT_DATE('%Y-%m', DATE(bsr_date)) AS yr_month,
	avg(CAST(buybox_price AS FLOAT64)) as bb_price,
	avg(CAST(list_price AS FLOAT64)) as list_pirce
from dw.amz_bsr_matt_top10_price
where brand in ('ZINUS','NOVILLA','EGOHOME','FDW')
group by 1,2
order by 1



-- 2026.03.18_2026년 경쟁사와 Zinus 가격 비교 
-- 2026.03.19 전체 평균 구하기
-- price index 추가
WITH cte_tmp1 AS (
	SELECT 
	    final_inch,
	    final_size,
	    COUNT(*) AS cnt,
	    COUNT(DISTINCT asin) as asins,
	    
	    -- 전체 평균 
	    AVG(CAST(buybox_price AS FLOAT64)) AS avg_buybox_price,
	    --PERCENTILE_CONT(CAST(buybox_price AS FLOAT64), 0.5)  AS median_buybox_price,
	    APPROX_QUANTILES(CAST(buybox_price AS FLOAT64), 2)[OFFSET(1)] AS median_price,
	    AVG(CAST(list_price AS FLOAT64)) AS avg_list_price,
	    
	    -- ZINUS 만
	    COUNT(CASE WHEN brand = 'ZINUS' THEN 1 END) AS zinus_cnt,
	    COUNT(DISTINCT CASE WHEN brand = 'ZINUS' THEN asin END) AS zinus_asins,
	    
	    AVG(CASE WHEN brand = 'ZINUS' THEN CAST(buybox_price AS FLOAT64) END) AS zinus_buybox_price,
	    APPROX_QUANTILES(
		  IF(brand = 'ZINUS', CAST(buybox_price AS FLOAT64), NULL),
		  2
		)[OFFSET(1)] AS zinus_median_price,
	    AVG(CASE WHEN brand = 'ZINUS' THEN CAST(list_price AS FLOAT64) END) AS zinus_list_price
	FROM dw.amz_bsr_matt_top10_price
	WHERE final_inch in (5,6,8,10,12,14) 
		and final_size in ('Twin', 'Twin XL', 'Full', 'Queen', 'King', 'Cal King')
		and bsr_date >= '2026-01-01'
		--and brand != 'ZINUS'
		--and brand in ('EGOHOME', 'NOVILLA', 'FDW', 'MLILY')
	GROUP BY 1,2
	--ORDER BY 1,2
)
, cte_tmp2 as (
SELECT *, 
	avg_list_price - avg_buybox_price AS avg_discount,
	(avg_list_price - avg_buybox_price) / avg_list_price AS avg_discount_ratio,
	zinus_list_price - zinus_buybox_price AS zinus_discount,	
	(zinus_list_price - zinus_buybox_price) / zinus_list_price AS zinus_discount_ratio,
	zinus_buybox_price - avg_buybox_price as diff_price,
	zinus_buybox_price / avg_buybox_price as price_index
FROM cte_tmp1 
order by 1,2
)
select final_inch, final_size, 
	avg_discount_ratio,
	zinus_discount_ratio 
	--zinus_discount_ratio - avg_discount_ratio as diff_discount_ratio,
	--zinus_discount_ratio / avg_discount_ratio as discount_index
from cte_tmp2 
--select avg(avg_buybox_price) as avg_buybox_price,
;



-- 01. 경쟁사와 Zinus 가격 비교 

SELECT 
    final_inch,
    final_size,
    COUNT(*) AS cnt,
    COUNT(DISTINCT asin) as asins,
    AVG(CAST(buybox_price AS FLOAT64)) AS avg_buybox_price,
    AVG(CAST(list_price AS FLOAT64)) AS avg_list_price,
    AVG(CAST(list_price AS FLOAT64)) - AVG(CAST(buybox_price AS FLOAT64)) AS diff,
    'COMPETITORS' as type
FROM dw.amz_bsr_matt_top10_price
WHERE final_inch in (5,6,8,10,12,14) 
	and final_size in ('Twin', 'Twin XL', 'Full', 'Queen', 'King', 'Cal King')
	--and brand in ('EGOHOME', 'NOVILLA', 'FDW', 'MLILY')
GROUP BY 1,2
--ORDER BY 1,2

UNION ALL 

SELECT 
    final_inch,
    final_size,
    COUNT(*) AS cnt,
    COUNT(DISTINCT asin) as asins,
    AVG(CAST(buybox_price AS FLOAT64)) AS avg_buybox_price,
    AVG(CAST(list_price AS FLOAT64)) AS avg_list_price,
    AVG(CAST(list_price AS FLOAT64)) - AVG(CAST(buybox_price AS FLOAT64)) AS diff,
    'ZINUS' as type 
FROM dw.amz_bsr_matt_top10_price
WHERE final_inch in (5,6,8,10,12,14) 
	and final_size in ('Twin', 'Twin XL', 'Full', 'Queen', 'King', 'Cal King')
	and brand = 'ZINUS'
GROUP BY 1,2

ORDER BY 1,2;



-- 02. Inch별 평균, 중위수 
SELECT
    CAST(final_inch AS INT64) AS final_inch,
    count(*),
    count(distinct asin),
    AVG(CAST(buybox_price AS FLOAT64)) AS avg_price,
    APPROX_QUANTILES(CAST(buybox_price AS FLOAT64), 2)[OFFSET(1)] AS median_price
FROM dw.amz_bsr_matt_top10_price
WHERE final_inch IN (5, 6, 8, 10, 12, 14)
    AND final_size IN ('Twin', 'Twin XL', 'Full', 'Queen', 'King', 'Cal King')
    AND bsr_date >= '2026-01-01'
GROUP BY final_inch
ORDER BY final_inch;


-- 025. Inch별 size별 평균, 중위수 
SELECT
    CAST(final_inch AS INT64) AS final_inch,
    final_size,
    count(*),
    count(distinct asin),
    AVG(CAST(buybox_price AS FLOAT64)) AS avg_price,
    APPROX_QUANTILES(CAST(buybox_price AS FLOAT64), 2)[OFFSET(1)] AS median_price
    --PERCENTILE_CONT(CAST(buybox_price AS FLOAT64), 0.5) OVER()
FROM dw.amz_bsr_matt_top10_price
WHERE final_inch IN (5, 6, 8, 10, 12, 14)
    AND final_size IN ('Twin', 'Twin XL', 'Full', 'Queen', 'King', 'Cal King')
    AND bsr_date >= '2026-01-01'
GROUP BY 1,2
ORDER BY 1,2;


-- 026. Inch별 size별 평균, 중위수 : Zinus 만 
SELECT
    CAST(final_inch AS INT64) AS final_inch,
    final_size,
    count(*),
    count(distinct asin),
    AVG(CAST(buybox_price AS FLOAT64)) AS avg_price,
    APPROX_QUANTILES(CAST(buybox_price AS FLOAT64), 2)[OFFSET(1)] AS median_price
    --PERCENTILE_CONT(CAST(buybox_price AS FLOAT64), 0.5) OVER()
FROM dw.amz_bsr_matt_top10_price
WHERE final_inch IN (5, 6, 8, 10, 12, 14)
    AND final_size IN ('Twin', 'Twin XL', 'Full', 'Queen', 'King', 'Cal King')
    AND bsr_date >= '2026-01-01'
    AND brand = 'ZINUS'
GROUP BY 1,2
ORDER BY 1,2;




/*
 * test
 */


with cte_media as (
    SELECT
        DISTINCT
        final_inch
        , final_size
        , PERCENTILE_CONT(CAST(buybox_price AS FLOAT64), 0.5)
                          OVER (PARTITION BY cast(final_inch as string), final_size) AS median_buybox_price
    FROM
        dw.amz_bsr_matt_top10_price
    where bsr_date >= '2026-01-01'
)
SELECT
    a.final_inch,
    a.final_size,
    COUNT(*) AS cnt,
    COUNT(DISTINCT asin) as asins,

    -- 전체 평균
    AVG(CAST(buybox_price AS FLOAT64)) AS avg_buybox_price,
    AVG(CAST(list_price AS FLOAT64)) AS avg_list_price,
    -- 차이
    AVG(CAST(list_price AS FLOAT64)) - AVG(CAST(buybox_price AS FLOAT64)) AS discount,

    APPROX_QUANTILES(buybox_price, 2)[OFFSET(1)] AS approx_median_buybox_price,
--     APPROX_QUANTILES(buybox_price, 2),
    ANY_VALUE(b.median_buybox_price) AS median_buybox_price,

    -- ZINUS 만
    COUNT(CASE WHEN brand = 'ZINUS' THEN 1 END) AS zinus_cnt,
    COUNT(DISTINCT CASE WHEN brand = 'ZINUS' THEN asin END) AS zinus_asins,

    AVG(CASE WHEN brand = 'ZINUS' THEN CAST(buybox_price AS FLOAT64) END) AS zinus_buybox_price,
    AVG(CASE WHEN brand = 'ZINUS' THEN CAST(list_price AS FLOAT64) END) AS zinus_list_price,
    -- 차이
    AVG(CASE
            WHEN brand = 'ZINUS'
                THEN CAST(list_price AS FLOAT64)
        END)
        -
    AVG(CASE
            WHEN brand = 'ZINUS'
                THEN CAST(buybox_price AS FLOAT64)
        END) AS zinus_discount

FROM dw.amz_bsr_matt_top10_price a
         LEFT JOIN cte_media b
            ON a.final_inch = b.final_inch
                and a.final_size = b.final_size
WHERE a.final_inch in (5,6,8,10,12,14)
    and a.final_size in ('Twin', 'Twin XL', 'Full', 'Queen', 'King', 'Cal King')
    and bsr_date >= '2026-01-01'
--and brand != 'ZINUS'
--and brand in ('EGOHOME', 'NOVILLA', 'FDW', 'MLILY')
GROUP BY 1,2
ORDER BY 1,2
;


select count(*) from dw.amz_bsr_matt_top10_price
where bsr_date >= '2026-01-01'

select min(bsr_date), max(bsr_date) from dw.amz_bsr_matt_top10_price

SELECT 
	--brand, 
	final_inch , final_size , 
	count(*) as cnt, 
	count(distinct asin) as asin_cnt ,
	avg(cast(buybox_price as float64)) as avg_price
FROM dw.amz_bsr_matt_top10_price
WHERE final_inch IN (5,6,8,10,12,14) 
	and final_size in ('Twin', 'Twin XL', 'Full', 'Queen', 'King', 'Cal King')
	and brand in ('EGOHOME', 'NOVILLA', 'FDW', 'MLILY', 'ZINUS')
GROUP BY 1,2
ORDER BY 1,2





select distinct brand 
from (
	select * from dw.amz_bsr_matt_top10_price
	where final_inch = 12 and final_size = 'King'
)


select distinct asin  
from (
	select * from dw.amz_bsr_matt_top10_price where upper(brand)='ZINUS' and style like '%Small Box%' and buybox_price is not null 
	
)
	
select brand, avg(sales_rank) from dw.amz_bsr_matt_top10_price
group by 1
order by 2 



	

select asin, max(size)
from (
	select * from dw.amz_bsr_matt_top10_price
	where final_inch = 5 and brand='FDW' -- final_size = 'King'
)
group by 1 

select * from dw.amz_bsr_matt_top10_price
where asin = 'B0C1BJKFQ5'

B0C1BJKFQ5


with cte_tmp1 as (
	select *  from dw.amz_bsr_matt_top10_price
	where is_bsr_asin in ('true', 'True')
)
select distinct asin from cte_tmp1 
;


select brand, count(distinct asin)  from dw.amz_bsr_matt_top10_price
group by 1
order by 2 desc


select * from dw.amz_bsr_matt_top10_price
where brand='FDW' and buybox_price is null


select brand, count(*) from dw.amz_bsr_matt_top10_price
group by 1 
order by 2 desc


select min(bsr_date), max(bsr_date) from dw.amz_bsr_matt_top10_price



select final_inch , final_size , buybox_price , list_price , buybox_shipping_price, brand  from dw.amz_bsr_matt_top10_price
--select *  from dw.amz_bsr_matt_top10_price
where bsr_date = '2026-03-10' and brand ='ZINUS' and style like '%Small Box%'
order by 1


select final_inch , final_size , style, count(*), min(buybox_price ), min(list_price ), MIN(sales_rank ) 
from dw.amz_bsr_matt_top10_price
where brand = 'NOVILLA' and bsr_date = '2026-03-10'
group by 1,2,3
order by 1,2,3

select * except(title) 
from dw.amz_bsr_matt_top10_price
where brand = 'NOVILLA' and bsr_date = '2026-03-10' and sales_rank=7
order by 1,2


select min(bsr_date), max(bsr_date) 
from dw.amz_bsr_matt_top10_price



/*
 * Small box 매출 비중 : 2025년 99% 
 */
WITH
    cte_gt as (
        SELECT
            TRIM(asin) AS asin
            , product_description
            , if(CONCAT(LOWER(collection), LOWER(product_description), LOWER(abbre)) LIKE '%wonder%', true, false) as is_wonder_box
        FROM
            meta.amz_zinus_master_pdt_pi_add_new_col
        WHERE
            UPPER(new_collection) LIKE '%GTFM%'
    )
    , cte_sales_src AS (
        SELECT
            CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS yr_month
            , CAST(FORMAT_DATE('%Y', date) AS INT64) AS yr
            , m.is_wonder_box
            , ordered_revenue
            , ordered_units
        FROM
            vc.amz_vc_sales_monthly f
                INNER JOIN cte_gt m
                    ON f.asin = m.asin
        WHERE
            date >= '2024-01-01'
    )
SELECT
    yr
    -- yr_month
    , is_wonder_box
    , SUM(ordered_revenue) AS amt
    , SUM(ordered_units) AS qty
FROM
    cte_sales_src
GROUP BY
    1, 2;
 

