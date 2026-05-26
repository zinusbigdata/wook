/*
 * Written Review 평점 하락 분석
 */

--# ZNS-2768: PPM 분석하기 --> all과 small로 나눠서 집계

SELECT 
	FORMAT('%d-Q%d', EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)) AS yr_qt
	--, sum(written_5star_cnt)
	--, sum(written_4star_cnt)
	--, sum(written_3star_cnt)
	--, sum(written_2star_cnt)
	--, sum(written_1star_cnt)
	, round((sum(written_5star_cnt)*5 + sum(written_4star_cnt)*4 + sum(written_3star_cnt)*3 + sum(written_2star_cnt)*2 + sum(written_1star_cnt)*1) / 
		(sum(written_5star_cnt) + sum(written_4star_cnt) + sum(written_3star_cnt) + sum(written_2star_cnt) + sum(written_1star_cnt)), 2) AS written_rating
	, sum(written_total_cnt) AS written_total_cnt
	, sum(written_12star_cnt) AS wirtten_12star_cnt
	, sum(shipped_units) AS shipped_units
	, round(sum(written_12star_cnt) / sum(written_total_cnt), 2) AS written_12star_ratio
	, round(sum(written_12star_cnt) / sum(shipped_units) * 1000000, 0) AS written_12star_ppm
	--, round(sum(written_total_cnt) / sum(shipped_units) * 1000000, 0) AS written_total_ppm
	, 'All' AS box_type
FROM wook.mattress_review_agg_and_sales_agg_for_ppm_calc
WHERE date between '2021-01-01' AND '2026-03-31' 
	AND financial_category = 'Foam Mattresses'
GROUP BY 1

UNION ALL

SELECT 
	FORMAT('%d-Q%d', EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)) AS yr_qt
	--, sum(written_5star_cnt)
	--, sum(written_4star_cnt)
	--, sum(written_3star_cnt)
	--, sum(written_2star_cnt)
	--, sum(written_1star_cnt)
	, round((sum(written_5star_cnt)*5 + sum(written_4star_cnt)*4 + sum(written_3star_cnt)*3 + sum(written_2star_cnt)*2 + sum(written_1star_cnt)*1) / 
		(sum(written_5star_cnt) + sum(written_4star_cnt) + sum(written_3star_cnt) + sum(written_2star_cnt) + sum(written_1star_cnt)), 2) AS written_rating
	, sum(written_total_cnt) AS written_total_cnt
	, sum(written_12star_cnt) AS wirtten_12star_cnt
	, sum(shipped_units) AS shipped_units
	, round(sum(written_12star_cnt) / sum(written_total_cnt), 2) AS written_12star_ratio
	, round(sum(written_12star_cnt) / sum(shipped_units) * 1000000, 0) AS written_12star_ppm
	--, round(sum(written_total_cnt) / sum(shipped_units) * 1000000, 0) AS written_total_ppm
	, 'Small' AS box_type
FROM wook.mattress_review_agg_and_sales_agg_for_ppm_calc
WHERE date between '2021-01-01' AND '2026-03-31' 
	AND financial_category = 'Foam Mattresses' AND box_type='SmallBox'
GROUP BY 1

UNION ALL 

SELECT 
	FORMAT('%d-Q%d', EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)) AS yr_qt
	--, sum(written_5star_cnt)
	--, sum(written_4star_cnt)
	--, sum(written_3star_cnt)
	--, sum(written_2star_cnt)
	--, sum(written_1star_cnt)
	, round((sum(written_5star_cnt)*5 + sum(written_4star_cnt)*4 + sum(written_3star_cnt)*3 + sum(written_2star_cnt)*2 + sum(written_1star_cnt)*1) / 
		(sum(written_5star_cnt) + sum(written_4star_cnt) + sum(written_3star_cnt) + sum(written_2star_cnt) + sum(written_1star_cnt)), 2) AS written_rating
	, sum(written_total_cnt) AS written_total_cnt
	, sum(written_12star_cnt) AS wirtten_12star_cnt
	, sum(shipped_units) AS shipped_units
	, round(sum(written_12star_cnt) / sum(written_total_cnt), 2) AS written_12star_ratio
	, round(sum(written_12star_cnt) / sum(shipped_units) * 1000000, 0) AS written_12star_ppm
	--, round(sum(written_total_cnt) / sum(shipped_units) * 1000000, 0) AS written_total_ppm
	, 'Big' AS box_type
FROM wook.mattress_review_agg_and_sales_agg_for_ppm_calc
WHERE date between '2021-01-01' AND '2026-03-31' 
	AND financial_category = 'Foam Mattresses' AND box_type IS NULL 
GROUP BY 1


ORDER BY 8, 1  
;

-- select box_type, count(*) from wook.mattress_review_agg_and_sales_agg_for_ppm_calc group by 1


--# 02. is_samllbox 컬럼 추가 버전

SELECT 
	FORMAT('%d-Q%d', EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)) AS yr_qt
	, CASE WHEN box_type = 'SmallBox' THEN 'Y' ELSE 'N' END AS is_smallbox
	--, sum(written_5star_cnt)
	--, sum(written_4star_cnt)
	--, sum(written_3star_cnt)
	--, sum(written_2star_cnt)
	--, sum(written_1star_cnt)
	, round((sum(written_5star_cnt)*5 + sum(written_4star_cnt)*4 + sum(written_3star_cnt)*3 + sum(written_2star_cnt)*2 + sum(written_1star_cnt)*1) / 
		(sum(written_5star_cnt) + sum(written_4star_cnt) + sum(written_3star_cnt) + sum(written_2star_cnt) + sum(written_1star_cnt)), 2) AS written_rating
	, sum(written_total_cnt) AS written_total_cnt
	, sum(written_12star_cnt) AS wirtten_12star_cnt
	, sum(shipped_units) AS shipped_units
	, round(sum(written_12star_cnt) / sum(written_total_cnt), 2) AS written_12star_ratio
	, round(sum(written_12star_cnt) / sum(shipped_units) * 1000000, 0) AS written_12star_ppm
	--, round(sum(written_total_cnt) / sum(shipped_units) * 1000000, 0) AS written_total_ppm
FROM wook.mattress_review_agg_and_sales_agg_for_ppm_calc
WHERE date between '2021-01-01' AND '2026-03-31' 
	AND financial_category = 'Foam Mattresses' 
	--AND box_type='SmallBox'
GROUP BY 1,2
ORDER BY 1 DESC, 2 
;

--# VC data check

SELECT 
	FORMAT('%d-Q%d', EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)) 
	, sum(ordered_units) AS ordered_units 
	, sum(shipped_units) AS shipped_units 
	, sum(shipped_revenue) AS shipped_revenue
FROM vc.amz_vc_sales_daily_all
GROUP BY 1
ORDER BY 1 DESC 



--# ZNS-2771: Complaint Factor 분석하기 

SELECT financial_category
	, Complaining_factor_Part_1_right  
	, count(*) AS cnt
FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type
GROUP BY 1,2
ORDER BY 3 DESC 


-- select min(date) from  wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type

SELECT financial_category
	, Complaining_factor_Part_1_right
	, FORMAT('%d-Q%d', EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)) yr_qt
	, count(*) AS cnt
FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type
WHERE financial_category IN ('Foam Mattresses', 'Spring Mattresses')
GROUP BY 1,2,3
ORDER BY 1,2,3,4 

-- select distinct new_collection from wook.amz_rvw_pivot_cmpl_and_sales_agg where financial_category='Foam Mattresses'
-- select min(review_quarter) from wook.amz_rvw_cmpl_pivot_and_sales_agg 
-- select min(date) from wook.mattress_review_agg_and_sales_agg_for_ppm_calc 


-- ALL
SELECT new_collection, review_quarter  
	, sum(cf1_too_hard) AS too_hard_cnt
	, sum(cf1_too_soft) AS too_soft_cnt
	, sum(cf1_recovery) AS recovery_cnt
	, sum(cf1_durability) AS durability_cnt
	, sum(shipped_units) AS unit_sales
	, round(sum(cf1_too_hard) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS too_hard_ppm
	, round(sum(cf1_too_soft) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS too_soft_ppm
	, round(sum(cf1_recovery) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS recovery_ppm
	, round(sum(cf1_durability) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS durability_ppm
	, 'All' AS type
FROM wook.amz_rvw_cmpl_pivot_and_sales_agg
WHERE financial_category = 'Foam Mattresses' AND review_quarter >= '2021-Q1'
	--AND box_type = 'SmallBox'
GROUP BY 1,2

UNION ALL 

-- Small Box
SELECT new_collection, review_quarter  
	, sum(cf1_too_hard) AS too_hard_cnt
	, sum(cf1_too_soft) AS too_soft_cnt
	, sum(cf1_recovery) AS recovery_cnt
	, sum(cf1_durability) AS durability_cnt
	, sum(shipped_units) AS unit_sales
	, round(sum(cf1_too_hard) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS too_hard_ppm
	, round(sum(cf1_too_soft) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS too_soft_ppm
	, round(sum(cf1_recovery) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS recovery_ppm
	, round(sum(cf1_durability) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS durability_ppm
	, 'Small' AS type
FROM wook.amz_rvw_cmpl_pivot_and_sales_agg
WHERE financial_category = 'Foam Mattresses' AND review_quarter >= '2021-Q1'
	AND box_type = 'SmallBox'
GROUP BY 1,2

UNION ALL 

-- Big Box
SELECT new_collection, review_quarter  
	, sum(cf1_too_hard) AS too_hard_cnt
	, sum(cf1_too_soft) AS too_soft_cnt
	, sum(cf1_recovery) AS recovery_cnt
	, sum(cf1_durability) AS durability_cnt
	, sum(shipped_units) AS unit_sales
	, round(sum(cf1_too_hard) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS too_hard_ppm
	, round(sum(cf1_too_soft) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS too_soft_ppm
	, round(sum(cf1_recovery) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS recovery_ppm
	, round(sum(cf1_durability) / NULLIF(sum(shipped_units),0) * 1000000, 0) AS durability_ppm
	, 'Big' AS type
FROM wook.amz_rvw_cmpl_pivot_and_sales_agg
WHERE financial_category = 'Foam Mattresses' AND review_quarter >= '2021-Q1'
	AND box_type IS null
GROUP BY 1,2

ORDER BY 12, 1,2
;

/*
 * ZNS-2763 : Written review가 전체 review에서 차지하는 비율
 */

--# 01 판매량 대비 written review 비율 

SELECT 
	FORMAT('%d-Q%d', EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)) AS yr_qt
	, round((sum(written_5star_cnt)*5 + sum(written_4star_cnt)*4 + sum(written_3star_cnt)*3 + sum(written_2star_cnt)*2 + sum(written_1star_cnt)*1) / 
		(sum(written_5star_cnt) + sum(written_4star_cnt) + sum(written_3star_cnt) + sum(written_2star_cnt) + sum(written_1star_cnt)), 2) AS written_rating
	, sum(written_total_cnt) AS written_total_cnt
	, sum(written_12star_cnt) AS wirtten_12star_cnt
	, sum(shipped_units) AS shipped_units
	, round(sum(written_12star_cnt) / sum(written_total_cnt), 2) AS written_12star_ratio
	, round(sum(written_12star_cnt) / sum(shipped_units) * 1000000, 0) AS written_12star_ppm
	, round(sum(written_total_cnt) / sum(shipped_units) * 100, 3) AS written_sales_ratio
	, 'All' AS box_type
FROM wook.mattress_review_agg_and_sales_agg_for_ppm_calc
WHERE date between '2021-01-01' AND '2026-03-31' 
	AND financial_category = 'Foam Mattresses'
GROUP BY 1

UNION ALL 

SELECT 
	FORMAT('%d-Q%d', EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)) AS yr_qt
	, round((sum(written_5star_cnt)*5 + sum(written_4star_cnt)*4 + sum(written_3star_cnt)*3 + sum(written_2star_cnt)*2 + sum(written_1star_cnt)*1) / 
		(sum(written_5star_cnt) + sum(written_4star_cnt) + sum(written_3star_cnt) + sum(written_2star_cnt) + sum(written_1star_cnt)), 2) AS written_rating
	, sum(written_total_cnt) AS written_total_cnt
	, sum(written_12star_cnt) AS wirtten_12star_cnt
	, sum(shipped_units) AS shipped_units
	, round(sum(written_12star_cnt) / sum(written_total_cnt), 2) AS written_12star_ratio
	, round(sum(written_12star_cnt) / sum(shipped_units) * 1000000, 0) AS written_12star_ppm
	, round(sum(written_total_cnt) / sum(shipped_units) * 100, 3) AS written_sales_ratio
	, 'Small' AS box_type
FROM wook.mattress_review_agg_and_sales_agg_for_ppm_calc
WHERE date between '2021-01-01' AND '2026-03-31' 
	AND financial_category = 'Foam Mattresses' AND box_type='SmallBox'
GROUP BY 1

ORDER BY 9,1


--# PDP review count 고려하기

SELECT yr_quarter 
	, min(date) AS qt_start_day
	, max(date) AS qt_end_day 
FROM wook.foam_mattress_asin_pdp_rat_and_written_rvw_and_sales
WHERE date between '2021-01-01' AND '2026-03-31'  
	AND adj_collection = 'GTFM' AND new_collection ='GTFM' 
GROUP BY 1
	
--# 

WITH cte_qt_bounds AS (
	SELECT '2021-Q1' AS review_quarter, '2021-01-01' AS quarter_start_date, '2021-03-31' AS quarter_end_date UNION ALL
	SELECT '2021-Q2', '2021-04-01', '2021-06-30' UNION ALL
	SELECT '2021-Q3', '2021-07-01', '2021-09-30' UNION ALL
	SELECT '2021-Q4', '2021-10-01', '2021-12-31' UNION ALL
	SELECT '2022-Q1', '2022-01-01', '2022-03-31' UNION ALL
	SELECT '2022-Q2', '2022-04-01', '2022-06-30' UNION ALL
	SELECT '2022-Q3', '2022-07-01', '2022-09-30' UNION ALL
	SELECT '2022-Q4', '2022-10-01', '2022-12-31' UNION ALL
	SELECT '2023-Q1', '2023-01-01', '2023-03-31' UNION ALL
	SELECT '2023-Q2', '2023-04-01', '2023-06-30' UNION ALL
	SELECT '2023-Q3', '2023-07-01', '2023-09-30' UNION ALL
	SELECT '2023-Q4', '2023-10-01', '2023-12-31' UNION ALL
	SELECT '2024-Q1', '2024-01-01', '2024-03-31' UNION ALL
	SELECT '2024-Q2', '2024-04-01', '2024-06-30' UNION ALL
	SELECT '2024-Q3', '2024-07-01', '2024-09-30' UNION ALL
	SELECT '2024-Q4', '2024-10-01', '2024-12-31' UNION ALL
	SELECT '2025-Q1', '2025-01-01', '2025-03-31' UNION ALL
	SELECT '2025-Q2', '2025-04-01', '2025-06-30' UNION ALL
	SELECT '2025-Q3', '2025-07-01', '2025-09-30' UNION ALL
	SELECT '2025-Q4', '2025-10-01', '2025-12-31' UNION ALL
	SELECT '2026-Q1', '2026-01-01', '2026-03-31'
)
SELECT a.*
	, b.ratings_total AS start_cnt
	, c.ratings_total AS end_cnt
	, c.ratings_total - b.ratings_total AS quarterly_diff
FROM cte_qt_bounds a
JOIN wook.foam_mattress_collection_pdp_rat_and_written_rvw_and_sales b
	ON CAST(a.quarter_start_date AS DATE) = b.date
	AND b.adj_collection = 'GTFM'
JOIN wook.foam_mattress_collection_pdp_rat_and_written_rvw_and_sales c
	ON CAST(a.quarter_end_date AS DATE) = c.date       -- ① quarter_end_date로 수정
	AND c.adj_collection = 'GTFM'        -- ② ON 절 안에 조건 배치
ORDER BY a.review_quarter
;

SELECT adj_collection, count(*)
FROM wook.foam_mattress_collection_pdp_rat_and_written_rvw_and_sales
GROUP BY 1 
ORDER BY 2 DESC 

/*
 * ZNS-2763 : pdp total cnt vs written cnt by collection
 */
SELECT
	adj_collection, yr_quarter
    --, FORMAT_DATE('%Y-Q%Q', date) as yr_quarter
    , ARRAY_AGG(ratings_total ORDER BY date ASC LIMIT 1)[OFFSET(0)] AS first_row
    , ARRAY_AGG(ratings_total ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS last_row
    , ARRAY_AGG(ratings_total ORDER BY date DESC LIMIT 1)[OFFSET(0)] - ARRAY_AGG(ratings_total ORDER BY date ASC LIMIT 1)[OFFSET(0)] as delta

    , sum(written_total_cnt) AS sum_written_total_cnt
    , sum(written_total_cnt) / (ARRAY_AGG(ratings_total ORDER BY date DESC LIMIT 1)[OFFSET(0)] - ARRAY_AGG(ratings_total ORDER BY date ASC LIMIT 1)[OFFSET(0)]) as ratio
FROM
    wook.foam_mattress_collection_pdp_rat_and_written_rvw_and_sales
WHERE
    adj_collection IN ('GTFM', 'MFMBHD', 'FGM', 'CMM')
--     adj_collection = 'GTFM'
GROUP BY 1,2
ORDER BY 1,2 
;

/*
 * Small Box 판매량 과 Written ratio 
 */

with cte_small_box_agg as (
    SELECT
        adj_collection
        , FORMAT_DATE('%Y Q%Q', date) as yr_quarter
        , sum(written_total_cnt) as small_box_written_total_cnt
    FROM
        wook.foam_mattress_asin_pdp_rat_and_written_rvw_and_sales
    WHERE
        box_type = 'SmallBox'
        AND adj_collection IN ( 'GTFM', 'MFMBHD', 'FGM' )
    GROUP BY 1,2
)
, cte_pdp_colletion_agg as (
        SELECT
            a.adj_collection
            , a.yr_quarter
            , ARRAY_AGG(ratings_total ORDER BY date ASC LIMIT 1)[OFFSET(0)] AS first_row
            , ARRAY_AGG(ratings_total ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS last_row
            , ARRAY_AGG(ratings_total ORDER BY date DESC LIMIT 1)[OFFSET(0)] - ARRAY_AGG(ratings_total ORDER BY date ASC LIMIT 1)[OFFSET(0)] AS delta

            , SUM(written_total_cnt) AS sum_written_total_cnt
            , SUM(written_total_cnt) / ( ARRAY_AGG(ratings_total ORDER BY date DESC LIMIT 1)[OFFSET(0)] - ARRAY_AGG(ratings_total ORDER BY date ASC LIMIT 1)[OFFSET(0)] ) AS ratio

        FROM
            wook.foam_mattress_collection_pdp_rat_and_written_rvw_and_sales a
        WHERE
            a.adj_collection IN ( 'GTFM', 'FGM', 'MFMBHD', 'MFMAMF', 'FMS', 'MSSA1ZI' )
        GROUP BY
            1, 2
    )
SELECT
    a.*
    , b.small_box_written_total_cnt
    , b.small_box_written_total_cnt / a.delta AS written_ratio
FROM
    cte_pdp_colletion_agg a
        LEFT JOIN cte_small_box_agg b
            ON a.adj_collection = b.adj_collection AND a.yr_quarter = b.yr_quarter
ORDER BY
    1, 2 DESC
;



/*
 * Small box filtering
 */
SELECT *,
 IF(CONCAT(LOWER(collection), LOWER(product_description), LOWER(abbre)) LIKE '%wonder%', 'SmallBox', CAST(NULL AS STRING)) AS box_type
FROM meta.amz_zinus_master_pdt_pi

/*
 * 가장 판매량이 높은 컬렉션
 */
SELECT new_collection
	, sum(shipped_units)
FROM wook.mattress_review_agg_and_sales_agg_for_ppm_calc
WHERE box_type = 'SmallBox'
GROUP BY 1
ORDER BY 2 DESC 
;



