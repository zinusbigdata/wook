/*
 * Recovery 불만 요인 분석 :  기본 통계 + PPM 
 */

-- [ ppm 구하기 : Claude가 보정 ]

WITH matt_asin AS (                       -- 매트리스 ASIN 마스터 (출하 필터용)
    SELECT DISTINCT asin
    FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type
    WHERE financial_category IN ('Foam Mattresses','Spring Mattresses')
)
, rec_q AS (                              -- 분자: 분기별 Recovery 리뷰 수
    SELECT
        FORMAT_DATE('%Y-Q%Q', DATE(date)) AS quarter,
        COUNT(*)                          AS recovery_reviews
    FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type
    WHERE financial_category IN ('Foam Mattresses','Spring Mattresses')
      AND Complaining_factor_Part_1_right = 'Recovery'
      AND date >= '2024-01-01'
    GROUP BY 1
)
, sales_q AS (                            -- 분모: 분기별 매트리스 출하
    SELECT
        FORMAT_DATE('%Y-Q%Q', s.date)     AS quarter,
        SUM(s.shipped_units)              AS shipped_units
    FROM vc.amz_vc_sales_daily_all s
    JOIN matt_asin m USING (asin)         -- ★ 매트리스 ASIN으로 한정
    WHERE s.date >= '2024-01-01'
      AND s.asin IS NOT NULL
    GROUP BY 1
)
SELECT
    q.quarter,
    r.recovery_reviews,
    q.shipped_units,
    ROUND(SAFE_DIVIDE(r.recovery_reviews, q.shipped_units) * 1000000, 1) AS recovery_ppm
FROM sales_q q
LEFT JOIN rec_q r USING (quarter)
ORDER BY q.quarter DESC
;

-- [ ppm 구하기 : Full Out 이후 Coalesce 과정에서 문제가 있음. ]

WITH matt_rw_recovery AS (
	SELECT DATE(date) dt
		, asin
	FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type
	WHERE financial_category IN ('Foam Mattresses','Spring Mattresses')
		AND Complaining_factor_Part_1_right = 'Recovery'
		AND date >= '2024-01-01'
)
, vc_sales AS (
    SELECT date, asin, SUM(shipped_units) AS shipped_units
    FROM vc.amz_vc_sales_daily_all
    WHERE asin IS NOT NULL AND date >= '2024-01-01'
    GROUP BY 1, 2
)
, joined AS (
    SELECT
        FORMAT_DATE('%Y-Q%Q', COALESCE(r.dt, s.date)) AS quarter
        , COALESCE(r.asin, s.asin)                     AS asin
        , CASE WHEN r.asin IS NOT NULL THEN 1 ELSE 0 END AS is_recovery_review
        , s.shipped_units
    FROM matt_rw_recovery r
    FULL OUTER JOIN vc_sales s
        ON r.asin = s.asin AND r.dt = s.date
)
SELECT
    quarter
    , SUM(is_recovery_review)                        AS recovery_reviews
    , SUM(shipped_units)                             AS shipped_units
    , round(SAFE_DIVIDE(SUM(is_recovery_review), SUM(shipped_units)) * 1000000,1) AS recovery_ppm
FROM joined
GROUP BY quarter
ORDER BY quarter DESC
;





-- [ VC 월별 판매량 : Mattress only ]

SELECT format_date('%Y%m', date) AS yr_month
	, a.asin
	, b.financial_category AS category
	, b.new_collection AS collection
	, b.box_type 
	, SUM(shipped_units) AS shipped_units
FROM vc.amz_vc_sales_daily_all a
JOIN meta.amz_zinus_master_pdt_pi_enriched b ON a.asin = b.asin
WHERE a.date >= '2024-01-01'
	AND b.financial_category in ('Foam Mattresses', 'Spring Mattresses')
GROUP BY 1, 2,3,4,5
ORDER BY 1 DESC, 2  


------------------

WITH matt_rw_recovery AS (
	SELECT DATE(date) dt
		, asin 
		, Complaining_factor_Part_1_right AS cf1
		, Complaining_factor_Part_2_right AS cf2
		, financial_category AS category 
		, new_collection AS collection
		, title 
		, review 
	FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type 
	WHERE financial_category IN ('Foam Mattresses','Spring Mattresses')
		AND Complaining_factor_Part_1_right = 'Recovery' 
			AND date >= '2024-01-01'
)
SELECT  
	asin, collection, 
	count(*) AS cnt
FROM matt_rw_recovery 
GROUP BY 1,2
ORDER BY 3 DESC 
;





SELECT review_quarter
	, sum(cf1_recovery)  
FROM wook.amz_rvw_cmpl_pivot_and_sales_agg 
GROUP BY 1
ORDER BY 1 




SELECT extract(YEAR FROM date) AS yr
	, Complaining_factor_Part_1_right 
	, Complaining_factor_Part_2_right
	, count(*)
FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type 
WHERE financial_category IN ('Foam Mattresses','Spring Mattresses')
	AND Complaining_factor_Part_1_right = 'Recovery' 
GROUP BY 1,2,3 
ORDER BY 1 DESC,2,3,4 DESC; 





SELECT *
FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type 
WHERE financial_category IN ('Foam Mattresses','Spring Mattresses')
	AND Complaining_factor_Part_1_right = 'Recovery' 
	AND Complaining_factor_Part_2_right = 'Corners/edges' 
;

SELECT extract(YEAR FROM date) AS yr
	, cf1, cf2
	, count(*)
FROM dw.cs_voc_cf
WHERE cf1 = 'Recovery'
GROUP BY 1,2,3
ORDER BY 1 desc,2,3 desc


SELECT cf1
	, count(*)
FROM dw.cs_voc_cf
GROUP BY 1
ORDER BY 2 DESC;


