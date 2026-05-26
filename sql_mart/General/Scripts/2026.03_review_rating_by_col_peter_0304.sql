
/*
 * Collection별 Review Rating 추출하기 - 2025년 3Q, 4Q 
 * Peter 요구사항: 그린티 가격 경쟁력 하락 보고서의 Backup 자료   
 */


WITH cte_review_by_col AS (
	SELECT 
		a.* 
		, COALESCE(b.financial_category, a.financial_category, NULL) AS zinus_category 		
		, COALESCE(b.origin_collection, a.collection, NULL) AS origin_collection
	--, b.origin_collection
	--, b.main_collection
		, b.new_collection
	FROM
		dw.amz_us_zinus_rvw a
	LEFT JOIN meta.amz_zns_cat_col_mst b ON
		a.asin = b.asin
	WHERE
		--review_date BETWEEN '2024-01-01' AND '2025-12-31'
		a.financial_category in ('Foam Mattresses','Spring Mattresses') 
		AND review_date >= '2025-01-01'  
)  
--> 12890 
, cte_review_by_col_qt AS (
	SELECT
		--CAST(FORMAT_DATE('%Y%m', review_date) AS INT64) AS yr_month
		FORMAT_DATE('%yY_%QQ', review_date) AS yr_qt
		, zinus_category
		, new_collection
		, origin_collection 
		--, asin
		, SUM(IF(rating < 3, 1, 0)) AS written_12_cnt
		, COUNT(1) AS written_all_cnt
		, SUM(IF(rating < 3, 1, 0)) / COUNT(1) AS written_12_ratio
		, SUM(IF(rating = 1, 1, 0)) AS written_1_cnt
		, SUM(IF(rating = 2, 1, 0)) AS written_2_cnt
		, SUM(IF(rating = 3, 1, 0)) AS written_3_cnt
		, SUM(IF(rating = 4, 1, 0)) AS written_4_cnt
		, SUM(IF(rating = 5, 1, 0)) AS written_5_cnt
		, (5 * SUM(IF(rating = 5, 1, 0)) + 4 * SUM(IF(rating = 4, 1, 0)) + 3 * SUM(IF(rating = 3, 1, 0)) + 2 * SUM(IF(rating = 2, 1, 0)) + 1 * SUM(IF(rating = 1, 1, 0))) 
		    / COUNT(1) AS avg_written_rating
	FROM 
		cte_review_by_col
	GROUP BY 
		1, 2, 3, 4
	ORDER BY 
		1, 2, 3, 4
)
select * from cte_review_by_col_qt




/*
 * 기초 분석
 */

select distinct financial_category  from dw.amz_us_zinus_rvw 

select * from dw.amz_us_zinus_rvw 
where collection='8in Cloud MF' and review_date between '2025-07-01' and '2025-09-30'
--review_date >= '2025-01-01'  

