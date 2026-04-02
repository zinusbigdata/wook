/*
 * Review - Sales 분석
 * 데이터 : review_sales_by_collection_category_year, review_sales_by_collection_category
 * 
 */



SELECT COUNT(*) 
FROM wook.review_sales_by_collection_category_year; 

SELECT *
FROM wook.review_sales_by_collection_category_year; 

SELECT
    substring(cast(yr_month as string), 1, 4)
    , sum(sales_qty)
    , sum(sales_amount)
FROM
    wook.review_sales_by_collection_category
WHERE origin_collection = '__TOTAL__'
GROUP BY 1 order by 1 desc;





