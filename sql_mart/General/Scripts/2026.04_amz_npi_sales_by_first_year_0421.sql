/*
 * Amazon NPI 제품의 출시 년도에 따른 매출 비중 분석
 */


-- # 01. mattress

SELECT DISTINCT first_year  
FROM wook.stck_sales_analysis_of_zns_and_comp_after_2023 


-- # 02. Beds
SELECT *
, CASE 
  WHEN EXTRACT(YEAR FROM first_date) <= 2022 THEN '2022b'
  ELSE CAST(EXTRACT(YEAR FROM first_date) AS STRING)
END AS first_year
FROM wook.beds_specific_brand_price_monthly_agg 
WHERE yr_month BETWEEN '2023-01' AND '2026-03'
;

-- # 03. Bed frames
SELECT * except(style, title, model, type)
, CASE 
  WHEN EXTRACT(YEAR FROM first_date) <= 2022 THEN '2022b'
  ELSE CAST(EXTRACT(YEAR FROM first_date) AS STRING)
END AS first_year
FROM wook.bedframes_specific_brand_price_monthly_agg 
WHERE yr_month BETWEEN '2023-01' AND '2026-03'





-- END --


