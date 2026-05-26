/*
 * Amazon Mattress 분석 - Inch별 Size별 시장 크기 
 */


-- 2026.3.19 : 2026년 inch/size별 시장 규모
select y, inch, size, sum(sales) as sales, sum(units) as units
from mart.stck_summ_by_mattress_brand_and_profile_seg
where brand != 'TOTAL' and inch != 'TOTAL' and size != 'TOTAL' and yr_month != 'TOTAL' and y = 2026
group by 1,2,3
order by sales desc

select y, inch, size, sum(sales) as sales, sum(units) as units
from mart.stck_summ_by_mattress_brand_and_profile_seg
where brand != 'TOTAL' and inch != 'TOTAL' and size != 'TOTAL' and yr_month != 'TOTAL' and y = 2025
group by 1,2,3
order by sales desc




-- 01. 년도별 시장 규모
with cte_tmp_2025 as ( 
	select y, inch, size, sum(sales) as sales, sum(units) as units
	from mart.stck_summ_by_mattress_brand_and_profile_seg
	where brand != 'TOTAL' and inch != 'TOTAL' and size != 'TOTAL' and yr_month != 'TOTAL' and y = 2025
	group by 1,2,3
)
, cte_tmp_2026 as ( 
	select y, inch, size, sum(sales) as sales, sum(units) as units
	from mart.stck_summ_by_mattress_brand_and_profile_seg
	where brand != 'TOTAL' and inch != 'TOTAL' and size != 'TOTAL' and yr_month != 'TOTAL' and y = 2026
	group by 1,2,3
)
select *, 
	sales / sum(sales) over() as ratio
from cte_tmp_2025 
ORDER BY sales desc

UNION ALL 

select *, 
	sales / sum(sales) over() as ratio
from cte_tmp_2026
ORDER BY sales desc

;




with tmp1 as (
	--> 303,476,008
	select y, inch, size, sum(sales) as sales, sum(units) as units
	from mart.stck_summ_by_mattress_brand_and_profile_seg
	where brand != 'TOTAL' and inch != 'TOTAL' and size != 'TOTAL' and yr_month != 'TOTAL' and y in (2025, 2026)
	group by 1,2,3
	order by 4 desc
)
select  sum(sales) from tmp1




SELECT
    sum(RetailSales)
FROM
    stck.atlas_sales_all a
        JOIN meta.amazon_mattress_master b
            ON a.RetailerSku = b.asin
WHERE
    EXTRACT(YEAR FROM WeekEnding) = 2025
    





/*
 * Han 이 작성한 코드 : Mart 생성
 * 일자: 2026.3.16
 */

CREATE OR REPLACE table mart.stck_summ_by_mattress_brand_and_profile_seg as
WITH
    cte_source   AS (
        SELECT
            mst.category
            , mst.size_adj as size_temp
            , if(mst.profile = 'Double', 'OTHERS'
                --                 , mst.profile
                , CASE
                      WHEN REGEXP_CONTAINS(TRIM(mst.profile), r'\.5$') THEN TRIM(mst.profile)  -- x.5 inch
                      WHEN SAFE_CAST(TRIM(mst.profile) AS FLOAT64) IS NULL THEN mst.profile  -- OTHERS
                      ELSE CAST(CAST(ROUND(SAFE_CAST(TRIM(mst.profile) AS FLOAT64)) AS INT64) AS STRING) -- int inch
                  END
              ) as profile

            --             , mst.subcategory
            , IFNULL(NULLIF(LOWER(REGEXP_REPLACE(TRIM(f.Brand), r'[^[:print:]]', '')), ''), 'OTHERS') as brand_temp
--             , IF(LOWER(REGEXP_REPLACE(TRIM(f.Brand), r'[^[:print:]]', '')) = 'zinus', "TRUE", "FALSE") AS zns_flag
            , EXTRACT(YEAR FROM WeekEnding) AS y
            , FORMAT_DATE('%Y-%m', WeekEnding) AS ym
            , f.RetailSales
            , f.UnitsSold
        FROM
            stck.atlas_sales_all f
                inner join meta.amazon_mattress_master mst
                    on f.RetailerSku = mst.asin
        WHERE
            mst.category in ('Foam Mattress', 'Spring Mattress')
    )
    , cte_agg as (
        SELECT
            category
            -- , IF(is_zinus, 'ZINUS', 'AMAZON') AS dtype
--             , brand
            , if(GROUPING(brand_temp) = 1, 'TOTAL', upper(brand_temp)) as brand
            , y
            , if(GROUPING(ym) = 1, 'TOTAL', ym) as yr_month
            , if(GROUPING(size_temp) = 1, 'TOTAL', size_temp) as size
            , if(GROUPING(profile) = 1, 'TOTAL', profile) as inch
            , SUM(RetailSales) AS sales
            , SUM(UnitsSold) AS units
            , if(SUM(UnitsSold) = 0, null, SUM(RetailSales) / SUM(UnitsSold)) AS asp
        FROM
            cte_source
        GROUP BY
            GROUPING SETS (
                ( category, y), ( category, y, ym)
                , ( category, y, brand_temp), ( category, y, ym, brand_temp)
                , ( category, y, size_temp), ( category, y, ym, size_temp)
                , ( category, y, brand_temp, size_temp), ( category, y, ym, brand_temp, size_temp)
                , ( category, y, profile, brand_temp, size_temp), ( category, y, ym, profile, brand_temp, size_temp)
                , ( category, y, profile, size_temp), ( category, y, ym, profile, size_temp)
                , ( category, y, profile, brand_temp), ( category, y, ym, profile, brand_temp)
                , ( category, y, profile), ( category, y, ym, profile)
            )
    )
    , cte_rnk as (
        SELECT
            brand
            , size
            , inch
            , CASE brand
                  WHEN 'TOTAL' THEN 1
                  -- WHEN 'zinus' THEN 2
                  ELSE ROW_NUMBER() OVER (PARTITION BY size, inch ORDER BY total_sales DESC) + 1
              END AS sales_rnk
        FROM
            (
                SELECT
                    brand
                    , size
                    , inch
                    , SUM(if(brand = 'TOTAL', -1, sales)) AS total_sales
                FROM
                    cte_agg
                GROUP BY 1, 2, 3
            )
    )
SELECT
    b.sales_rnk as ord
    , a.*
FROM
    cte_agg a
        JOIN cte_rnk b
            ON a.brand = b.brand AND a.size = b.size AND a.inch = b.inch
;
 

