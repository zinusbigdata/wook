/*
 * Amazon Mattress 4개 경쟁사의 Price 분석하기
 * 경쟁사 : ZINUS, NOVILLA, EGOHOME, FDW  
 * Table : tmp.stck_zns_comp_sales_anal
 */


-- Top 4 brand 데이터 마트 생성하기  

CREATE OR REPLACE TABLE tmp.stck_zns_comp_sales_anal AS
WITH cte_meta         AS (
    SELECT
        *
    FROM
        (
            -- pi Master
            SELECT DISTINCT
                TRIM(a.asin) AS asin
                , TRIM(a.zinus_sku) AS zinus_sku
                , IF(a.asin = 'B0B6FQZMJ4', '5', REGEXP_EXTRACT(LOWER(TRIM(inch_color)), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)')) AS inch
                , TRIM(size) AS size
                , if(a.financial_category = 'Foam Mattresses', 'Foam Mattress', a.financial_category) as category
                , 1 AS ord
            FROM
                meta.amz_zinus_master_pdt_pi_add_new_col a
            WHERE
                a.financial_category in ('Foam Mattresses', 'Spring Mattresses')

            UNION ALL

            -- GPT Mattress Master
            SELECT DISTINCT
                a.asin
                , cast(null as string) AS zinus_sku
                , profile as inch
                , size_adj as size
                , a.category
                , 2 AS ord
            FROM
                meta.amazon_mattress_master a

        )

    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY asin ORDER BY category is null, ord) = 1
)
, cte_event_day AS (
        SELECT
            UPPER(TRIM(asin)) AS asin
            , metric
            , DATE(event_ts_utc) AS event_date
            , value_num
        FROM tmp.stck_zns_comp_sales_anal_hist_events
        WHERE
            metric = 'list_price'
        --             ( metric = 'list_price' AND value_num IS NOT NULL )
        --             OR metric != 'list_price'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(asin)), metric, DATE(event_ts_utc)
            ORDER BY event_ts_utc DESC
        ) = 1
    )
, cte_list_price AS (
    SELECT
        asin
        , metric
        , event_date
        , LEAD(event_date) OVER (
            PARTITION BY asin, metric
            ORDER BY event_date
            ) AS next_event_date
        , value_num
    FROM cte_event_day
)
, cte_with_meta as (
        SELECT
            a.RetailerSku AS asin
            , FORMAT_DATE('%Y-%m', a.WeekEnding) as yr_month
            , ANY_VALUE(b.zinus_sku) AS zinus_sku
            --     , (ARRAY_AGG(a.Brand)) as brand
            , MAX(REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','')) as brand

            , SUM(a.RetailSales) AS sales
            , SUM(a.UnitsSold) AS units
            , AVG(RetailPrice) AS avg_retail_price
            , AVG(listprice.value_num) AS avg_list_price

            , ANY_VALUE(b.category) AS category
            , ANY_VALUE(b.size) AS size
            , ANY_VALUE(b.inch) AS inch
            --     , ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)] AS title
            , LOWER(
                    REGEXP_REPLACE(
                            NORMALIZE(ARRAY_AGG(a.title ORDER BY a.title IS NULL, a.WeekEnding DESC LIMIT 1)[OFFSET(0)],
                                      NFKC), -- NBSP 등 정규화
                            r'[^[:print:]]',
                            ' '
                    )
              ) AS title
        FROM
            stck.atlas_sales_all a
                --         join meta.amazon_mattress_master b on a.RetailerSku = b.asin
                JOIN cte_meta b
                    ON a.RetailerSku = b.asin
                left join cte_list_price listprice
                    ON a.RetailerSku = listprice.asin
                        AND a.WeekEnding BETWEEN listprice.event_date AND COALESCE(DATE_SUB(listprice.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
        WHERE
            REGEXP_REPLACE(UPPER(a.Brand), r'[^[:print:]]','') IN ( 'FDW', 'EGOHOME', 'NOVILLA', 'ZINUS' )
            AND b.category IS NOT NULL
            AND a.WeekEnding >= '2025-01-01'
        GROUP BY 1, 2
        -- HAVING ARRAY_LENGTH((ARRAY_AGG(a.Brand))) = 1
    )
SELECT DISTINCT
    a.asin
    , COALESCE(a.zinus_sku, b.model) as sku
    , a.brand
    , yr_month
    , a.title
--     , ROW_NUMBER() OVER (PARTITION BY brand, category, yr_month ORDER BY sales DESC) as yr_month_sales_rank
    , ROW_NUMBER() OVER (PARTITION BY category, yr_month ORDER BY sales DESC) as yr_month_sales_rank
    , sales
    , units
    , avg_retail_price
    , avg_list_price
    , category
    , COALESCE(nullif(inch, 'OTHERS'), REGEXP_EXTRACT(LOWER(a.title), r'(\d+(?:\.\d+)?)\s*(?:"|\-?\s*(?:in(?:ch)?|inch(?:es)?)\b)'), 'OTHERS' ) as inch
--     , size
    ,     CASE
              WHEN a.asin in ('B00X6L6DCO', 'B00X6LCL3O') THEN 'Twin'
              WHEN a.asin = 'B0765C7YPX' THEN 'King'
              WHEN LOWER(a.size) IN ('ck', 'california king') THEN 'Cal King'
              WHEN LOWER(a.size) = '12 inch queen medium firm' THEN 'Queen'
              WHEN LOWER(a.size) IN ('k', 'king (u.s. standard)', 'king') THEN 'King'
              WHEN LOWER(a.size) IN ('t', 'twin', 'twin (75*38)') THEN 'Twin'
              WHEN LOWER(a.size) IN ('f', 'full', 'full (75*54)') THEN 'Full'
              WHEN LOWER(a.size) IN ('s', 'single') THEN 'Single'
              WHEN LOWER(a.size) IN ('txl', 'twin-xl', 'twin xl') THEN 'Twin XL'
              WHEN LOWER(a.size) = 'sq' THEN 'Short Queen'
              WHEN LOWER(a.size) IN ('q', 'queen (u.s. standard)', 'queen') THEN 'Queen'
              WHEN LOWER(a.size) = 'nt' THEN 'Narrow Twin'
              WHEN REGEXP_CONTAINS(a.title, r'\b(cal(?:ifornia)?\s*king|cal\s*king)\b') THEN 'Cal King'
              WHEN REGEXP_CONTAINS(a.title, r'\bshort\s*queen\b') THEN 'Short Queen'
              WHEN REGEXP_CONTAINS(a.title, r'\bnarrow\s*twin\b') THEN 'Narrow Twin'
              WHEN REGEXP_CONTAINS(a.title, r'\btwin[\s-]*xl\b') THEN 'Twin XL'
              WHEN REGEXP_CONTAINS(a.title, r'\bqueen\b|\bqeen\b') THEN 'Queen'
              WHEN REGEXP_CONTAINS(a.title, r'\bking\b') THEN 'King'
              WHEN REGEXP_CONTAINS(a.title, r'\bfull\b') THEN 'Full'
              WHEN REGEXP_CONTAINS(a.title, r'\btwin\b') THEN 'Twin'
              WHEN REGEXP_CONTAINS(a.title, r'\bsingle\b') THEN 'Single'
              ELSE a.size
          END AS size
FROM
    cte_with_meta a
         left join tmp.stck_zns_comp_sales_anal_mst b
            on a.asin = b.asin
;

 









-- 1.우리가 알고 있는 아마존 매트리스의 가격 히스토리가 실제로 맞는지?
-- 관세 부과 후 공급가 인상으로 대응 --> 아마존이 SRP 인상 --> 가격경쟁력 약화 --> Sell-out 하락
-- 이 스토리가 실제 Data로 확인이 되는지?

-- 4개 경쟁사의 갸격 히스토리 뽑기, bb price, list price,  
-- 1.1 각 brand의 top asin 



select *  
from tmp.stck_zns_comp_sales_anal
where brand='NOVILLA'



select brand, count(distinct asin) 
from tmp.stck_zns_comp_sales_anal
group by 1 
 

select min(yr_month ) from tmp.stck_zns_comp_sales_anal



-- 0322

select brand, asin, 
	round(sum(sales),0) as sales 
from tmp.stck_zns_comp_sales_anal
group by 1,2
order by 1,3 desc


WITH cte_top4_asins AS (
	SELECT *
	FROM tmp.stck_zns_comp_sales_anal
	WHERE (brand, asin) IN (
	    ('EGOHOME','B0DKT9ZGW3'),
	    ('EGOHOME','B0DKT86Z4D'),
	    ('EGOHOME','B0DKT8N498'),
	    ('EGOHOME','B0DKTCWC8X'),
	    ('EGOHOME','B0DKTFD2M7'),
	
	    ('FDW','B08YDJS51B'),
	    ('FDW','B09Y1WGXWP'),
	    ('FDW','B08YDT6WKN'),
	    ('FDW','B0B2JTCJ8R'),
	    ('FDW','B0C1BK89M3'),
	
	    ('NOVILLA','B083YYBNF9'),
	    ('NOVILLA','B083YXZP19'),
	    ('NOVILLA','B083YWMX49'),
	    ('NOVILLA','B0DWK56CJD'),
	    ('NOVILLA','B0DWK7DTNW'),
	
	    ('ZINUS','B0CKZ1CK1H'),
	    ('ZINUS','B0CKYZ3B83'),
	    ('ZINUS','B0CKYZC93L'),
	    ('ZINUS','B0CSJTKX7K'),
	    ('ZINUS','B0CP1LR1PW'),
	    ('ZINUS','B0CKYZCVXK'),
	    ('ZINUS','B0CKYYHD47'),
	    ('ZINUS','B0CKZ1RXKH'),
	    ('ZINUS','B0CKYXPC4Z'),
	    ('ZINUS','B0CKYZ4DJB')
	)
)
select asin, yr_month, avg_retail_price, avg_list_price, sales
from cte_top4_asins
where brand='EGOHOME'
order by 1,2 
;




select brand, yr_month,
	avg(avg_retail_price) as bb_price,
	avg(avg_list_price) as list_price	
from tmp.stck_zns_comp_sales_anal
group by 1,2
order by 1,2
;




select brand,
	count(*) as cnt,
	count(distinct asin) as asin_cnt,
	count(distinct yr_month)
from tmp.stck_zns_comp_sales_anal
group by 1
;




-- 참고 코드
select brand,
	FORMAT_DATE('%Y-%m', DATE(bsr_date)) AS yr_month,
	avg(CAST(buybox_price AS FLOAT64)) as bb_price,
	avg(CAST(list_price AS FLOAT64)) as list_pirce
from tmp.stck_zns_comp_sales_anal
where brand in ('ZINUS','NOVILLA','EGOHOME','FDW')
group by 1,2
order by 1
