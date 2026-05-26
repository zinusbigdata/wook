/*
 * 그린티 Review 분석하기 : 최근 review 평점 하락 때문에
 * 소스 데이터:  crwl.amz_pdt_all(keepa) , dw.rf_amz_pdt_zns_comp_daily (rain forest)
 * 		, meta.amz_zinus_master_pdt_pi_add_new_col
 * 		, tmp1.keepa_sales_ranks_5y (그린티만 5년)
 * 	
 * 작성일 : 2026-02-27
 * 작성자 : 한남식   
 */

CREATE OR REPLACE TABLE wook.gtfm_price_avg_rating_raw AS
WITH
    cte_gt as ( --> 46 asin
        SELECT
            TRIM(asin) AS asin
            , TRIM(size) AS size
            , TRIM(inch_color) AS inch_color
            , product_description
        FROM
            meta.amz_zinus_master_pdt_pi_add_new_col
        WHERE
            UPPER(new_collection) LIKE '%GTFM%'
    )
    , cte_crwl_pdp_mst AS (
        with mst_src as (     --> 79,534 rows
            SELECT
                p.asin
                , pageAsin
                , DATE(crawlTime) AS crawl_date
                --             , COALESCE(dealPrice, price) AS crwl_price
                -- , brand
                , title
                , productDimensions
                , buyBoxSellerName
                , color
                , itemThickness
                , packageHeight
                , packageWidth
                , packageWeight
                , packageLength
                , weight
                , p.size
                , bedSize
                , g.size as pi_mst_size
                , g.inch_color as pi_mst_inch
                , g.product_description as pi_mst_desc
            FROM
                crwl.amz_pdt_all p
                    join cte_gt g on p.asin = g.asin
        )
        select * from
            (
                SELECT
                    asin
                    , * EXCEPT (asin, pageAsin)
                FROM
                    mst_src p

                UNION ALL

                SELECT
                    pageAsin as asin
                    , * EXCEPT (asin, pageAsin)
                FROM
                    mst_src p
            )
        WHERE
            productDimensions IS NOT NULL and DATE(crawl_date) >= '2025-01-01'
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY
                    asin
                --                     , DATE(crawlTime)
                ORDER BY crawl_date DESC
                ) = 1
    )
    --select * from cte_crwl_pdp_mst --> Prodcut dimensions 마스터 추출 --> 46개 asin   
    , cte_keepa_sales_interval AS (
        with cte_keepa_change_daily_last AS (
            SELECT
                k.asin
                , DATE(timestamp_utc) AS change_date
                , sales_rank
            FROM
                tmp1.keepa_sales_ranks_5y k
            WHERE
                category_name = 'Mattresses'
                AND sales_rank IS NOT NULL
            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY k.asin, DATE(timestamp_utc)
                    ORDER BY timestamp_utc DESC
                    ) = 1
        )
        SELECT
            asin
            , change_date AS valid_from
            , DATE_SUB(
                    LEAD(change_date) OVER (PARTITION BY asin ORDER BY change_date),
                    INTERVAL 1 DAY
              ) AS valid_to
            , sales_rank
        FROM cte_keepa_change_daily_last
    )
    --> sales rank 정리
    , cte_keepa_list_price_interval AS (
        with cte_keepa_change_daily_last AS (
            SELECT
                k.asin
                , DATE(timestamp_utc) AS change_date
                , price
            FROM
                tmp1.keepa_prices_5y k
            WHERE
                metric = 'list_price'
                and price is not null
            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY k.asin, DATE(timestamp_utc)
                    ORDER BY timestamp_utc DESC
                    ) = 1
        )
        SELECT
            asin
            , change_date AS valid_from
            , DATE_SUB(
                    LEAD(change_date) OVER (PARTITION BY asin ORDER BY change_date),
                    INTERVAL 1 DAY
              ) AS valid_to
            , price
        FROM cte_keepa_change_daily_last
    )
    --> price 정리 
    , cte_keepa_bb_price_interval AS (
        with cte_keepa_change_daily_last AS (
            SELECT
                k.asin
                , DATE(timestamp_utc) AS change_date
                , price
            FROM
                tmp1.keepa_prices_5y k
            WHERE
                metric = 'buybox_price'
                and price is not null
            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY k.asin, DATE(timestamp_utc)
                    ORDER BY timestamp_utc DESC
                    ) = 1
        )
        SELECT
            asin
            , change_date AS valid_from
            , DATE_SUB(
                    LEAD(change_date) OVER (PARTITION BY asin ORDER BY change_date),
                    INTERVAL 1 DAY
              ) AS valid_to
            , price
        FROM cte_keepa_change_daily_last
    )
    --> rating 정리
    , cte_keepa_rating_interval AS (
        with cte_keepa_change_daily_last AS (
            SELECT
                k.asin
                , DATE(timestamp_utc) AS change_date
                , value as rating
            FROM
                tmp1.keepa_reviews_5y k
            WHERE
                metric = 'review_rating'
--                 and value is not null
            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY k.asin, DATE(timestamp_utc)
                    ORDER BY timestamp_utc DESC
                    ) = 1
        )
        SELECT
            asin
            , change_date AS valid_from
            , DATE_SUB(
                    LEAD(change_date) OVER (PARTITION BY asin ORDER BY change_date),
                    INTERVAL 1 DAY
              ) AS valid_to
            , rating
        FROM cte_keepa_change_daily_last
    )
    --> rw cnt 정리 
    , cte_keepa_review_count_interval AS (
        with cte_keepa_change_daily_last AS (
            SELECT
                k.asin
                , DATE(timestamp_utc) AS change_date
                , value as review_count
            FROM
                tmp1.keepa_reviews_5y k
            WHERE
                metric = 'review_count'
            --                 and value is not null
            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY k.asin, DATE(timestamp_utc)
                    ORDER BY timestamp_utc DESC
                ) = 1
        )
        SELECT
            asin
            , change_date AS valid_from
            , DATE_SUB(
                    LEAD(change_date) OVER (PARTITION BY asin ORDER BY change_date),
                    INTERVAL 1 DAY
              ) AS valid_to
            , review_count
        FROM cte_keepa_change_daily_last
    )
    , cte_asin_date_src AS (
        SELECT
            asin
            , MIN(dt) AS min_dt
            , MAX(dt) AS max_dt
        FROM (
            SELECT
                asin
                , DATE(crawlTime_utc) AS dt
            FROM dw.rf_amz_pdt_zns_comp_daily
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                request_asin AS asin
                , DATE(crawlTime_utc) AS dt
            FROM dw.rf_amz_pdt_zns_comp_daily
            WHERE request_asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , valid_from AS dt
            FROM cte_keepa_sales_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , COALESCE(valid_to, valid_from) AS dt
            FROM cte_keepa_sales_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , valid_from AS dt
            FROM cte_keepa_list_price_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , COALESCE(valid_to, valid_from) AS dt
            FROM cte_keepa_list_price_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , valid_from AS dt
            FROM cte_keepa_bb_price_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , COALESCE(valid_to, valid_from) AS dt
            FROM cte_keepa_bb_price_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , valid_from AS dt
            FROM cte_keepa_rating_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , COALESCE(valid_to, valid_from) AS dt
            FROM cte_keepa_rating_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , valid_from AS dt
            FROM cte_keepa_review_count_interval
            WHERE asin IS NOT NULL

            UNION ALL

            SELECT
                asin
                , COALESCE(valid_to, valid_from) AS dt
            FROM cte_keepa_review_count_interval
            WHERE asin IS NOT NULL
        ) src
        WHERE dt IS NOT NULL
        GROUP BY asin
    )
    , cte_asin_date_bounds AS (
        SELECT
            s.asin
            , s.min_dt
            , s.max_dt
        FROM cte_asin_date_src s
            JOIN cte_gt g
                ON s.asin = g.asin
    )
    , cte_date AS (
        SELECT
            b.asin
            , fill_date
        FROM cte_asin_date_bounds b
            JOIN UNNEST(GENERATE_DATE_ARRAY(b.min_dt, b.max_dt)) AS fill_date
    )
    , cte_rf_extra  AS (
        WITH cte_rf_temp as (
            SELECT
                d.asin
                , d.fill_date AS date
                , salesrank
                , IFNULL(rf.bw_price_value, FIRST_VALUE(rf.bw_price_value IGNORE NULLS) OVER (PARTITION BY d.asin ORDER BY d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS retail_price

                , rf.rating
                , rf.ratings_total
                , rf.rating_breakdown_one_star_count
                , rf.rating_breakdown_two_star_count
                , rf.rating_breakdown_three_star_count
                , rf.rating_breakdown_four_star_count
                , rf.rating_breakdown_five_star_count

                , rf.rating_breakdown_one_star_percentage
                , rf.rating_breakdown_two_star_percentage
                , rf.rating_breakdown_three_star_percentage
                , rf.rating_breakdown_four_star_percentage
                , rf.rating_breakdown_five_star_percentage

--                 , IFNULL(rf.rating, FIRST_VALUE(rf.rating IGNORE NULLS) OVER (PARTITION BY d.asin ORDER BY d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS rating_fill
--                 , IFNULL(rf.ratings_total, FIRST_VALUE(rf.ratings_total IGNORE NULLS) OVER (PARTITION BY d.asin ORDER BY d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS ratings_fill_total

                , IFNULL(rf.request_asin, FIRST_VALUE(rf.request_asin IGNORE NULLS) OVER (PARTITION BY d.asin ORDER BY d.fill_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS request_asin
            -- , UPPER(rf.bw_ff_type) AS seller_type
            FROM
                cte_date d
                    LEFT JOIN (
                        SELECT
                            a.asin
                            , a.request_asin
                            , a.bw_price_value
                            -- , a.bw_ff_type
    --                         , least(a.salesrank1, a.salesrank2, a.salesrank3, a.salesrank4) AS salesrank
    --                         , LEAST(CAST(a.salesrank1 AS INT64),
    --                                 CAST(a.salesrank2 AS INT64),
    --                                 CAST(a.salesrank3 AS INT64),
    --                                 CAST(a.salesrank4 AS INT64)) AS salesrank
    --                         ,
                            , (SELECT MIN(cast(val as INT64)) FROM UNNEST([a.salesrank1, a.salesrank2, a.salesrank3, a.salesrank4]) AS val) AS salesrank -- min 함수 null 무시
                            , a.rating
                            , a.ratings_total

                            , a.rating_breakdown_one_star_count
                            , a.rating_breakdown_two_star_count
                            , a.rating_breakdown_three_star_count
                            , a.rating_breakdown_four_star_count
                            , a.rating_breakdown_five_star_count

                            , a.rating_breakdown_one_star_percentage
                            , a.rating_breakdown_two_star_percentage
                            , a.rating_breakdown_three_star_percentage
                            , a.rating_breakdown_four_star_percentage
                            , a.rating_breakdown_five_star_percentage

                            , PARSE_DATE('%Y-%m-%d', SUBSTRING(crawlTime_utc, 0, 10)) AS date
                        FROM
                            dw.rf_amz_pdt_zns_comp_daily a
                        QUALIFY
                            ROW_NUMBER() OVER (PARTITION BY a.asin, PARSE_DATE('%Y-%m-%d', SUBSTRING(crawlTime_utc, 0, 10)) ORDER BY crawlTime_utc DESC) = 1
                    ) rf
                        ON d.asin = rf.asin AND d.fill_date = rf.date
        )
        SELECT
            a.* EXCEPT (request_asin)
        FROM
            cte_rf_temp a
                JOIN cte_gt g ON a.asin = g.asin


        UNION DISTINCT

        SELECT
            request_asin as asin
            , a.* EXCEPT (asin, request_asin)
        FROM
            cte_rf_temp a
                JOIN cte_gt g ON a.request_asin = g.asin
    )
SELECT
    b.asin
    , b.date
    , FORMAT_DATE('%Y-%m', b.date) as yr_month
    -- , COALESCE(k.keepa_amazon_price, bk.keepa_amazon_price, c.crwl_price) as price
    -- , COALESCE(k.amz_price, r.retail_price) AS price

    , b.salesrank as rf_sales_rank
    , b.rating as rf_rating
    , b.ratings_total as rf_ratings_total
    , b.rating_breakdown_one_star_count as rf_rvw_1_cnt
    , b.rating_breakdown_two_star_count as rf_rvw_2_cnt
    , b.rating_breakdown_three_star_count as rf_rvw_3_cnt
    , b.rating_breakdown_four_star_count as rf_rvw_4_cnt
    , b.rating_breakdown_five_star_count as rf_rvw_5_cnt

    , b.rating_breakdown_one_star_percentage as rf_rvw_1_per
    , b.rating_breakdown_two_star_percentage as rf_rvw_2_per
    , b.rating_breakdown_three_star_percentage as rf_rvw_3_per
    , b.rating_breakdown_four_star_percentage as rf_rvw_4_per
    , b.rating_breakdown_five_star_percentage as rf_rvw_5_per

    , r.rating as kp_rating 
    , rc.review_count as kp_review_count

    , s.sales_rank as kp_sales_rank
    , l.price as kp_list_price
    , bb.price as kp_bb_price
    , b.retail_price as rf_price
    , mst.* EXCEPT (asin, crawl_date)


--             , IF(UPPER(COALESCE(c.brand, r.brand)) LIKE '%ZINUS%', TRUE, FALSE) AS is_zinus
FROM
    cte_rf_extra b

        LEFT JOIN cte_crwl_pdp_mst mst
            ON b.asin = mst.asin

        LEFT JOIN cte_keepa_sales_interval s
            ON mst.asin = s.asin
               AND b.date BETWEEN s.valid_from AND COALESCE(s.valid_to, DATE '9999-12-31')
        LEFT JOIN cte_keepa_list_price_interval l
            ON mst.asin = l.asin
                AND b.date BETWEEN l.valid_from AND COALESCE(l.valid_to, DATE '9999-12-31')
        LEFT JOIN cte_keepa_bb_price_interval bb
            ON mst.asin = bb.asin
                AND b.date BETWEEN bb.valid_from AND COALESCE(bb.valid_to, DATE '9999-12-31')

        LEFT JOIN cte_keepa_rating_interval r
            ON mst.asin = r.asin
                AND b.date BETWEEN r.valid_from AND COALESCE(r.valid_to, DATE '9999-12-31')
        LEFT JOIN cte_keepa_review_count_interval rc
            ON mst.asin = rc.asin
                AND b.date BETWEEN rc.valid_from AND COALESCE(rc.valid_to, DATE '9999-12-31')
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY b.asin, b.date) = 1
-- ORDER BY
--     b.bsr_date desc, b.rank, b.asin
;

-- END : wook.gtfm_price_avg_rating_raw





/*
 * 월별 평균 Rank, List Price, BB Price 구하기
 * 2025년 그린티 제품만
 * Keepa 데이터로만 처리하기 
 */ 

select 
	asin, title, pi_mst_inch, pi_mst_size, 
	--FORMAT_DATE('%yY_%QQ', date) AS yr_qt,
	yr_month, 
	count(*) as row_cnt,
	
	avg(kp_sales_rank) as kp_rank,
	--avg(rf_sales_rank) as rf_rank,
	
	avg(kp_list_price) as kp_list_price,
	avg(kp_bb_price) as kp_bb_price,
	--avg(rf_list_price) as kp_list_price,
	--avg(rf_price) as rf_price,
from wook.gtfm_price_avg_rating_raw
where yr_month >= '2025-01'
	and asin != 'B004TMZ76M'
group by 1,2,3,4,5
order by 1,2,3,4,5
;


select
	yr_month,
	avg(kp_sales_rank) as kp_rank,
	--avg(rf_sales_rank) as rf_rank,
	
	avg(kp_list_price) as kp_list_price,
	avg(kp_bb_price) as kp_bb_price,
	--avg(rf_list_price) as kp_list_price,
	--avg(rf_price) as rf_price,
from wook.gtfm_price_avg_rating_raw
where yr_month >= '2025-01' and asin != 'B004TMZ76M'
group by 1
order by 1
;


/*
 * Small Box만 추출하기
 */
with cte_gtfm_tmp1 as (
	select a.*
		, b.financial_category 
		, b.new_collection 
		, b.origin_collection 
	from  wook.gtfm_price_avg_rating_raw a 
	left join meta.amz_zns_cat_col_mst b ON a.asin = b.asin 
	where yr_month >= '2025-01' and a.asin != 'B004TMZ76M'
)
select 
	asin, new_collection, origin_collection, pi_mst_inch, pi_mst_size, 
	--FORMAT_DATE('%yY_%QQ', date) AS yr_qt,
	yr_month, 
	count(*) as row_cnt,
	
	avg(kp_sales_rank) as kp_rank,
	--avg(rf_sales_rank) as rf_rank,
	
	avg(kp_list_price) as kp_list_price,
	avg(kp_bb_price) as kp_bb_price,
	
	case 
		when origin_collection like '%WonderBox%' then 1
		else 0
	end as is_smallbox 
from cte_gtfm_tmp1
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6 




-- 테스트 

select distinct asin from wook.gtfm_price_avg_rating_raw

select asin, count(kp_sales_rank), count(rf_sales_rank), min(date), MAX(date) from wook.gtfm_price_avg_rating_raw
group by 1 

select * 
from wook.gtfm_price_avg_rating_raw
where yr_month >= '2025-01'and asin='B00Q7EPSHI'
order by date


SELECT asin, date, rf_sales_rank, kp_sales_rank, kp_list_price, kp_bb_price, rf_price from wook.gtfm_price_avg_rating_raw
where yr_month >= '2025-01' and  asin = 'B0CKYZC93L'
order by date desc

