-- zns2705 create tables only
-- source: sql/zns2705-extract_data_for_key_competitor_comparisons_by_zinus_line_item.sql

-- mattress target asins (top 5)
CREATE OR REPLACE TABLE tmp1.target_asins_temp AS
WITH
    cte_src AS (
        SELECT
            *,
            CASE
                WHEN LOWER(size) LIKE '%king%' THEN 'King'
                WHEN LOWER(size) LIKE '%queen%' THEN 'Queen'
                WHEN LOWER(size) LIKE '%twin%' THEN 'Twin'
                WHEN LOWER(size) LIKE '%full%' THEN 'Full'
                ELSE size
            END AS adj_size
        FROM wook.stck_zns_comp_sales_anal
        WHERE inch IN ('12', '8', '10', '14', '5', '6')
    ),
    cte_grouped AS (
        SELECT
            brand,
            inch,
            adj_size AS size,
            asin,
            SUM(sales) AS sales_sum,
            SUM(units) AS units_sum
        FROM cte_src
        WHERE yr_month BETWEEN '2025-09' AND '2026-02'
          AND adj_size IN ('King', 'Queen', 'Twin', 'Full')
        GROUP BY brand, inch, adj_size, asin
    )
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY brand, inch, size ORDER BY sales_sum DESC) AS top5_ord
FROM cte_grouped
QUALIFY ROW_NUMBER() OVER (PARTITION BY brand, inch, size ORDER BY sales_sum DESC) <= 5
;

-- mattress mart
CREATE OR REPLACE TABLE tmp.zns2705_mattress_mart AS
WITH
    cte_meta AS (
        SELECT
            base_mst.brand,
            base_mst.inch,
            base_mst.size,
            base_mst.asin,
            base_mst.top5_ord,
            DATE(keepa_mst.listed_since_utc) AS listed_since,
            DATE(keepa_mst.tracking_since_utc) AS tracking_since,
            title
        FROM tmp1.target_asins_temp base_mst
        LEFT JOIN tmp.zns2705_mattress_mst keepa_mst
          ON base_mst.asin = keepa_mst.asin
    ),
    cte_event_day AS (
        SELECT
            UPPER(TRIM(asin)) AS asin,
            metric,
            DATE(event_ts_utc) AS event_date,
            value_num
        FROM tmp.zns2705_mattress_hist_events
        WHERE metric IN ('list_price', 'review_rating', 'sales_rank')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(asin)), metric, DATE(event_ts_utc)
            ORDER BY event_ts_utc DESC
        ) = 1
    ),
    cte_hist AS (
        SELECT
            asin,
            metric,
            event_date,
            LEAD(event_date) OVER (PARTITION BY asin, metric ORDER BY event_date) AS next_event_date,
            value_num
        FROM cte_event_day
    )
SELECT
    b.brand,
    b.inch,
    b.size,
    b.asin,
    b.top5_ord,
    FORMAT_DATE('%Y-%m', a.WeekEnding) AS yr_month,
    MAX(b.title) AS title,
    MIN(b.listed_since) AS listed_since,
    MIN(b.tracking_since) AS tracking_since,
    SUM(a.RetailSales) AS sales,
    SUM(a.UnitsSold) AS units,
    AVG(a.RetailPrice) AS avg_retail_price,
    AVG(listprice.value_num) AS avg_list_price,
    AVG(salesrank.value_num) AS avg_sales_rank,
    AVG(rating.value_num) AS avg_rating
FROM stck.atlas_sales_all a
JOIN cte_meta b
  ON a.RetailerSku = b.asin
LEFT JOIN cte_hist listprice
  ON a.RetailerSku = listprice.asin
 AND listprice.metric = 'list_price'
 AND a.WeekEnding BETWEEN listprice.event_date
                     AND COALESCE(DATE_SUB(listprice.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
LEFT JOIN cte_hist rating
  ON a.RetailerSku = rating.asin
 AND rating.metric = 'review_rating'
 AND a.WeekEnding BETWEEN rating.event_date
                     AND COALESCE(DATE_SUB(rating.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
LEFT JOIN cte_hist salesrank
  ON a.RetailerSku = salesrank.asin
 AND salesrank.metric = 'sales_rank'
 AND a.WeekEnding BETWEEN salesrank.event_date
                     AND COALESCE(DATE_SUB(salesrank.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
WHERE a.WeekEnding >= '2024-09-01'
GROUP BY 1, 2, 3, 4, 5, 6
;

-- beds mart
CREATE OR REPLACE TABLE tmp.zns2705_beds_mart AS
WITH
    cte_target AS (
        SELECT *
        FROM UNNEST([
            STRUCT('Metal' AS material, 'Queen' AS size, 'Zinus & Mellow' AS brand, 'B0BQ1XZ53G' AS asin),
            STRUCT('Metal', 'Queen', 'Lifezone', 'B0CKQSJ333'),
            STRUCT('Metal', 'Queen', 'Allewie', 'B0C69F8DPM'),
            STRUCT('Metal', 'Queen', 'SHA CERLIN', 'B0C9QBVNW8'),
            STRUCT('Metal', 'Full', 'Zinus & Mellow', 'B0CSYJ133B'),
            STRUCT('Metal', 'Full', 'Lifezone', 'B0CNKBTZ6D'),
            STRUCT('Metal', 'Full', 'Allewie', 'B0BPSLYXLR'),
            STRUCT('Metal', 'Full', 'SHA CERLIN', 'B0C9Q5TG97'),
            STRUCT('Metal', 'King', 'Zinus & Mellow', 'B01B8GQCB0'),
            STRUCT('Metal', 'King', 'Lifezone', 'B0DP98NHK4'),
            STRUCT('Metal', 'King', 'Allewie', 'B0BRD166BR'),
            STRUCT('Metal', 'King', 'SHA CERLIN', 'B0DPQ9VYZ9'),
            STRUCT('Wood', 'Queen', 'Zinus & Mellow', 'B07DZT2SZ3'),
            STRUCT('Wood', 'Queen', 'Lifezone', 'B0FRNHHK6D'),
            STRUCT('Wood', 'Queen', 'Allewie', NULL),
            STRUCT('Wood', 'Queen', 'SHA CERLIN', 'B09F95CNJR'),
            STRUCT('Upholstered', 'Queen', 'Zinus & Mellow', 'B0CSYHJM8K'),
            STRUCT('Upholstered', 'Queen', 'Lifezone', 'B0FF4GZN95'),
            STRUCT('Upholstered', 'Queen', 'Allewie', 'B0C9JFV24R'),
            STRUCT('Upholstered', 'Queen', 'SHA CERLIN', 'B0BQCJJQ5R')
        ]) AS t
        WHERE asin IS NOT NULL
    ),
    cte_meta AS (
        SELECT
            base_mst.brand,
            base_mst.material,
            base_mst.size,
            base_mst.asin,
            DATE(keepa_mst.listed_since_utc) AS listed_since,
            DATE(keepa_mst.tracking_since_utc) AS tracking_since,
            title
        FROM cte_target base_mst
        LEFT JOIN tmp.zns2705_beds_mst keepa_mst
          ON base_mst.asin = keepa_mst.asin
    ),
    cte_event_day AS (
        SELECT
            UPPER(TRIM(asin)) AS asin,
            metric,
            DATE(event_ts_utc) AS event_date,
            value_num
        FROM tmp.zns2705_beds_hist_events
        WHERE metric IN ('list_price', 'review_rating', 'sales_rank')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(asin)), metric, DATE(event_ts_utc)
            ORDER BY event_ts_utc DESC
        ) = 1
    ),
    cte_hist AS (
        SELECT
            asin,
            metric,
            event_date,
            LEAD(event_date) OVER (PARTITION BY asin, metric ORDER BY event_date) AS next_event_date,
            value_num
        FROM cte_event_day
    )
SELECT
    b.brand,
    b.material,
    b.size,
    b.asin,
    FORMAT_DATE('%Y-%m', a.WeekEnding) AS yr_month,
    MAX(b.title) AS title,
    MIN(b.listed_since) AS listed_since,
    MIN(b.tracking_since) AS tracking_since,
    SUM(a.RetailSales) AS sales,
    SUM(a.UnitsSold) AS units,
    AVG(a.RetailPrice) AS avg_retail_price,
    AVG(listprice.value_num) AS avg_list_price,
    AVG(salesrank.value_num) AS avg_sales_rank,
    AVG(rating.value_num) AS avg_rating
FROM stck.atlas_sales_all a
JOIN cte_meta b
  ON a.RetailerSku = b.asin
LEFT JOIN cte_hist listprice
  ON a.RetailerSku = listprice.asin
 AND listprice.metric = 'list_price'
 AND a.WeekEnding BETWEEN listprice.event_date
                     AND COALESCE(DATE_SUB(listprice.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
LEFT JOIN cte_hist rating
  ON a.RetailerSku = rating.asin
 AND rating.metric = 'review_rating'
 AND a.WeekEnding BETWEEN rating.event_date
                     AND COALESCE(DATE_SUB(rating.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
LEFT JOIN cte_hist salesrank
  ON a.RetailerSku = salesrank.asin
 AND salesrank.metric = 'sales_rank'
 AND a.WeekEnding BETWEEN salesrank.event_date
                     AND COALESCE(DATE_SUB(salesrank.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
WHERE a.WeekEnding >= '2024-09-01'
GROUP BY 1, 2, 3, 4, 5
;

-- bed frames mart
CREATE OR REPLACE TABLE tmp.zns2705_bed_frames_mart AS
WITH
    cte_target AS (
        SELECT *
        FROM UNNEST([
            STRUCT('14' AS inch, 'Queen' AS size, 'Zinus' AS brand, 'B0CSYBPMW4' AS asin),
            STRUCT('14', 'Queen', 'New Jeto', 'B0B8VQLN6Y'),
            STRUCT('14', 'Queen', 'Amazon Basic', 'B073WRLNS9'),
            STRUCT('14', 'Queen', 'HLIPHA', 'B0CQM7SR4H'),
            STRUCT('18', 'Full', 'Zinus', 'B01M0DX32O'),
            STRUCT('18', 'Full', 'New Jeto', 'B0BN3ZZV4V'),
            STRUCT('18', 'Full', 'Amazon Basic', 'B07RCMX761'),
            STRUCT('18', 'Full', 'HLIPHA', 'B0CQM8ZGSN'),
            STRUCT('14', 'King', 'Zinus', 'B0CL4FQMVK'),
            STRUCT('14', 'King', 'New Jeto', 'B0B8VPF9WC'),
            STRUCT('14', 'King', 'Amazon Basic', 'B073WR5DGC'),
            STRUCT('14', 'King', 'HLIPHA', 'B0CQMB4ZRP'),
            STRUCT('18', 'King', 'Zinus', 'B017YETI16'),
            STRUCT('18', 'King', 'New Jeto', 'B0BN413GP3'),
            STRUCT('18', 'King', 'Amazon Basic', 'B07R7XFD22'),
            STRUCT('18', 'King', 'HLIPHA', 'B0CQM829DG'),
            STRUCT('18', 'Queen', 'Zinus', 'B017YETH8K'),
            STRUCT('18', 'Queen', 'New Jeto', 'B0BN3Y8D9H'),
            STRUCT('18', 'Queen', 'Amazon Basic', 'B07RB7LWMS'),
            STRUCT('18', 'Queen', 'HLIPHA', 'B0CQM8KQ2D')
        ]) AS t
        WHERE asin IS NOT NULL
    ),
    cte_meta AS (
        SELECT
            base_mst.brand,
            base_mst.inch,
            base_mst.size,
            base_mst.asin,
            DATE(keepa_mst.listed_since_utc) AS listed_since,
            DATE(keepa_mst.tracking_since_utc) AS tracking_since,
            title
        FROM cte_target base_mst
        LEFT JOIN tmp.zns2705_bed_frames_mst keepa_mst
          ON base_mst.asin = keepa_mst.asin
    ),
    cte_event_day AS (
        SELECT
            UPPER(TRIM(asin)) AS asin,
            metric,
            DATE(event_ts_utc) AS event_date,
            value_num
        FROM tmp.zns2705_bed_frames_hist_events
        WHERE metric IN ('list_price', 'review_rating', 'sales_rank')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(asin)), metric, DATE(event_ts_utc)
            ORDER BY event_ts_utc DESC
        ) = 1
    ),
    cte_hist AS (
        SELECT
            asin,
            metric,
            event_date,
            LEAD(event_date) OVER (PARTITION BY asin, metric ORDER BY event_date) AS next_event_date,
            value_num
        FROM cte_event_day
    )
SELECT
    b.brand,
    b.inch,
    b.size,
    b.asin,
    FORMAT_DATE('%Y-%m', a.WeekEnding) AS yr_month,
    MAX(b.title) AS title,
    MIN(b.listed_since) AS listed_since,
    MIN(b.tracking_since) AS tracking_since,
    SUM(a.RetailSales) AS sales,
    SUM(a.UnitsSold) AS units,
    AVG(a.RetailPrice) AS avg_retail_price,
    AVG(listprice.value_num) AS avg_list_price,
    AVG(salesrank.value_num) AS avg_sales_rank,
    AVG(rating.value_num) AS avg_rating
FROM stck.atlas_sales_all a
JOIN cte_meta b
  ON a.RetailerSku = b.asin
LEFT JOIN cte_hist listprice
  ON a.RetailerSku = listprice.asin
 AND listprice.metric = 'list_price'
 AND a.WeekEnding BETWEEN listprice.event_date
                     AND COALESCE(DATE_SUB(listprice.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
LEFT JOIN cte_hist rating
  ON a.RetailerSku = rating.asin
 AND rating.metric = 'review_rating'
 AND a.WeekEnding BETWEEN rating.event_date
                     AND COALESCE(DATE_SUB(rating.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
LEFT JOIN cte_hist salesrank
  ON a.RetailerSku = salesrank.asin
 AND salesrank.metric = 'sales_rank'
 AND a.WeekEnding BETWEEN salesrank.event_date
                     AND COALESCE(DATE_SUB(salesrank.next_event_date, INTERVAL 1 DAY), DATE '9999-12-31')
WHERE a.WeekEnding >= '2024-09-01'
GROUP BY 1, 2, 3, 4, 5
;
