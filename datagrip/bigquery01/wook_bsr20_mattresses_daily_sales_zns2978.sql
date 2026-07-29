/* -------------------------------------------------------------------------------------------------
   File purpose:
     Mattress 카테고리에서 일별 BSR rank 20 안에 들어온 ASIN을 대표 상품 ASIN으로 선정하고,
     해당 ASIN의 본품 ASIN과 variant ASIN들의 판매 실적을 합산한다.

     판매 실적은 주 단위 Atlas sales와 매칭하므로,
     최종 결과는 일별 BSR Top 20 ASIN별로 해당 주차의 본품 + variant sales 합계를 보여준다.

     주의:
       - 판매 원천(stck.atlas_sales_all)은 주차 단위 데이터이므로,
         결과의 dt는 BSR 관측일이고 sales 값은 해당 dt가 속한 주차의 sales 값이다.
       - 즉, 이 테이블은 일별 판매 집계가 아니라
         "일별 BSR Top 20 대표 ASIN + 본품/variant 포함 해당 주차 sales/YTD" 결과이다.

   Main output:
     asin, dt 기준
       - 일별 BSR rank
       - title, collection
       - 매칭된 Stackline week id(stck_week_id)
       - BSR dt 기준 ISO week id(iso_week_id)
       - 판매 합산 대상 ASIN 목록(본품 + variant)
       - 본품 + variant 기준 해당 주차 판매 수량/매출/평균 가격
       - 본품 + variant 기준 2026년 이후 누적 YTD 판매 수량/매출

   Important assumptions:
     1. rfapi.rf_amz_bsr_all은 하루에 여러 번 수집될 수 있으므로,
        cte_bsr에서는 날짜별 최신 crawlTime 스냅샷만 사용한다.
     2. dw.rf_amz_pdt_zns_comp_daily는 asin + crawl date 기준 여러 건이 있을 수 있으므로,
        cte_pdp에서는 asin + dt별 최신 crawlTime_utc 1건을 사용한다.
     3. variant_expanded 단계에서 asin + dt 기준 상품의 본품 ASIN과 variant ASIN을 판매 조회 대상으로 확장한다.
     4. sales 매칭은 BSR dt를 Stackline 주차 종료일인 토요일로 변환한 뒤,
        Atlas의 WeekEnding과 직접 맞춘다.
     5. YTD sales는 2026-01-01 이후 Atlas weekly sales를 RetailerSku별 WeekId 순서로 누적한다.
     6. stck.atlas_sales_all은 RetailerSku + WeekId 기준 단일 row라고 가정한다.
------------------------------------------------------------------------------------------------- */


/* -------------------------------------------------------------------------------------------------
   일별 Top 20 ASIN 기준 제품 판매 실적 매칭
   -------------------------------------------------------------------------------------------------
   목적:
     - Mattress 일별 BSR rank 20 안에 들어온 ASIN을 대표 상품 ASIN으로 사용한다.
     - 대표 ASIN의 PDP에서 확인되는 본품/variant ASIN을 판매 조회 대상으로 확장한다.
     - BSR 관측일을 Stackline week ending 토요일로 변환해 Atlas weekly sales와 매칭한다.

   핵심 grain:
     - cte_bsr:
         dt + asin
     - cte_pdp:
         dt + asin
     - variant_expanded / variant_lookup:
         dt + asin + variant_asin
     - 최종 결과:
         dt + asin + rank

   처리 방식:
     1) 일별 최신 BSR 스냅샷에서 rank <= 20 ASIN 추출
     2) PDP 수집 데이터와 asin 기준 매칭하되, 같은 날짜 데이터가 없으면 BSR 관측일 이전의 최신 snapshot 사용
     3) variant_asins_flat 콤마 구분 문자열을 UNNEST로 행 분해
     4) 본품 asin도 판매 조회 대상에 포함하기 위해 ARRAY_CONCAT([asin], variant list) 사용
     5) Atlas sales를 RetailerSku + Stackline WeekEnding 기준으로 매칭
     6) 최종적으로 BSR 대표 asin + dt 기준으로 본품 + variant current week sales와 YTD sales 집계
------------------------------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE wook.bsr20_mattresses_daily_product_sales AS
WITH
    cte_bsr AS (
        /* ---------------------------------------------------------------------------------------------
           일별 BSR Top 20 스냅샷 추출

           - 원천 BSR 데이터는 하루에 여러 번 쌓일 수 있으므로,
             날짜별 최신 crawlTime에 해당하는 row만 사용한다.
           - 해당 최신 스냅샷 안에서 rank <= 20인 ASIN만 유지한다.
           - dt는 BSR 관측일이며, sales 매칭용 Stackline week ending은 이후 cte_pdp에서 생성한다.
        --------------------------------------------------------------------------------------------- */
        SELECT
            DATE(initialTime) AS dt
            , asin
            , rank
        FROM
            rfapi.rf_amz_bsr_all
        WHERE
            fullCategory = 'Any Department > Home & Kitchen > Furniture > Bedroom Furniture > Mattresses & Box Springs > Mattresses'
            AND rank <= 20
            AND initialTime >= '2026-01-01'
        QUALIFY
            RANK() OVER (PARTITION BY DATE (initialTime) ORDER BY crawlTime DESC) = 1
    )
    , cte_pdp AS (
        /* -----------------------------------------------------------------------------------------
           일별 Top 20 ASIN과 PDP 상품/variant 수집 테이블 매칭

           - cte_bsr:
               dt + asin 기준 일별 Top 20 ASIN을 보유한다.

           - dw.rf_amz_pdt_zns_comp_daily:
               ASIN별 수집 시점의 title, variant_asins_flat 정보를 보유한다.

           - 조인 조건:
               같은 asin
               PDP crawl date는 BSR dt 이전 또는 같은 날짜만 사용한다.

           - QUALIFY:
               같은 asin + dt 안에서 BSR dt에 가장 가까운 최신 crawlTime_utc 1건만 사용한다.
               같은 날짜 PDP가 있으면 해당 날짜의 최신 snapshot을 사용하고,
               없으면 BSR dt 이전의 가장 최근 snapshot을 사용한다.
        ----------------------------------------------------------------------------------------- */
        SELECT DISTINCT
            a.request_asin
            , COALESCE(a.asin, b.asin) AS asin
            , a.title
            , a.variant_asins_flat
--             , b.* EXCEPT (asin, dt)
            , b.dt
            , b.rank
--             , FORMAT_DATE('%Y-%U', b.dt) as yr_wk
            , DATE_ADD(DATE_TRUNC(b.dt, WEEK(SUNDAY)), INTERVAL 6 DAY) AS stackline_week_ending
        FROM
            cte_bsr b
                LEFT JOIN dw.rf_amz_pdt_zns_comp_daily a
                    ON a.asin = b.asin
                       --AND DATE(SUBSTRING(a.crawlTime_utc, 1, 10)) <= b.dt
        QUALIFY
            --ROW_NUMBER() OVER (PARTITION BY b.asin, b.dt ORDER BY a.crawlTime_utc DESC) = 1
            ROW_NUMBER() OVER (
            PARTITION BY b.asin, b.dt
            ORDER BY
                CASE WHEN DATE(SUBSTRING(a.crawlTime_utc,1,10)) <= b.dt THEN 0 ELSE 1 END, -- 과거 기록 우선
                ABS(DATE_DIFF(DATE(SUBSTRING(a.crawlTime_utc,1,10)), b.dt, DAY))            -- 그 다음 가장 가까운 날짜
            ) = 1
    )
    , variant_expanded AS (
        /* -----------------------------------------------------------------------------------------
           variant_asins_flat 분해 및 본품 asin 포함

           목적:
             - variant_asins_flat은 콤마 구분 문자열이므로 SPLIT + UNNEST로 행 분해한다.
             - 판매 실적 조회 시 variant뿐 아니라 본품 asin도 포함해야 하므로,
               ARRAY_CONCAT([asin], SPLIT(...))을 사용한다.

           예:
             asin = A
             variant_asins_flat = 'B,C,D'
             결과 variant_asin:
               A
               B
               C
               D

           variant_asins_flat이 NULL 또는 빈 문자열인 경우:
             ARRAY_CONCAT([asin], SPLIT('', ',')) 결과에 빈 문자열이 섞일 수 있다.
             UNNEST 직전에 NULLIF(TRIM(...), '')로 빈 문자열을 NULL 처리하고,
             DISTINCT로 본품/variant 목록 내 중복 ASIN을 제거한다.
             이후 variant_lookup에서 v.variant_asin IS NOT NULL 조건으로 NULL을 제거한다.

           중복 제거:
             - 본품 asin이 variant_asins_flat에도 포함된 경우
             - variant_asins_flat 문자열 안에 같은 ASIN이 반복된 경우
             위 케이스는 ARRAY(SELECT DISTINCT ...) 단계에서 1건으로 정리한다.
        ----------------------------------------------------------------------------------------- */
        SELECT
            pdp.* EXCEPT (variant_asins_flat)
            , variant_asin
        FROM
            cte_pdp pdp
                LEFT JOIN UNNEST(
                    ARRAY(
                        SELECT DISTINCT
                            NULLIF(TRIM(raw_variant_asin), '')
                        FROM
                            UNNEST(ARRAY_CONCAT([asin], SPLIT(IFNULL(variant_asins_flat, ''), ','))) AS raw_variant_asin
                    )
                ) AS variant_asin
    )
    , cte_ytd_sales as (
        /* -----------------------------------------------------------------------------------------
           Atlas weekly sales 기준 YTD 계산

           - stck.atlas_sales_all은 주차 단위 판매 데이터이다.
           - 2026-01-01 이후 WeekEnding만 사용한다.
           - RetailerSku별 WeekId 순서로 UnitsSold / RetailSales를 누적한다.
           - 현재 로직은 variant_expanded에서 확장된 본품/variant ASIN만 먼저 필터링한다.
           - RetailerSku + WeekId는 단일 row라고 가정하므로, 별도 주차 단위 사전 집계 없이 YTD를 계산한다.
        ----------------------------------------------------------------------------------------- */
        WITH
            cte_target_asins AS (
                SELECT DISTINCT
                    v.variant_asin
                FROM
                    variant_expanded v
            )
        SELECT
            RetailerSku
            , Brand
            , WeekEnding
            , WeekId
            , RetailSales
            , UnitsSold
            , RetailPrice
            , SUM(UnitsSold) OVER (PARTITION BY RetailerSku ORDER BY WeekId ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ytd_units_sold -- 연도 변경과 관계 없이 26년 이후 누적
--             , SUM(UnitsSold) OVER (PARTITION BY RetailerSku, EXTRACT(YEAR FROM WeekEnding) ORDER BY WeekId ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ytd_units_sold -- 연도 변경시(2026 -> 2027) 누적값 리셋

            , SUM(RetailSales) OVER (PARTITION BY RetailerSku ORDER BY WeekId ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ytd_retail_sales
--             , SUM(RetailSales) OVER (PARTITION BY RetailerSku, EXTRACT(YEAR FROM WeekEnding) ORDER BY WeekId ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ytd_retail_sales -- 연도 변경시(2026 -> 2027) 누적값 리셋
        FROM
            stck.atlas_sales_all a
                JOIN cte_target_asins b
                    ON a.RetailerSku = b.variant_asin
        WHERE
            --             EXISTS (select 1 from variant_expanded b where a.RetailerSku = b.variant_asin)
            --             AND
--             a.WeekEnding >= '2026-01-01'
            a.WeekId >= 202553 -- Cluster
    )
    , variant_lookup   AS (
        /* -----------------------------------------------------------------------------------------
           판매 조회 대상 ASIN 기준 Atlas sales 매칭

           - variant_expanded.variant_asin을 cte_ytd_sales.RetailerSku와 매칭한다.
           - 주차는 BSR dt가 속한 Stackline week ending 토요일과 Atlas WeekEnding을 맞춘다.
           - Brand는 공백/비출력 문자 제거 후 대문자로 정규화한다.

           주의:
             - Atlas는 RetailerSku + WeekId 기준 단일 row라고 가정한다.
             - current_week_* 컬럼은 dt 단위 판매가 아니라 dt가 속한 주차의 판매 값이다.
        ----------------------------------------------------------------------------------------- */
        SELECT
            v.asin
            , v.variant_asin
            , v.rank
            , v.title
            , v.dt
--             , v.yr_wk
            , v.stackline_week_ending

            , UPPER(REGEXP_REPLACE(TRIM(stck.Brand), r'[^[:print:]]', '')) AS brand
--             , stck.WeekEnding
            , stck.WeekId
            , stck.RetailSales as current_week_retail_sales
            , stck.UnitsSold as current_week_units_sold
            , stck.RetailPrice as current_week_retail_price
            , stck.ytd_units_sold
            , stck.ytd_retail_sales
        FROM
            variant_expanded v
                LEFT JOIN cte_ytd_sales stck
                    ON v.variant_asin = stck.RetailerSku
                        AND v.stackline_week_ending = stck.WeekEnding
        WHERE
            v.variant_asin IS NOT NULL
    )
/* -------------------------------------------------------------------------------------------------
   최종 일별 BSR Top 20 대표 ASIN별 본품 + variant 주차 판매 집계

   grain:
     - BSR Top 20에 들어온 대표 asin + dt + rank
     - collection, title, stck_week_id, iso_week_id는 위 grain에 부가되는 속성이다.

   집계값:
     - brand:
         현재는 MAX(brand)로 대표 brand 1개만 사용
         variant별 brand가 다를 수 있으면 STRING_AGG(DISTINCT brand) 검토 가능

     - variant_asins:
         대표 asin의 판매 합산 대상으로 확장된 본품 + variant ASIN 목록
         DISTINCT 제거 후 ASIN 오름차순으로 콤마 결합

     - current_week_retail_price:
         매칭된 RetailPrice의 평균

     - current_week_units_sold / current_week_retail_sales:
         본품 + variant_asin 기준 매칭된 해당 주차 판매 수량/금액 합산

     - ytd_units_sold / ytd_retail_sales:
         본품 + variant_asin 기준 매칭된 2026년 이후 누적 판매 수량/금액 합산
------------------------------------------------------------------------------------------------- */
SELECT
    v.asin
    , v.rank
    , v.title
    , COALESCE(z.new_collection, '-') AS collection
    , v.dt
--     , yr_wk
--     , v.stackline_week_ending
    , v.WeekId as stck_week_id
    , FORMAT_DATE('%G-%V', v.dt) as iso_week_id

    , MAX(v.brand) AS brand

    , STRING_AGG(DISTINCT v.variant_asin, ',' ORDER BY v.variant_asin) AS variant_asins
    -- , COUNT(DISTINCT v.variant_asin) AS variant_cnt
    , AVG(v.current_week_retail_price) AS current_week_retail_price
    , SUM(v.current_week_units_sold) AS current_week_units_sold
    , SUM(v.current_week_retail_sales) AS current_week_retail_sales
    , SUM(v.ytd_units_sold) AS ytd_units_sold
    , SUM(v.ytd_retail_sales) AS ytd_retail_sales
FROM
    variant_lookup v
        LEFT JOIN meta.amz_zinus_master_pdt_pi_enriched z
            ON v.asin = z.asin
WHERE
--     v.stackline_week_ending <= (select MAX(DATE(s.WeekEnding)) FROM cte_ytd_sales s)
    v.WeekId IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5, 6
ORDER BY
    dt DESC, rank;
