/*
    Description:
        dw.cs_voc_cf 를 CS VOC complaint factor 대시보드용 factor 상세 마트로 정제한다.
        AMZ 참조 mart.amz_us_zinus_crwl_rpa_rvw (amz_us_zns_rvw_with_cf.sql) 와 유사한 역할.

    Purpose:
        Customer Review / VOC Analysis Dashboard - CS VOC CF Trend Page

    Input:
        dw.cs_voc_cf
            - voc_id 기준 factor fan-out, cf1/cf2/cf3 분할, mdm 사이즈 보강 완료

    Output:
        Table:
            mart.cs_voc_cf_factor

        Schema:
            mart

    Logic:
        1. voc_id 단위 cf1/cf2/cf3 distinct 문자열 agg (cf1_agg, cf2_agg, cf3_agg)
        2. date(TIMESTAMP) → DATE, yr_month 파생
        3. meta.wk_calendar 로 yr_wk 부착
        4. 차원 컬럼 COALESCE(..., 'N/A') 표준화
        5. meta.cs_channel_mapping 으로 channel → channel_group(상위 그룹) 부착
           (미매핑/신규 채널은 'Others')
        6. voc_id + cf1 + cf2 + cf3 + factor_seq 기준 중복 제거

    Change History:
        2026-06-05 / ZNS-2913 / v1.0
            - 최초 생성
            - dw.cs_voc_cf 기준 CS VOC factor mart 생성

        2026-06-05 / ZNS-2913 / v1.1
            - claim_issue, description, channel, hq_keywords, reason, sub_reason 컬럼 추가
            - channel, zformat COALESCE(..., 'N/A') 표준화 (BI 필터용)

        2026-06-17 / ZNS-2913 / v1.2
            - mdm 보강 컬럼 추가: zcollet(Collection), zzmaktx(Material Text)
            - COALESCE(..., 'N/A') 표준화 (BI 필터용)

        2026-06-17 / ZNS-2913 / v1.3
            - channel_group 컬럼 추가 (meta.cs_channel_mapping LEFT JOIN)
            - 채널 상위 그룹핑은 dw 원본(channel) 보존하고 mart 에서만 반영

        2026-06-17 / ZNS-2913 / v1.4
            - zformat 을 profile(숫자)/size(영문) 로 분리하여 컬럼 추가, zformat 컬럼 제거
            - 빈 값(NULL/'N/A' 등)은 패턴 미스매치로 profile/size NULL 처리(행은 유지)
            - 분리/파생은 dw 원본(zformat) 보존하고 mart 에서만 반영

    Created Date:
        2026-06-05

    Version:
        1.4
*/

CREATE OR REPLACE TABLE mart.cs_voc_cf_factor AS
WITH
    cte_cf_agg       AS (
        SELECT
            voc_id
            , TRIM(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT cf1 ORDER BY cf1), ',')) AS cf1_agg
            , TRIM(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT cf2 ORDER BY cf2), ',')) AS cf2_agg
            , TRIM(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT cf3 ORDER BY cf3), ',')) AS cf3_agg
        FROM
            dw.cs_voc_cf
        WHERE
            voc_id IS NOT NULL
        GROUP BY
            voc_id
    )
    , cte_base       AS (
        SELECT
            f.voc_id
            , COALESCE(f.sku, 'N/A') AS sku
            , COALESCE(f.category, 'N/A') AS category
            , COALESCE(f.matt_or_nonmatt, 'N/A') AS matt_or_nonmatt
            , DATE(f.date) AS date
            , FORMAT_DATE('%Y-%m', DATE(f.date)) AS yr_month
            , f.cf1
            , f.cf2
            , f.cf3
            , f.factor3
            , f.factor_seq
            , a.cf1_agg
            , a.cf2_agg
            , a.cf3_agg
            , TRIM(f.voc_text) AS voc_text
            , f.claim_issue
            , TRIM(f.description) AS description
            , COALESCE(f.channel, 'N/A') AS channel
            , f.hq_keywords
            , f.reason
            , f.sub_reason
            , f.zjde_size
            -- zformat(<숫자><영문>, 예 '12Q') 분리: 숫자=profile, 영문=size
            -- 전체 패턴(^숫자+영문$)에 맞을 때만 추출 → NULL/'N/A' 등 빈값은 자동 NULL (쓰레기값 방지)
            , REGEXP_EXTRACT(f.zformat, r'^([0-9]+)[A-Za-z]+$') AS profile
            , REGEXP_EXTRACT(f.zformat, r'^[0-9]+([A-Za-z]+)$') AS size
            , COALESCE(f.zcollet, 'N/A') AS zcollet
            , COALESCE(f.zzmaktx, 'N/A') AS zzmaktx
            , f.factor1
            , f.factor2
            , f.duration
            , f.manufactured_date
            , f.verified_purchase
            , f.factor3_detail
            , f.cf1_reasoning
            , f.cf1_evidence
            , f.cf2_reasoning
            , f.cf2_evidence
            , f.created_datetime
            , f.modified_datetime
        FROM
            dw.cs_voc_cf f
                LEFT JOIN cte_cf_agg a
                    ON f.voc_id = a.voc_id
        WHERE
            f.date IS NOT NULL
    )
SELECT
    b.*
    -- 채널 상위 그룹: meta.cs_channel_mapping 참조, 미매핑/신규 채널은 'Others'
    , COALESCE(cm.channel_group, 'Others') AS channel_group
    , wk.yr_wk
FROM
    cte_base b
        LEFT JOIN meta.cs_channel_mapping cm
            ON b.channel = cm.channel_std
        LEFT JOIN meta.wk_calendar wk
            ON b.date BETWEEN wk.start_date AND wk.end_date
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY voc_id, cf1, cf2, cf3, factor_seq
        ORDER BY date DESC
    ) = 1
;