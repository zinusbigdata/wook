BEGIN

DECLARE pivot_cols STRING;
DECLARE dyn_sql STRING;

SET pivot_cols = (
    SELECT
        STRING_AGG(
            FORMAT(
                "'%s' AS `%s`",
                REPLACE(cf1_val, "'", "''"),
                CONCAT(
                    'cf1_',
                    REGEXP_REPLACE(LOWER(cf1_val), r'[^a-z0-9]+', '_')
                )
            ),
            ', '
            ORDER BY cf1_val
        )
    FROM (
        SELECT DISTINCT
            Complaining_factor_Part_1_right AS cf1_val
        FROM
            wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type
        WHERE
            Complaining_factor_Part_1_right IS NOT NULL
    )
);

IF pivot_cols IS NULL THEN
    SET pivot_cols = "'__no_value__' AS `cf1_no_value`";
END IF;

SET dyn_sql = FORMAT(
    """
    CREATE OR REPLACE TABLE wook.amz_rvw_cmpl_pivot_and_sales_agg AS
    WITH vc_sales AS (
        SELECT
            FORMAT_DATE('%%Y-Q%%Q', date) AS sales_quarter
            , asin
            , SUM(ordered_units)   AS ordered_units
            , SUM(ordered_revenue) AS ordered_revenue
            , SUM(shipped_units)   AS shipped_units
            , SUM(shipped_revenue) AS shipped_revenue
        FROM vc.amz_vc_sales_daily_all
        WHERE asin IS NOT NULL
        GROUP BY 1, 2
    ),
    review_base AS (
        SELECT
            f.asin
            , FORMAT_DATE('%%Y-Q%%Q', DATE(f.`date`)) AS review_quarter
            , f.box_type
            , f.financial_category
            , f.origin_collection
            , f.main_collection
            , f.new_collection
            , f.Complaining_factor_Part_1_right
        FROM wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type f
        WHERE f.Complaining_factor_Part_1_right IS NOT NULL
          AND f.asin IS NOT NULL
    ),
    review_pivot AS (
        SELECT *
        FROM review_base
        PIVOT (
            COUNT(1)
            FOR Complaining_factor_Part_1_right IN (%s)
        )
    )
    SELECT
        p.*
        , s.ordered_units
        , s.ordered_revenue
        , s.shipped_units
        , s.shipped_revenue
    FROM review_pivot p
    LEFT JOIN vc_sales s
        ON p.asin = s.asin
       AND p.review_quarter = s.sales_quarter
    ORDER BY
        review_quarter DESC
        , asin
    """,
    pivot_cols
);

EXECUTE IMMEDIATE dyn_sql;

END;