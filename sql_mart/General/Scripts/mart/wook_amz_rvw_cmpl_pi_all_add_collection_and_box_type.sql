create or replace table wook.amz_rvw_cmpl_pi_all_add_collection_and_box_type as
WITH
    cte_cf1_dict         AS (
        SELECT
            cf
            , right_cf
        FROM
            meta.cf_typo_cor_dict
        WHERE
            cf_no = 1
    )
    , cte_cf2_dict       AS (
        SELECT
            cf
            , right_cf
        FROM
            meta.cf_typo_cor_dict
        WHERE
            cf_no = 2
    )
SELECT
    t_fact.*
    , cte_cf1_dict.right_cf AS Complaining_factor_Part_1_right
    , cte_cf2_dict.right_cf AS Complaining_factor_Part_2_right
    , m.financial_category
    , m.origin_collection
    , m.main_collection
    , m.new_collection
FROM
    (
        SELECT DISTINCT
            if(rv_1 = 'nan', null, rv_1) as rv_1
            , if(rv2 = 'nan', null, rv2) as rv2
            , t_f.date
            , if(t_m.category = 'nan', null, t_m.category) as category
            , if(customer_material = 'nan', null, customer_material) as customer_material
            , if(t_m.product_description = 'nan', null, t_m.product_description) as product_description
            , if(abb = 'nan', null, abb) as abb
            , if(inch = 'nan', null, inch) as inch
            , if(t_m.size = 'nan', null, t_m.size) as size
            , star_rate
            , if(complaining_factor_part_1 = 'nan', null, complaining_factor_part_1) as complaining_factor_part_1
            , if(complaining_factor_part_2 = 'nan', null, complaining_factor_part_2) as complaining_factor_part_2
            , if(complaining_factor_part_3 = 'nan', null, complaining_factor_part_3) as complaining_factor_part_3
            , if(duration = 'nan' , null, duration) as duration
            , if(t_f.Title = 'nan', null, t_f.Title) as title
            , if(review = 'nan', null, review) as review
            , link
            , if(image = 'nan', null, image) as image
            , IFNULL(
                    IFNULL(
                            REGEXP_EXTRACT(link, r'gp/customer-reviews/([^/?ref]+)'),
                            REGEXP_EXTRACT(link, '.*-reviews-(.*?)-ref')
                    ),
                    REGEXP_EXTRACT(image, r'gp/customer-reviews/([^/]+)')
              ) AS reviewid
            , t_m.asin
            , IF(CONCAT(LOWER(t_m.collection), LOWER(t_m.product_description), LOWER(t_m.abbre)) LIKE '%wonder%', 'SmallBox', CAST(NULL AS STRING)) AS box_type
        FROM
            dw.amz_rvw_cmpl_pi_all t_f
                LEFT JOIN meta.amz_zinus_master_pdt_pi_add_new_col t_m
                    ON t_f.Customer_Material = t_m.sku
    ) t_fact
        LEFT JOIN cte_cf1_dict
            ON t_fact.Complaining_factor_Part_1 = cte_cf1_dict.cf
        LEFT JOIN cte_cf2_dict
            ON t_fact.Complaining_factor_Part_2 = cte_cf2_dict.cf
        LEFT JOIN meta.amz_zns_cat_col_mst m
            ON t_fact.asin = m.asin
WHERE
    cte_cf1_dict.right_cf IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY reviewid, cte_cf1_dict.right_cf, cte_cf2_dict.right_cf ORDER BY t_fact.date) = 1
;
