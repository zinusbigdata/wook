-- 0) AMZ SiteWalk SKU List
  
CREATE OR REPLACE TABLE tmp.amz_sitewalk_sku_list AS
SELECT 
      asin
    , MAX(zinus_sku_cd) AS zinus_sku_cd
    , MAX(zinus_sku_nm) AS zinus_sku_nm
    , MAX(collection) AS collection
    , MAX(abc_in) AS abc_in
    , MAX(prdct_h_lv1) AS prdct_h_lv1
FROM vs_pb.amz_retail_sales_1p_3p
WHERE asin is not null and year>=2024 -- and ord_qty_ty>0 
GROUP BY 1
;


  /*
  - variation
  - bullet 
  - title
  - kids / toddler  / baby / Youth word
  - browse nodes :: 
  - buyboxsellor
  - PDP SUPPRESSION with Inventory, Price Match, Net PPM
  - FREQUENTLY RETURNED Unit
  - PDP
  */

  -- 1) AMZ Rainforest API DATA
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_all AS
  WITH TMP2 AS (
  WITH TMP1 AS (
  SELECT 
     A.asin -- page availability
    ,A.title
    ,A.variant_asins_flat
    ,A.keywords
    ,A.feature_bullets
    ,A.attributes
    ,A.description
    ,A.bw_ff_type
    ,A.bw_availability_type -- in_stock
    ,A.bw_availability_raw
    ,COALESCE(A.bw_ff_amazon_seller_name, A.bw_ff_third_party_seller_name) AS bw_ff_amazon_seller_name  
    ,A.country_of_origin  
    ,A.bw_rrp_value   
    ,A.bw_price_value 
    ,A.salesrank1
    ,A.salesrank2
    ,A.salesrank3
    ,A.salesrank4
    ,A.salesrank_category1
    ,A.salesrank_category2
    ,A.salesrank_category3
    ,A.salesrank_category4
    ,A.bw_deal_with_deal_with_deal_shown
    ,A.bw_deal_with_deal_raw

    ,A.rating
    ,A.ratings_total
    ,A.rating_breakdown_five_star_percentage
    ,A.rating_breakdown_five_star_count
    ,A.rating_breakdown_four_star_percentage
    ,A.rating_breakdown_four_star_count
    ,A.rating_breakdown_three_star_percentage
    ,A.rating_breakdown_three_star_count
    ,A.rating_breakdown_two_star_percentage
    ,A.rating_breakdown_two_star_count
    ,A.rating_breakdown_one_star_percentage
    ,A.rating_breakdown_one_star_count

    ,CAST(DATETIME(A.crawlTime_pcf) AS DATE) AS date
    ,DATETIME(crawlTime_pcf) AS date_time
  FROM dw.rf_amz_sitewalk_daily A 
  INNER JOIN tmp.amz_sitewalk_sku_list B ON A.request_asin=B.asin 
  WHERE CAST(DATETIME(A.crawlTime_pcf) AS DATE)>="2024-01-01"

   UNION ALL 

  SELECT 
     A.asin -- page availability
    ,A.title
    ,A.variant_asins_flat
    ,A.keywords
    ,A.feature_bullets
    ,A.attributes
    ,A.description
    ,A.bw_ff_type
    ,A.bw_availability_type -- in_stock
    ,A.bw_availability_raw
    ,COALESCE(A.bw_ff_amazon_seller_name, A.bw_ff_third_party_seller_name) AS bw_ff_amazon_seller_name  
    ,A.country_of_origin  
    ,A.bw_rrp_value   
    ,A.bw_price_value 
    ,A.salesrank1
    ,A.salesrank2
    ,A.salesrank3
    ,A.salesrank4
    ,A.salesrank_category1
    ,A.salesrank_category2
    ,A.salesrank_category3
    ,A.salesrank_category4
    ,A.bw_deal_with_deal_with_deal_shown
    ,A.bw_deal_with_deal_raw

      ,A.rating
    ,A.ratings_total
    ,A.rating_breakdown_five_star_percentage
    ,A.rating_breakdown_five_star_count
    ,A.rating_breakdown_four_star_percentage
    ,A.rating_breakdown_four_star_count
    ,A.rating_breakdown_three_star_percentage
    ,A.rating_breakdown_three_star_count
    ,A.rating_breakdown_two_star_percentage
    ,A.rating_breakdown_two_star_count
    ,A.rating_breakdown_one_star_percentage
    ,A.rating_breakdown_one_star_count

    ,CAST(DATETIME(A.crawlTime_pcf) AS DATE) AS date
    ,DATETIME(crawlTime_pcf) AS date_time
  FROM dw.rf_amz_comp_price_match_sitewalk A 
  INNER JOIN tmp.amz_sitewalk_sku_list B ON A.request_asin=B.asin
  WHERE CAST(DATETIME(A.crawlTime_pcf) AS DATE)>="2024-01-01"


   UNION ALL 

  SELECT 
     A.asin -- page availability
    ,A.title
    ,A.variant_asins_flat
    ,A.keywords
    ,A.feature_bullets
    ,A.attributes
    ,A.description
    ,A.bw_ff_type
    ,A.bw_availability_type -- in_stock
    ,A.bw_availability_raw
    ,COALESCE(A.bw_ff_amazon_seller_name, A.bw_ff_third_party_seller_name) AS bw_ff_amazon_seller_name  
    ,A.country_of_origin  
    ,A.bw_rrp_value   
    ,A.bw_price_value 
    ,A.salesrank1
    ,A.salesrank2
    ,A.salesrank3
    ,A.salesrank4
    ,A.salesrank_category1
    ,A.salesrank_category2
    ,A.salesrank_category3
    ,A.salesrank_category4
    ,A.bw_deal_with_deal_with_deal_shown
    ,A.bw_deal_with_deal_raw

      ,A.rating
    ,A.ratings_total
    ,A.rating_breakdown_five_star_percentage
    ,A.rating_breakdown_five_star_count
    ,A.rating_breakdown_four_star_percentage
    ,A.rating_breakdown_four_star_count
    ,A.rating_breakdown_three_star_percentage
    ,A.rating_breakdown_three_star_count
    ,A.rating_breakdown_two_star_percentage
    ,A.rating_breakdown_two_star_count
    ,A.rating_breakdown_one_star_percentage
    ,A.rating_breakdown_one_star_count

    ,CAST(DATETIME(A.crawlTime_pcf) AS DATE) AS date
    ,DATETIME(crawlTime_pcf) AS date_time
  FROM dw.rf_amz_comp_price_match_sitewalk A 
  INNER JOIN tmp.amz_sitewalk_sku_list B ON A.asin=B.asin

  ) 
  SELECT 
     A.asin -- page availability
    ,A.title
    ,A.variant_asins_flat
    ,A.keywords
    ,A.feature_bullets
    ,A.attributes
    ,A.description
    ,A.bw_ff_type
    ,A.bw_availability_type -- in_stock
    ,A.bw_availability_raw
    ,A.bw_ff_amazon_seller_name  
    ,A.country_of_origin  
    ,A.bw_rrp_value   
    ,A.bw_price_value 
    ,A.salesrank1
    ,A.salesrank2
    ,A.salesrank3
    ,A.salesrank4
    ,A.salesrank_category1
    ,A.salesrank_category2
    ,A.salesrank_category3
    ,A.salesrank_category4
    ,A.bw_deal_with_deal_with_deal_shown
    ,A.bw_deal_with_deal_raw
    ,A.rating
    ,A.ratings_total
    ,A.rating_breakdown_five_star_percentage
    ,A.rating_breakdown_five_star_count
    ,A.rating_breakdown_four_star_percentage
    ,A.rating_breakdown_four_star_count
    ,A.rating_breakdown_three_star_percentage
    ,A.rating_breakdown_three_star_count
    ,A.rating_breakdown_two_star_percentage
    ,A.rating_breakdown_two_star_count
    ,A.rating_breakdown_one_star_percentage
    ,A.rating_breakdown_one_star_count

    ,A.date
    ,A.date_time 
    ,ROW_NUMBER() OVER (PARTITION BY asin, date ORDER BY date_time DESC) AS day_filter_rnk
  FROM TMP1 A)
  select 
     A.asin -- page availability
    ,A.title
    ,A.variant_asins_flat
    ,A.keywords
    ,A.feature_bullets
    ,A.attributes
    ,A.description
    ,A.bw_ff_type
    ,A.bw_availability_type -- in_stock
    ,A.bw_availability_raw
    ,A.bw_ff_amazon_seller_name  
    ,A.country_of_origin  
    ,A.bw_rrp_value   
    ,A.bw_price_value 
    ,A.salesrank1
    ,A.salesrank2
    ,A.salesrank3
    ,A.salesrank4
    ,A.salesrank_category1
    ,A.salesrank_category2
    ,A.salesrank_category3
    ,A.salesrank_category4
    ,A.bw_deal_with_deal_with_deal_shown
    ,A.bw_deal_with_deal_raw
        ,A.rating
    ,A.ratings_total
    ,A.rating_breakdown_five_star_percentage
    ,A.rating_breakdown_five_star_count
    ,A.rating_breakdown_four_star_percentage
    ,A.rating_breakdown_four_star_count
    ,A.rating_breakdown_three_star_percentage
    ,A.rating_breakdown_three_star_count
    ,A.rating_breakdown_two_star_percentage
    ,A.rating_breakdown_two_star_count
    ,A.rating_breakdown_one_star_percentage
    ,A.rating_breakdown_one_star_count

    ,A.date
    ,A.date_time  
  FROM TMP2 A
  WHERE day_filter_rnk=1
  ;



  -- 2) VARIATION CHANGE 
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_variation AS
  WITH TMP3 AS (
    WITH TMP2 AS (
      WITH TMP1 AS (
        SELECT 
           A.asin 
          ,A.variant_asins_flat
          ,MAX(A.date_time) AS date_time
        FROM tmp.amz_sitewalk_raw_all A
        GROUP BY 1,2)
      SELECT 
        *
       ,ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date_time DESC) AS filter_rnk
      FROM TMP1)
    SELECT *
    FROM TMP2
    WHERE filter_rnk<=2)
  SELECT 
    A.asin
   ,A.variant_asins_flat AS variant_current
   ,A.date_time AS variant_current_time
   
   ,B.variant_asins_flat AS variant_before
   ,B.date_time AS variant_before_time

   ,(LENGTH(A.variant_asins_flat) - LENGTH(REGEXP_REPLACE(A.variant_asins_flat, ',', '')) + 1) AS var_asin_num_current
   ,(LENGTH(B.variant_asins_flat) - LENGTH(REGEXP_REPLACE(B.variant_asins_flat, ',', '')) + 1) AS var_asin_num_before

   ,REGEXP_REPLACE(REGEXP_REPLACE(A.variant_asins_flat, REPLACE(REPLACE(B.variant_asins_flat, ',,', ' '),",", " "), ""),","," ") AS var_asin_added 
   ,REGEXP_REPLACE(REGEXP_REPLACE(B.variant_asins_flat, REPLACE(REPLACE(A.variant_asins_flat, ',,', ' '),",", " "), ""),","," ") AS var_asin_excluded

  FROM (SELECT * FROM TMP3 WHERE filter_rnk=1) A
  LEFT OUTER JOIN (SELECT * FROM TMP3 WHERE filter_rnk=2) B ON A.asin=B.asin 
  ;




  -- 3) Title 
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_title AS
  WITH TMP3 AS (
    WITH TMP2 AS (
      WITH TMP1 AS (
        SELECT 
           A.asin 
          ,A.item_name AS title
          ,MAX(DATETIME(A.load_datetime)) AS date_time
        FROM ods.vc_catalog_daily A
        INNER JOIN tmp.amz_sitewalk_sku_list B ON A.asin=B.asin 
        WHERE A.item_name IS NOT NULL
        GROUP BY 1,2)
      SELECT 
        *
       ,ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date_time DESC) AS filter_rnk
      FROM TMP1)
    SELECT *
    FROM TMP2
    WHERE filter_rnk<=2)
  SELECT 
    A.asin
   ,A.title AS title_current
   ,A.date_time AS title_current_time
   
   ,B.title AS title_before
   ,B.date_time AS title_before_time

  FROM (SELECT * FROM TMP3 WHERE filter_rnk=1) A
  LEFT OUTER JOIN (SELECT * FROM TMP3 WHERE filter_rnk=2) B ON A.asin=B.asin 
  --WHERE B.title IS NOT NULL
  ;




  -- 4) Bullet 
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_feature_bullets AS
  WITH TMP3 AS (
    WITH TMP2 AS (
      WITH TMP1 AS (
        SELECT 
           A.asin 
          ,A.feature_bullets
          ,MAX(A.date_time) AS date_time
        FROM tmp.amz_sitewalk_raw_all A
        WHERE A.feature_bullets IS NOT NULL
        GROUP BY 1,2)
      SELECT 
        *
       ,ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date_time DESC) AS filter_rnk
      FROM TMP1)
    SELECT *
    FROM TMP2
    WHERE filter_rnk<=2)
  SELECT 
    A.asin
   ,A.feature_bullets AS feature_bullets_current
   ,A.date_time AS feature_bullets_current_time
   
   ,B.feature_bullets AS feature_bullets_before
   ,B.date_time AS feature_bullets_before_time

  FROM (SELECT * FROM TMP3 WHERE filter_rnk=1) A
  LEFT OUTER JOIN (SELECT * FROM TMP3 WHERE filter_rnk=2) B ON A.asin=B.asin 
  WHERE B.feature_bullets IS NOT NULL
  ;


  -- 5) Kids / Toddler / Baby Description 
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_kids_alert AS
  WITH TMP2 AS (
  WITH TMP1 AS (
      SELECT 
           A.asin 
          ,A.keywords
          ,A.feature_bullets
          ,A.attributes
          ,MAX(A.date_time) AS date_time
        FROM tmp.amz_sitewalk_raw_all A
        GROUP BY 1,2,3,4)
    SELECT 
      *
     ,ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date_time DESC) AS filter_rnk
    FROM TMP1)
  SELECT 
    asin
   ,CASE WHEN REGEXP_CONTAINS(LOWER(feature_bullets),r'kids|toddler|baby|infant') THEN "Y" ELSE "N" END is_kids -- keywords||" "||feature_bullets||" "||attributes
   ,CASE WHEN REGEXP_CONTAINS(LOWER(keywords),r'kids|toddler|baby|infant') THEN "Y" ELSE "N" END is_kids_keywords
   ,CASE WHEN REGEXP_CONTAINS(LOWER(feature_bullets),r'kids|toddler|baby|infant') THEN "Y" ELSE "N" END is_kids_bullet
   ,CASE WHEN REGEXP_CONTAINS(LOWER(attributes),r'kids|toddler|baby|infant') THEN "Y" ELSE "N" END is_kids_attributes
   
   --,SUBSTR(pdp_string, LEAST(STRPOS(LOWER(pdp_string),'kids'),
   --  STRPOS(LOWER(pdp_string),'toddler'),STRPOS(LOWER(pdp_string),'baby'),STRPOS(LOWER(pdp_string),'infant'))-15,60) AS pdp_string
   ,A.keywords
   ,A.feature_bullets
   ,A.attributes
  FROM TMP2 A
  WHERE filter_rnk=1
  ;


  -- 6) Retail Price 
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_retail_price AS
  WITH TMP3 AS (
    WITH TMP2 AS (
      WITH TMP1 AS (
        WITH PRICE AS (
        SELECT 
           A.asin 
          ,A.bw_price_value
          ,MAX(A.date_time) AS date_time
        FROM tmp.amz_sitewalk_raw_all A
        WHERE A.bw_price_value IS NOT NULL
        GROUP BY 1,2
         union all 
        select 
          asin
         ,BUY_BOX_SHIPPING
         ,DATETIME(BUY_BOX_SHIPPING_time)
        from  tmp.zinus_amz_bb_ship_price_agg 
        where BUY_BOX_SHIPPING is not null ) 
        SELECT 
           A.asin 
          ,A.bw_price_value
          ,MAX(A.date_time) AS date_time
        FROM PRICE A
        GROUP BY 1,2
        )
      SELECT 
        *
       ,ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date_time DESC) AS filter_rnk
      FROM TMP1)
    SELECT *
    FROM TMP2
    WHERE filter_rnk<=2)
  SELECT 
    A.asin
   ,A.bw_price_value AS bw_price_value_current
   ,A.date_time AS bw_price_value_current_time
   
   ,B.bw_price_value AS bw_price_value_before
   ,B.date_time AS bw_price_value_before_time

  FROM (SELECT * FROM TMP3 WHERE filter_rnk=1) A
  LEFT OUTER JOIN (SELECT * FROM TMP3 WHERE filter_rnk=2) B ON A.asin=B.asin 
  WHERE B.bw_price_value IS NOT NULL
  ;




  -- 7) Current COO, Seller, Suppression, Rank Nodes
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_current_buybox AS
    WITH TMP1 AS (
        SELECT 
           A.asin 
          ,A.bw_availability_type -- in_stock
          ,A.bw_availability_raw
          ,A.bw_ff_amazon_seller_name  
          ,A.bw_price_value
          ,A.country_of_origin  
          ,A.salesrank2
          ,A.salesrank3
          ,A.salesrank4
          ,A.salesrank_category2
          ,A.salesrank_category3
          ,A.salesrank_category4
              ,A.rating
    ,A.ratings_total
    ,A.rating_breakdown_five_star_percentage
    ,A.rating_breakdown_five_star_count
    ,A.rating_breakdown_four_star_percentage
    ,A.rating_breakdown_four_star_count
    ,A.rating_breakdown_three_star_percentage
    ,A.rating_breakdown_three_star_count
    ,A.rating_breakdown_two_star_percentage
    ,A.rating_breakdown_two_star_count
    ,A.rating_breakdown_one_star_percentage
    ,A.rating_breakdown_one_star_count

          ,A.date_time
          ,ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date_time DESC) AS filter_rnk
        FROM tmp.amz_sitewalk_raw_all A)
  SELECT 
     A.asin 
    ,A.bw_availability_type -- in_stock
    ,A.bw_availability_raw
    ,A.bw_ff_amazon_seller_name  
    ,A.country_of_origin  
    ,A.salesrank2
    ,A.salesrank3
    ,A.salesrank4
    ,A.salesrank_category2
    ,A.salesrank_category3
    ,A.salesrank_category4
        ,A.rating
    ,A.ratings_total
    ,A.rating_breakdown_five_star_percentage
    ,A.rating_breakdown_five_star_count
    ,A.rating_breakdown_four_star_percentage
    ,A.rating_breakdown_four_star_count
    ,A.rating_breakdown_three_star_percentage
    ,A.rating_breakdown_three_star_count
    ,A.rating_breakdown_two_star_percentage
    ,A.rating_breakdown_two_star_count
    ,A.rating_breakdown_one_star_percentage
    ,A.rating_breakdown_one_star_count

    ,if(COALESCE(A.salesrank_category2,"NA")<>"NA",1,0)+if(COALESCE(A.salesrank_category3,"NA")<>"NA",1,0)+if(COALESCE(A.salesrank_category4,"NA")<>"NA",1,0) AS browse_node_num
    ,if((A.bw_availability_type<>"in_stock" OR A.bw_price_value IS NULL),"Y","N") AS is_pdp_supp 
    ,A.date_time AS date_time_all
  FROM TMP1 A
  WHERE filter_rnk=1
  ;


  -- 8) OH Inv
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_oh_inv AS
  SELECT 
     A.asin
    ,A.sellable_on_hand_units
    ,A.sellable_on_hand_inventory
    ,A.date
  FROM vc.amz_vc_inv_daily_all A 
  INNER JOIN tmp.amz_sitewalk_sku_list B ON A.asin=B.asin
  WHERE A.date=(SELECT MAX(date) FROM vc.amz_vc_inv_daily_all)
  ;

  -- 9) Net PPM
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_raw_net_ppm AS
  SELECT 
     A.asin
    ,A.net_ppm/100 AS net_ppm
    ,A.date
  FROM vc.amz_vc_netppm_daily_all A 
  INNER JOIN tmp.amz_sitewalk_sku_list B ON A.asin=B.asin
  WHERE A.date=(SELECT MAX(date) FROM vc.amz_vc_netppm_daily_all)
  ;


-- 11) Deal Tag
  CREATE OR REPLACE TABLE tmp.amz_sitewalk_deal AS
  SELECT 
     A.asin
    ,A.created_time_utc 
    ,MAX(A.percent_off) AS percent_off
    ,MAX(A.deal_badge) AS deal_badge 
    ,MAX(A.starts_at) AS starts_at
    ,MAX(A.ends_at) AS ends_at
  FROM dw.amz_us_deals A 
  INNER JOIN tmp.amz_sitewalk_sku_list B ON A.asin=B.asin
  WHERE A.created_time_utc=(SELECT MAX(created_time_utc) FROM dw.amz_us_deals)
  GROUP BY 1,2
  ;



  -- 10) Sales Trend
  CREATE OR REPLACE TABLE vs_pb.amz_sitewalk_sales_trend AS
  SELECT 
     A.asin
    ,A.date
    ,MAX(A.customer_returns) AS customer_returns
    ,MAX(A.ordered_units) AS ordered_units 
    ,MAX(A.ordered_revenue) AS ordered_revenue
  FROM vc.amz_vc_sales_daily_all A 
  INNER JOIN tmp.amz_sitewalk_sku_list B ON A.asin=B.asin
  where date>='2024-01-01'
  GROUP BY 1,2
  ;




  -- Aggregate 
  CREATE OR REPLACE TABLE vs_pb.amz_site_content_monitor AS
  SELECT 
      A.asin
    , A.zinus_sku_cd
    , A.zinus_sku_nm
    , A.collection
    , A.abc_in
    , A.prdct_h_lv1

    ,B.variant_current
    ,B.variant_current_time
    ,B.variant_before
    ,B.variant_before_time
    ,IF(COALESCE(B.var_asin_num_current,1)=0,1,COALESCE(B.var_asin_num_current,1)) AS var_asin_num_current
    ,IF(COALESCE(B.var_asin_num_before,1)=0,1,COALESCE(B.var_asin_num_before,1)) AS var_asin_num_before
    ,B.var_asin_added 
    ,B.var_asin_excluded
    ,if(DATE(B.variant_current_time)=CURRENT_DATE("America/Los_Angeles") AND B.variant_before_time<CURRENT_DATE("America/Los_Angeles") AND B.variant_before IS NOT NULL,"Y","N") AS is_variant_change
    
    ,if(COALESCE(B.var_asin_num_current,0)<=1 AND COALESCE(B.var_asin_num_before,0)>1,"Y","N") AS is_pdp_broken 

    ,C.title_current
    ,C.title_current_time
    ,C.title_before
    ,C.title_before_time
    ,if(DATE(C.title_current_time)=CURRENT_DATE("America/Los_Angeles") AND C.title_before_time<CURRENT_DATE("America/Los_Angeles"),"Y","N") AS is_title_change

    ,D.feature_bullets_current
    ,D.feature_bullets_current_time
    ,D.feature_bullets_before
    ,D.feature_bullets_before_time
    ,if(DATE(D.feature_bullets_current_time)=CURRENT_DATE("America/Los_Angeles") AND D.feature_bullets_before_time<CURRENT_DATE("America/Los_Angeles"),"Y","N") AS is_bullet_change

    ,REPLACE(REPLACE(D.feature_bullets_current,"['",""),"']","") AS feature_bullets_current_unichar

    ,REPLACE(REPLACE(D.feature_bullets_before,"['",""),"']","") AS feature_bullets_before_unichar


    ,F.is_kids
    ,F.is_kids_keywords
    ,F.is_kids_bullet
    ,F.is_kids_attributes
    ,F.keywords
    ,F.feature_bullets
    ,F.attributes

    ,G.bw_price_value_current
    ,G.bw_price_value_current_time
    ,G.bw_price_value_before
    ,G.bw_price_value_before_time
    ,if(DATE(G.bw_price_value_current_time)=CURRENT_DATE("America/Los_Angeles") AND G.bw_price_value_before_time<CURRENT_DATE("America/Los_Angeles"),"Y","N") AS is_price_change

    ,H.bw_availability_type -- in_stock
    ,H.bw_availability_raw
    ,H.bw_ff_amazon_seller_name  
    ,CASE WHEN H.bw_ff_amazon_seller_name IS NULL THEN "NULL (Suppression)" 
          WHEN UPPER(H.bw_ff_amazon_seller_name) LIKE '%AMAZON%' then "AMAZON"
          ELSE "NOT AMAZON"
      END AS is_bb_amz

    ,H.country_of_origin  
    ,H.salesrank2
    ,H.salesrank3
    ,H.salesrank4
    ,H.salesrank_category2
    ,H.salesrank_category3
    ,H.salesrank_category4
    ,H.browse_node_num
    ,COALESCE(H.is_pdp_supp,"N") AS is_pdp_supp 


    ,H.rating
    ,H.ratings_total
    ,H.rating_breakdown_five_star_percentage
    ,H.rating_breakdown_five_star_count
    ,H.rating_breakdown_four_star_percentage
    ,H.rating_breakdown_four_star_count
    ,H.rating_breakdown_three_star_percentage
    ,H.rating_breakdown_three_star_count
    ,H.rating_breakdown_two_star_percentage
    ,H.rating_breakdown_two_star_count
    ,H.rating_breakdown_one_star_percentage
    ,H.rating_breakdown_one_star_count

    ,I.sellable_on_hand_units
    ,I.sellable_on_hand_inventory
    ,I.date AS on_hand_inv_date

    ,J.net_ppm
    ,J.date AS net_ppm_date

    ,date_time_all

    ,CASE WHEN K.COO="CN" THEN "China"
          WHEN K.COO="VN" THEN "Vietnam"
          WHEN K.COO="ID" THEN "Indonesia"
          WHEN K.COO="IT" THEN "Italy"
          WHEN K.COO="TW" THEN "Taiwan"  
          WHEN K.COO="US" THEN "USA"
          ELSE K.COO 
      END AS COO_SAP

    ,K.ABC
    ,COALESCE(W.Index,"Others") AS top_50_200_ind
    ,IF(AV.sku IS NOT NULL,"Y","N") AS is_avenger

    ,IF(A.asin=DL.asin, "Y", "N") AS is_deal 
    ,TRIM(percent_off||"% off "||deal_badge) AS deal_tag

  FROM  tmp.amz_sitewalk_sku_list A
  LEFT OUTER JOIN tmp.amz_sitewalk_raw_variation B ON A.asin=B.asin
  LEFT OUTER JOIN tmp.amz_sitewalk_raw_title C ON A.asin=C.asin 
  LEFT OUTER JOIN tmp.amz_sitewalk_raw_feature_bullets D ON A.asin=D.asin 
  LEFT OUTER JOIN tmp.amz_sitewalk_raw_kids_alert F ON A.asin=F.asin
  LEFT OUTER JOIN tmp.amz_sitewalk_raw_retail_price G ON A.asin=G.asin
  LEFT OUTER JOIN tmp.amz_sitewalk_raw_current_buybox H ON A.asin=H.asin
  LEFT OUTER JOIN tmp.amz_sitewalk_raw_oh_inv I ON A.asin=I.asin
  LEFT OUTER JOIN tmp.amz_sitewalk_raw_net_ppm J ON A.asin=J.asin
  LEFT OUTER JOIN tmp.zinus_erp_sku_info_0902MST K ON A.zinus_sku_cd=K.SKU
  --LEFT OUTER JOIN meta.amz_winner_sku W ON A.asin=W.ASIN
  LEFT OUTER JOIN (SELECT sku FROM meta.avengers13 where sku is not null group by 1) AV ON A.zinus_sku_cd=AV.sku
  LEFT OUTER JOIN tmp.amz_sitewalk_deal DL ON A.asin=DL.asin

  LEFT OUTER JOIN meta.amz_core_asin_list W ON A.asin=W.ASIN
  ;

  
--------------------------------------------------------------------------------------------------------------------------


