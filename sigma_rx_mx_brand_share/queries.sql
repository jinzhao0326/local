--Part I: Brand Overview

--Business Name
SELECT
  store_id,
  name AS store_name
FROM
  public.dimension_store
WHERE
  TRUE
  AND business_id = {{ Business - Id }}
;

--Buinsess Overview
SELECT
  COUNT(DISTINCT store_id) AS store_count,
  COUNT(delivery_id) AS orders,
  SUM(subtotal / 100) AS sales
FROM
  public.dimension_deliveries
WHERE
  TRUE
  AND business_id = {{ Business - Id }}
  AND TO_DATE (created_at) >= {{ start_date }}
  AND TO_DATE (created_at) <= {{ end_date }}
;

-- Part II: Brand Visibility

--Impression Share
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  consumer_within_radius AS (
    SELECT DISTINCT
      consumer_id
    FROM
      public.fact_store_availability
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  ),
  total_views_from_cx_in_radius AS (
    SELECT
      COUNT(id) AS total_views_from_cx_in_radius
    FROM
      public.fact_cx_card_view
    WHERE
      TRUE
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND event_date >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND event_date < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  views_on_business_from_cx_in_radius AS (
    SELECT
      COUNT(id) AS views_on_business_from_cx_in_radius
    FROM
      public.fact_cx_card_view
    WHERE
      TRUE
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND event_date >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND event_date < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  )
SELECT
  'Impressions on the Business from Eligible Cx' AS metric,
  views_on_business_from_cx_in_radius AS impressions
FROM
  views_on_business_from_cx_in_radius
UNION ALL
SELECT
  'Total Impressions from Eligible Cx ' AS metric,
  total_views_from_cx_in_radius AS impressions
FROM
  total_views_from_cx_in_radius
UNION ALL
SELECT
  'Impressions on the Busines as a Share of Total Impressions from Eligible Cx' AS metric,
  views_on_business_from_cx_in_radius / (
    SELECT
      total_views_from_cx_in_radius
    FROM
      total_views_from_cx_in_radius
  ) AS impression_share
FROM
  views_on_business_from_cx_in_radius
;

--Cx Reach
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  consumer_within_radius AS (
    SELECT DISTINCT
      consumer_id
    FROM
      public.fact_store_availability
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  ),
  business_consumer_reach AS (
    SELECT
      COUNT(DISTINCT consumer_id) AS cx_within_radius_reached
    FROM
      public.fact_cx_card_view
    WHERE
      TRUE
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND event_date >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND event_date < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  )
SELECT
  'Total Cx within Radius' AS metric,
  COUNT(consumer_id) AS Unique_Cx_Ct
FROM
  consumer_within_radius
UNION ALL
SELECT
  'Cx within Radius Reached' AS metric,
  cx_within_radius_reached AS Unique_Cx_Ct
FROM
  business_consumer_reach
UNION ALL
SELECT
  'Cx Reached as a Share of Total Cx within Radius' AS metric,
  cx_within_radius_reached / (
    SELECT
      COUNT(consumer_id)
    FROM
      consumer_within_radius
  )
FROM
  business_consumer_reach
;

--Brand Keyword Rank
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  consumer_within_radius AS (
    SELECT DISTINCT
      consumer_id
    FROM
      public.fact_store_availability
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  ),
  brand AS (
    SELECT
      COALESCE(TRIM(search_term_clean), LOWER(product_name)) AS product_name,
      CASE
        WHEN search_term IS NULL THEN 'carousel'
        ELSE 'search'
      END AS product_surface,
      COUNT(id) AS impressions,
      COUNT(DISTINCT consumer_id) Unique_Cx_Ct,
      AVG(card_position) AS average_card_position
    FROM
      public.fact_cx_card_view
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND event_date >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND event_date < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND product_name IS NOT NULL
    GROUP BY
      1,
      2
  ),
  total_share AS (
    SELECT
      COALESCE(TRIM(search_term_clean), LOWER(product_name)) AS product_name,
      CASE
        WHEN search_term IS NULL THEN 'carousel'
        ELSE 'search'
      END AS product_surface,
      COUNT(id) AS impressions,
      COUNT(DISTINCT consumer_id) Unique_Cx_Ct
    FROM
      public.fact_cx_card_view
    WHERE
      TRUE
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND event_date >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND event_date < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND product_name IS NOT NULL
    GROUP BY
      1,
      2
  )
SELECT
  brand.product_name AS Product_Name,
  brand.product_surface AS Product_Surface,
  brand.average_card_position AS Brand_Average_Card_Position,
  brand.impressions AS Brand_Impressions_from_Eligible_Cx,
  total.impressions AS Total_DoorDash_Impressions_from_Eligible_Cx,
  DIV0 (brand.impressions, total.impressions) AS Impression_Share_from_Eligible_Cx,
  brand.Unique_Cx_Ct AS Brand_Impression_Unique_Cx_Ct,
  total.Unique_Cx_Ct AS Total_DoorDash_Impression_Unique_Cx_Ct,
  DIV0 (brand.Unique_Cx_Ct, total.Unique_Cx_Ct) AS Unique_Cx_Share
FROM
  brand brand
  LEFT JOIN total_share total ON brand.product_name = total.product_name
  AND brand.product_surface = total.product_surface
ORDER BY
  4 DESC
;

--Part III: Brand Engagement

--How many Cx got impressions from my stores?
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  )
SELECT
  COUNT(DISTINCT consumer_id) AS unique_cx_count
FROM
  public.fact_cx_card_view
WHERE
  TRUE
  AND store_id IN (
    SELECT
      store_id
    FROM
      store
  )
  AND event_date >= TO_TIMESTAMP_NTZ ({{ start_date }})
  AND event_date < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
;

--How many Cx visited my stores?
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  brand_store_visit AS (
    SELECT
      user_id
    FROM
      segment_events_raw.consumer_production.m_store_page_load
    WHERE
      TRUE
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
    UNION ALL
    SELECT
      user_id
    FROM
      segment_events_raw.consumer_production.store_page_load
    WHERE
      TRUE
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  )
SELECT
  COUNT(DISTINCT user_id) as Unique_Cx_Ct
FROM
  brand_store_visit
;

--How many Cx added items from my stores?
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  brand_store_add_item AS (
    SELECT
      user_id
    FROM
      segment_events_raw.consumer_production.action_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
    UNION ALL
    SELECT
      user_id
    FROM
      segment_events_raw.consumer_production.m_item_page_action_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
    UNION ALL
    SELECT
      user_id
    FROM
      segment_events_raw.consumer_production.action_quick_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  )
SELECT
  COUNT(DISTINCT user_id) AS Unique_Cx_Count
FROM
  brand_store_add_item
;

--How many Cx checked out from my stores? (How many Cx placed orders with my stores?)
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  )
SELECT
  COUNT(DISTINCT creator_id) AS Unique_Cx_Count
FROM
  public.dimension_deliveries
WHERE
  TRUE
  AND store_id IN (
    SELECT
      store_id
    FROM
      store
  )
  AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
  AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  AND cancelled_at IS NULL
;

--How many Cx added these items to cart from my stores?
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  brand_store_add_item AS (
    SELECT
      item_name,
      user_id
    FROM
      segment_events_raw.consumer_production.action_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
    UNION ALL
    SELECT
      item_name,
      user_id
    FROM
      segment_events_raw.consumer_production.m_item_page_action_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
    UNION ALL
    SELECT
      item_name,
      user_id
    FROM
      segment_events_raw.consumer_production.action_quick_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  )
SELECT
  item_name,
  COUNT(DISTINCT user_id) AS Unique_Cx_Count
FROM
  brand_store_add_item
WHERE
  item_name IS NOT NULL
GROUP BY
  1
ORDER BY
  2 DESC
;

--How many Cx checked out these items from my stores?
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  final_table AS (
    SELECT
      doi.item_name,
      cs.user_id
    FROM
      segment_events_raw.consumer_production.m_checkout_page_system_checkout_success cs
      INNER JOIN proddb.public.dimension_deliveries dd ON cs.order_uuid = dd.order_cart_uuid
      INNER JOIN proddb.public.dimension_order_item doi ON dd.delivery_id = doi.delivery_id
    WHERE
      TRUE
      AND dd.store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND cs.timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND cs.timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND dd.is_filtered_core = TRUE
    UNION ALL
    SELECT
      doi.item_name,
      cs.user_id
    FROM
      segment_events_raw.consumer_production.system_checkout_success cs
      INNER JOIN proddb.public.dimension_deliveries dd ON cs.order_uuid = dd.order_cart_uuid
      INNER JOIN proddb.public.dimension_order_item doi ON dd.delivery_id = doi.delivery_id
    WHERE
      TRUE
      AND dd.store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND cs.timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND cs.timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND dd.is_filtered_core = TRUE
  )
SELECT
  item_name,
  COUNT(DISTINCT user_id) AS Unique_Cx_Count
FROM
  final_table
GROUP BY
  1
ORDER BY
  2 DESC
;

--Total Unique # of Items Added to Cart by Cx from My Stores
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  brand_store_add_item AS (
    SELECT
      item_name
    FROM
      segment_events_raw.consumer_production.action_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
    UNION ALL
    SELECT
      item_name
    FROM
      segment_events_raw.consumer_production.m_item_page_action_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
    UNION ALL
    SELECT
      item_name
    FROM
      segment_events_raw.consumer_production.action_quick_add_item
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  )
SELECT
  COUNT(DISTINCT item_name) AS Unique_Item_Count
FROM
  brand_store_add_item
;

--Total Unique # of Items Checked out by Cx from My Stores
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  final_table AS (
    SELECT
      doi.item_name
    FROM
      segment_events_raw.consumer_production.m_checkout_page_system_checkout_success cs
      INNER JOIN proddb.public.dimension_deliveries dd ON cs.order_uuid = dd.order_cart_uuid
      INNER JOIN proddb.public.dimension_order_item doi ON dd.delivery_id = doi.delivery_id
    WHERE
      TRUE
      AND dd.store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND cs.timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND cs.timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND dd.is_filtered_core = TRUE
    UNION ALL
    SELECT
      doi.item_name
    FROM
      segment_events_raw.consumer_production.system_checkout_success cs
      INNER JOIN proddb.public.dimension_deliveries dd ON cs.order_uuid = dd.order_cart_uuid
      INNER JOIN proddb.public.dimension_order_item doi ON dd.delivery_id = doi.delivery_id
    WHERE
      TRUE
      AND dd.store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND cs.timestamp >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND cs.timestamp < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
      AND dd.is_filtered_core = TRUE
  )
SELECT
  COUNT(DISTINCT item_name) AS Unique_Item_Count
FROM
  final_table
;

--Part V: Ad Investment

--Total Brand Ads Fee Share
--Ad Spend by Brand/Total Ad Spend from Competitors on DD
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  competing_stores AS (
    SELECT
      store_id
    FROM
      dimension_store
    WHERE
      TRUE
      AND CASE
        WHEN LEN (
          ARRAY_TO_STRING (ARRAY_CONSTRUCT {{ competitor_business_id }}, ',')
        ) = 0 THEN True
        ELSE business_id IN {{ competitor_business_id }}
      END
  ),
  brand_ads_fee AS (
    SELECT
      SUM(ads_fee / 100) AS total_ads_fee
    FROM
      public.fact_ads_sl_attributions
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  competing_ads_fee AS (
    SELECT
      SUM(ads_fee / 100) AS total_ads_fee
    FROM
      public.fact_ads_sl_attributions
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          competing_stores
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  consumer_within_radius AS (
    SELECT DISTINCT
      consumer_id
    FROM
      public.fact_store_availability
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  ),
  competitor_ads_fee AS (
    SELECT
      SUM(ads_fee / 100) AS total_ads_fee
    FROM
      public.fact_ads_sl_attributions
    WHERE
      TRUE
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  )
SELECT
  'Mx within Cx Radius Total Ads Fee' AS metric,
  total_ads_fee AS Total_Ads_Fee
FROM
  competitor_ads_fee
UNION ALL
SELECT
  'Business Total Ads Fee' AS metric,
  total_ads_fee AS Total_Ads_Fee
FROM
  brand_ads_fee
UNION ALL
SELECT
  'Competitor Businesses Total Ads Fee' AS metric,
  total_ads_fee AS Total_Ads_Fee
FROM
  competing_ads_fee
;

--Total Brand Promo Fee Share
--Ad Spend by Brand/Total Ad Spend from Competitors on DD
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  competing_stores AS (
    SELECT
      store_id
    FROM
      dimension_store
    WHERE
      TRUE
      AND CASE
        WHEN LEN (
          ARRAY_TO_STRING (ARRAY_CONSTRUCT {{ competitor_business_id }}, ',')
        ) = 0 THEN True
        ELSE business_id IN {{ competitor_business_id }}
      END
  ),
  brand_promo_fee AS (
    SELECT
      SUM(promotion_fee / 100) AS total_promo_fee
    FROM
      public.fact_promotion_deliveries
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  competing_promo_fee AS (
    SELECT
      SUM(promotion_fee / 100) AS total_promo_fee
    FROM
      public.fact_promotion_deliveries
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          competing_stores
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  consumer_within_radius AS (
    SELECT DISTINCT
      consumer_id
    FROM
      public.fact_store_availability
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  ),
  competitor_promo_fee AS (
    SELECT
      SUM(promotion_fee / 100) AS total_promo_fee
    FROM
      public.fact_promotion_deliveries
    WHERE
      TRUE
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  )
SELECT
  'Mx within Cx Radius Total Promo Fee' AS Metric,
  total_promo_fee AS Total_Promo_Fee
FROM
  competitor_promo_fee
UNION ALL
SELECT
  'Business Total Promo Fee' AS Metric,
  total_promo_fee AS Total_Promo_Fee
FROM
  brand_promo_fee
UNION ALL
SELECT
  'Competitor Businesses Total Promo Fee' AS Metric,
  total_promo_fee AS Total_Promo_Fee
FROM
  competing_promo_fee
;

--Ads Fee Per Store
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  competing_stores AS (
    SELECT
      store_id
    FROM
      dimension_store
    WHERE
      TRUE
      AND CASE
        WHEN LEN (
          ARRAY_TO_STRING (ARRAY_CONSTRUCT {{ competitor_business_id }}, ',')
        ) = 0 THEN True
        ELSE business_id IN {{ competitor_business_id }}
      END
  ),
  brand_ads_fee AS (
    SELECT
      SUM(ads_fee / 100) / (
        SELECT
          COUNT(store_id)
        FROM
          store
      ) AS ads_fee_per_store
    FROM
      public.fact_ads_sl_attributions
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  competing_ads_fee AS (
    SELECT
      SUM(ads_fee / 100) / (
        SELECT
          COUNT(store_id)
        FROM
          competing_stores
      ) AS ads_fee_per_store
    FROM
      public.fact_ads_sl_attributions
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          competing_stores
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  consumer_within_radius AS (
    SELECT DISTINCT
      consumer_id
    FROM
      public.fact_store_availability
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  ),
  competitor_ads_fee AS (
    SELECT
      SUM(ads_fee / 100) / COUNT(DISTINCT store_id) AS ads_fee_per_store
    FROM
      public.fact_ads_sl_attributions
    WHERE
      TRUE
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  )
SELECT
  'Mx within Cx Radius Ads Fee per Store' AS Metric,
  ads_fee_per_store AS Ads_Fee_per_Store
FROM
  competitor_ads_fee
UNION ALL
SELECT
  'Business Ads Fee per Store' AS Metric,
  ads_fee_per_store AS Ads_Fee_per_Store
FROM
  brand_ads_fee
UNION ALL
SELECT
  'Competitor Businesses Ads Fee per Store' AS Metric,
  ads_fee_per_store AS Ads_Fee_per_Store
FROM
  competing_ads_fee
;

--Promotion Fee Per Store
WITH
  store AS (
    SELECT
      store_id
    FROM
      public.dimension_store
    WHERE
      TRUE
      AND business_id = {{ Business - Id }}
  ),
  competing_stores AS (
    SELECT
      store_id
    FROM
      dimension_store
    WHERE
      TRUE
      AND CASE
        WHEN LEN (
          ARRAY_TO_STRING (ARRAY_CONSTRUCT {{ competitor_business_id }}, ',')
        ) = 0 THEN True
        ELSE business_id IN {{ competitor_business_id }}
      END
  ),
  brand_promo_fee AS (
    SELECT
      SUM(promotion_fee / 100) / (
        SELECT
          COUNT(store_id)
        FROM
          store
      ) AS promotion_fee_per_store
    FROM
      public.fact_promotion_deliveries
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  competing_promo_fee AS (
    SELECT
      SUM(promotion_fee / 100) / (
        SELECT
          COUNT(store_id)
        FROM
          competing_stores
      ) AS promotion_fee_per_store
    FROM
      public.fact_promotion_deliveries
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          competing_stores
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  ),
  consumer_within_radius AS (
    SELECT DISTINCT
      consumer_id
    FROM
      public.fact_store_availability
    WHERE
      TRUE
      AND store_id IN (
        SELECT
          store_id
        FROM
          store
      )
  ),
  competitor_promo_fee AS (
    SELECT
      SUM(promotion_fee / 100) / COUNT(DISTINCT store_id) AS promotion_fee_per_store
    FROM
      public.fact_promotion_deliveries
    WHERE
      TRUE
      AND consumer_id IN (
        SELECT
          consumer_id
        FROM
          consumer_within_radius
      )
      AND created_at >= TO_TIMESTAMP_NTZ ({{ start_date }})
      AND created_at < TIMESTAMPADD (DAY, 1, TO_TIMESTAMP_NTZ ({{ end_date }}))
  )
SELECT
  'Mx within Cx Radius Promo Fee per Store' AS Metric,
  promotion_fee_per_store AS Promo_Fee_per_Store
FROM
  competitor_promo_fee
UNION ALL
SELECT
  'Business Promo Fee per Store' AS Metric,
  promotion_fee_per_store AS Promo_Fee_per_Store
FROM
  brand_promo_fee
UNION ALL
SELECT
  'Competitor Businesses Promo Fee per Store' AS Metric,
  promotion_fee_per_store AS Promo_Fee_per_Store
FROM
  competing_promo_fee
;