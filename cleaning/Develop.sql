select *
from data.lake.api_b2b_hsm_events_summarized_by_day
where eventdate >= date'2025-08-01'
--and partnerid in ('AP12908', 'AP12909', 'AP12576')
and guestnationality is not null
limit 100


----
select * 
from raw.trk_pageview_conversion
where event_date is not null
--and partner_id in ('AP12908', 'AP12909', 'AP12576')
limit 100

parsed_events pe
 
unique_transactions

hourly_counts hc

selected_dates sd

average_past_4_weeks ap4w


---

WITH bo_tpc AS (
  SELECT 
    p.transaction_id AS product_id_original,
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE('2024-01-01') 
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
SELECT 
  gestion_date,
  CASE WHEN fh.parent_channel = 'API' THEN pnl.brand ELSE 'Best Day' END AS brand,
  CASE WHEN pnl.line_of_business = 'B2B2C' THEN 'B2B2C' ELSE fh.parent_channel END AS parent_channel,
CASE
  WHEN fh.partner_id IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  WHEN fh.partner_id IN (
    'AP12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  WHEN fh.country_code IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN fh.country_code
  WHEN fh.country_code IN ('US','PA','ES','CR') THEN 'GL'
  WHEN fh.country_code IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END
AS country_code,
  CASE WHEN fh.buy_type_code IN ('Hoteles','Vuelos','Carrito','Traslados') THEN fh.buy_type_code ELSE 'DS' END AS productooriginal,
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb,
  ROUND(
    SUM(
      CASE 
        WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') THEN
          (pnl.net_revenues_usd - (bo.tpc_usd * IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
        WHEN fh.channel = 'expedia' THEN
          (pnl.net_revenues_usd - (bo.tpc_usd * IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
        ELSE pnl.net_revenues_usd
      END
    ), 2
  ) AS nr,
  ROUND(
    SUM(
      (
        (pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
         + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
         + pnl.affiliates_usd)
        / IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))
      ) 
      - COALESCE(bo.tpc_usd,0)
    ) 
    * MAX(IF(pnl.b2b_gradient_margin = '1', 1, CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5))))
  ) AS fvm
FROM analytics.bi_sales_fact_sales_recognition fh 
LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
  ON fh.product_id = pnl.product_id 
  AND pnl.date_reservation_year_month > '2023-01'
LEFT JOIN analytics.bi_transactional_fact_charges c 
  ON fh.product_id = c.product_id 
  AND c.reservation_year_month >= DATE '2024-01-01'
LEFT JOIN data.analytics.bi_transactional_fact_products prod 
  ON fh.product_id = prod.product_id 
  AND prod.reservation_year_month >= DATE '2024-01-01'
LEFT JOIN analytics.bi_pnlop_fact_pricing_model pr 
  ON pr.product_id = fh.product_id 
  AND pr.date_reservation_year_month >= '2024-01'
LEFT JOIN bo_tpc bo 
  ON bo.product_id_original = fh.origin_product_id
WHERE 
  fh.gestion_date >= DATE('2024-01-01')
  and fh.gestion_date < CURRENT_DATE
  AND partition_period > '2023-01'
  --AND fh.country_code = 'MX'
  AND month(fh.gestion_date) <= month(current_date)
  AND fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
  AND pnl.line_of_business = 'B2B'
  AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
GROUP BY 1,2,3,4,5
order by gestion_date desc



--Ventas
CASE
  WHEN fh.partner_id IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  WHEN fh.partner_id IN (
    'AP12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  WHEN fh.country_code IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN fh.country_code
  WHEN fh.country_code IN ('US','PA','ES','CR') THEN 'GL'
  WHEN fh.country_code IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END
AS country_code,
