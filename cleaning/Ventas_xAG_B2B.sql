
      ------ 2025-10-09 Se resuelve detalle cartera  --------------------------------------------------

      -----Query de venta 
WITH bo_tpc AS (
  SELECT 
  p.transaction_id AS product_id_original,
  /* Métricas */
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd -- Third Party Commission
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) > DATE('2023-12-31') 
  AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
  SELECT 
  gestion_date,
 -- fh.partner_id, 
  IF(tx.channel = 'expedia', 'expedia', tx.partner_data_id) AS partner_id,     ---- se añade Expedia para que matche partner.id = reference.id de cartera Agencias_MB
 -- kam.partner_code as cartera_partner_code,
 /* CASE 
    WHEN kam.partner_code IS NULL OR kam.partner_code = '' 
        THEN CASE 
                 WHEN tx.channel = 'expedia' THEN 'expedia'
                 ELSE tx.partner_data_id
             END
    ELSE kam.partner_code
END as cartera_partner_code, */
 -- kam.kam_name,
  --
case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
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
case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
--  case when fh.buy_type_code in ('Vuelos') then  'Vuelos'  else 'Other' end AS productooriginal,
 ---------- METRICAS ----------------------------------
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb,
    ROUND(SUM(fh.gestion_gb), 2) AS gb_usd_bruto,
      ROUND(SUM(fh.gb_in_local_currency),2) as gb_lc,
         ROUND(SUM(fh.gb_in_local_currency * fh.confirmation_gradient),2) as gb_cx_lc,
--   ,count(distinct bo.product_id_original) as bookings
  ROUND(
    SUM(
      CASE 
      WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') 
      THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                                      IF(pnl.b2b_gradient_margin = '1', 1, 
                                         CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
      WHEN fh.channel = 'expedia' 
      THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                                      IF(pnl.b2b_gradient_margin = '1', 1, 
                                         CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
      ELSE pnl.net_revenues_usd
      END
    )
    ,2) AS nr,
  --ROUND(SUM(pnl.npv_net_usd), 2) AS npv_net_usd,
  ROUND(
    SUM(
      (
        (pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
         + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
         + pnl.affiliates_usd) -- Sumamos afiliadas
        / IF(pnl.b2b_gradient_margin = '1', 1, 
             CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5))) -- Quitar gradiente
      ) 
      - COALESCE(bo.tpc_usd,0) -- Quitar TPC (en sustitución de afiliadas)
    ) 
    * MAX(IF(pnl.b2b_gradient_margin = '1', 1, 
             CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))) 
  ) AS fvm  
  ----- FUENTE Y LEFT JOINS ------
  FROM analytics.bi_sales_fact_sales_recognition fh 
  LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
  ON fh.product_id = pnl.product_id 
  AND pnl.date_reservation_year_month > '2023-12'
  LEFT JOIN analytics.bi_transactional_fact_charges c 
  ON fh.product_id = c.product_id 
  AND c.reservation_year_month >= DATE '2024-01-01'
  LEFT JOIN data.analytics.bi_transactional_fact_products prod 
  ON fh.product_id = prod.product_id 
  AND prod.reservation_year_month >= DATE '2024-01-01'
  left join data.analytics.bi_transactional_fact_transactions tx ON cast(fh.transaction_code as VARCHAR) = tx.transaction_code   -------
  and tx.reservation_year_month >= DATE '2024-01-01'
  LEFT JOIN analytics.bi_pnlop_fact_pricing_model pr 
  ON pr.product_id = fh.product_id 
  AND pr.date_reservation_year_month >= '2024-01'
  LEFT JOIN bo_tpc bo 
  ON bo.product_id_original = fh.origin_product_id
--LEFT JOIN raw.cartera_b2b_b2b2c_test kam
 -- ON kam.partner_code = CASE 
   --                        WHEN fh.channel = 'expedia' THEN 'AG72472'
 --                          ELSE fh.partner_id
   --                      END
 --AND kam.partition IS NOT null
 --- FILTROS GENERALES ----
  WHERE 1=1
  and fh.partition_period > '2023-12'
       and fh.gestion_date >= DATE('2024-01-01')
       and fh.gestion_date < CURRENT_DATE
       --AND month(fh.gestion_date) <= month(current_date)
        and fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
 --     and fh.channel = 'expedia' 
     -- and kam.partner_code is null
    --  and partner_id = ('AP12397')
  GROUP BY 1,2,3,4,5,6

   