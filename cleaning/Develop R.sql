---------------------------------------------------------------------------------
----------- 2025-08-11 tendencia mensual por KAM  --------------------------------------------------

WITH bo_tpc AS (
  SELECT 
  p.transaction_id AS product_id_original,
  /* Métricas */
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd -- Third Party Commission
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE('2024-01-01') 
  AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
  SELECT 
  gestion_date,
 -- fh.partner_id, 
 -- IF(tx.channel = 'expedia', 'Expedia', tx.partner_data_id) AS partner_id,     ---- se añade Expedia para que matche partner.id = reference.id de cartera Agencias_MB
  --kam.partner_code,
  kam.kam_name,
  --
/*  case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
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
AS country_code,*/
  case when fh.buy_type_code in ('Vuelos') then  'Vuelos'  else 'Other' end AS productooriginal,
 ---------- METRICAS ----------------------------------
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb 
--   ,count(distinct bo.product_id_original) as bookings
  ,ROUND(
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
LEFT JOIN raw.cartera_b2b_b2b2c_test kam
  ON kam.partner_code = CASE 
                           WHEN fh.channel = 'expedia' THEN 'AG72472'
                           ELSE fh.partner_id
                         END
 AND kam.partition IS NOT null
 --- FILTROS GENERALES ----
  WHERE 1=1
  and fh.partition_period > '2023-12'
       and fh.gestion_date >= DATE('2024-01-01')
       AND month(fh.gestion_date) <= month(current_date)
        and fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
   --   and fh.channel = 'expedia' 
  GROUP BY 1,2,3
  limit 100
  

  
  SELECT
    partner_code      AS agency_code,
    kam_region        AS market,
    kam_director      AS lead,
    kam_manager       AS manager,
    kam_name          AS kam
FROM raw.cartera_b2b_b2b2c_test
WHERE partition IS NOT null
--and partner_code = 'expedia'

select *
FROM raw.cartera_b2b_b2b2c_test kam
WHERE partition IS NOT null
--and kam.responsable_mail = 'arina.garutti@hoteldo.com'
 
select *
FROM analytics.bi_sales_fact_sales_recognition fh 
where partition_period = '2025-07'
and line_of_business_code = 'B2B'
and channel = 'expedia'
limit 100

agent_code,
agency_code,
partner_id   --- agent_code / agency_code / partner id = AG72472, agency_name = 'Expedia US', channel = 'expedia'
 
---------------------------------------------------------------------------------
----------- 2025-08-11 tendencia mensual por Agencia  --------------------------------------------------

WITH bo_tpc AS (
  SELECT 
  p.transaction_id AS product_id_original,
  /* Métricas */
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd -- Third Party Commission
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE('2024-01-01') 
  AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
  SELECT 
  gestion_date,
 -- fh.partner_id, 
  IF(tx.channel = 'expedia', 'expedia', tx.partner_data_id) AS partner_id,     ---- se añade Expedia para que matche partner.id = reference.id de cartera Agencias_MB
  --
  case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
CASE
  -- 1) Partner overrides a 'PY'
  WHEN fh.partner_id IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  -- 2) Todos los partner_id que originalmente iban a *_CTA → 'CTA'
  WHEN fh.partner_id IN (
    'P12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  -- 3) Si no hay override de partner, clasificar por country_code original:
  -- 3a) Mantener códigos tal cual para estos países
  WHEN fh.country_code IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN fh.country_code
  -- 3b) Estados Unidos, Panamá, España, Costa Rica → 'GL'
  WHEN fh.country_code IN ('US','PA','ES','CR') THEN 'GL'
  -- 3c) Uruguay o Bolivia → 'UY'
  WHEN fh.country_code IN ('UY','BO') THEN 'UY'
  -- 4) Resto de casos → 'GL'
  ELSE 'GL'
END
AS country_code,
  case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb 
--   ,count(distinct bo.product_id_original) as bookings
  ,ROUND(
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
  WHERE 
  fh.gestion_date >= DATE('2024-01-01')
  AND partition_period > '2023-12'
  AND month(fh.gestion_date) <= month(current_date)
  AND 
   fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
   --   and fh.channel = 'expedia' 
  GROUP BY 1,2,3,4,5,6


   select *
    from data.analytics.bi_transactional_fact_transactions tx
    where reservation_year_month is not null
        limit 100
        
       transaction_code
       customer_id
       user_agent
       agent_code
       
       select *
       from analytics.bi_sales_fact_sales_recognition fh
       where partition_period is not null
       limit 100
       
       transaction_code
    
---------------------------------------------------------------------------------
----------- 2025-08-05 actual tendencia mensual metas  --------------------------------------------------

WITH bo_tpc AS (
  SELECT 
  p.transaction_id AS product_id_original,
  /* Métricas */
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd -- Third Party Commission
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE('2024-01-01') 
  AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
  SELECT 
  gestion_date,
  case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
CASE
  -- 1) Partner overrides a 'PY'
  WHEN fh.partner_id IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  -- 2) Todos los partner_id que originalmente iban a *_CTA → 'CTA'
  WHEN fh.partner_id IN (
    'P12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  -- 3) Si no hay override de partner, clasificar por country_code original:
  -- 3a) Mantener códigos tal cual para estos países
  WHEN fh.country_code IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN fh.country_code
  -- 3b) Estados Unidos, Panamá, España, Costa Rica → 'GL'
  WHEN fh.country_code IN ('US','PA','ES','CR') THEN 'GL'
  -- 3c) Uruguay o Bolivia → 'UY'
  WHEN fh.country_code IN ('UY','BO') THEN 'UY'
  -- 4) Resto de casos → 'GL'
  ELSE 'GL'
END
AS country_code,
  case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb 
--   ,count(distinct bo.product_id_original) as bookings
  ,ROUND(
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
  AND partition_period > '2023-01'
  AND month(fh.gestion_date) <= month(current_date)
  AND 
   fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
  GROUP BY 1,2,3,4,5
  
  
  ----------------------------------------------------------------------------------------------------------------------------------------
  ----------------------------------------------------------------------------------------------------------------------------------------
  --------- QBR WebBeds 2025-08 ----------------
  
  
  
  WITH bo_tpc AS (
  SELECT 
    p.transaction_id AS product_id_original,
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd,
    MAX(CAST(p.cancelled AS DATE)) AS cancelled,
    MAX(
      CASE 
        WHEN COALESCE(p.status, '') = '' THEN 'ACTIVE'
        ELSE p.status
      END
    ) AS bo_status
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s
    ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE '2024-01-01'
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY p.transaction_id
)
SELECT
 -- bo.cancelled,
 -- bo.bo_status,
  gestion_date,
  case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
  case 
  when fh.country_code in ('BR', 'CL', 'CO' ,'MX' , 'PE' ,'AR' ,'EC') then  fh.country_code
  when fh.country_code in ('US' ,'UY','PA') and fh.parent_channel IN ('API','Agencias afiliadas') then 'GL'
  else 'OT'
  end as country_code,
  case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
--  fh.product_status as product_status,
 -- fh.product_is_confirmed_flg as product_is_confirmed_flg,
    ROUND(SUM(fh.gestion_gb), 2) AS gb 
  --ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb_cgx
--   ,count(distinct bo.product_id_original) as bookings
--   ,sum(pnl.fee_net_usd) as fee
--   ,sum(pnl.commission_net_usd) as comision_conAA
--   ,-sum(pnl.discounts_net_usd) as descuentos
--   ,sum((pnl.fee_net_usd+pnl.commission_net_usd-pnl.discounts_net_usd)) as rmn
--   ,+sum(pnl.media_revenue_usd ) as Otros_Media_Rev
--   ,+sum(pnl.discounts_mkt_funds_usd) as mkt
--   ,sum(pnl.backend_non_air_usd)+sum(pnl.backend_air_usd) as backend_incentives
--   ,sum(pnl.other_incentives_air_usd) as otros_incentivos_air
--   ,sum(pnl.other_incentives_non_air_usd) as otros_incentivos_non_air
--   ,sum(pnl.revenue_taxes_usd) as revenue_tax
--   ,-sum(pnl.cancellations_usd) as cancelaciones
--   ,sum(pnl.breakage_revenue_usd) as breakage_rev
--   ,-sum(pnl.ccp_usd) as ccp 
--   ,-sum(pnl.coi_usd) as coi
--   ,sum(pnl.coi_interest_usd) as interes_coi
--   ,sum(pnl.customer_service_usd) as Customer_Service
--   ,sum(pnl.customer_claims_usd) as customer_claims
--   ,sum(pnl.errors_usd) as Errors
--   ,sum(pnl.frauds_usd) as Frauds
--   ,sum(pnl.loyalty_usd) as loyalty
--   ,sum(pnl.ott_usd) as ott
--   ,-sum(pnl.vendor_commission_usd) as Comisiones_Vendedores
--   ,sum(pnl.financial_result_usd) as Resultado_Financiero
--   ,sum(pr.dif_fx_usd+pr.dif_fx_air_usd) as dif_fx
--   ,sum (pr.currency_hedge_usd + pr.currency_hedge_air_usd) as currency_hedge
  ,ROUND(
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
  FROM analytics.bi_sales_fact_sales_recognition fh 
  LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
  ON fh.product_id = pnl.product_id 
  AND pnl.date_reservation_year_month >= '2024-01'
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
  AND partition_period >= '2024-01'
--  AND month(fh.gestion_date) <= month(current_date)
  AND 
   fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
     and fh.partner_id in ('AP12576', 'AP12577')   --- Webbeds
   --   and fh.product_status = 'xx'
    --  and fh.product_is_confirmed_flg = 1
      and bo.bo_status = 'EMITTED'
      GROUP BY 1,2,3,4,5--,6,7
      
  
  
  -----
  
  
  ---Resumido
  
  
WITH bo_tpc AS (
  SELECT 
    p.transaction_id AS product_id_original,
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd,
    MAX(CAST(p.cancelled AS DATE)) AS cancelled,
    MAX(
      CASE 
        WHEN COALESCE(p.status, '') = '' THEN 'ACTIVE'
        ELSE p.status
      END
    ) AS bo_status
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s
    ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE '2024-01-01'
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY p.transaction_id
)
SELECT
 -- bo.cancelled,
 -- bo.bo_status,
  gestion_date,
 -- case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
  case 
  when fh.country_code in ('BR', 'CL', 'CO' ,'MX' , 'PE' ,'AR' ,'EC') then  fh.country_code
  when fh.country_code in ('US' ,'UY','PA') and fh.parent_channel IN ('API','Agencias afiliadas') then 'GL'
  else 'OT'
  end as country_code,
  --case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb 
--   ,count(distinct bo.product_id_original) as bookings
--   ,sum(pnl.fee_net_usd) as fee
--   ,sum(pnl.commission_net_usd) as comision_conAA
--   ,-sum(pnl.discounts_net_usd) as descuentos
--   ,sum((pnl.fee_net_usd+pnl.commission_net_usd-pnl.discounts_net_usd)) as rmn
--   ,+sum(pnl.media_revenue_usd ) as Otros_Media_Rev
--   ,+sum(pnl.discounts_mkt_funds_usd) as mkt
--   ,sum(pnl.backend_non_air_usd)+sum(pnl.backend_air_usd) as backend_incentives
--   ,sum(pnl.other_incentives_air_usd) as otros_incentivos_air
--   ,sum(pnl.other_incentives_non_air_usd) as otros_incentivos_non_air
--   ,sum(pnl.revenue_taxes_usd) as revenue_tax
--   ,-sum(pnl.cancellations_usd) as cancelaciones
--   ,sum(pnl.breakage_revenue_usd) as breakage_rev
--   ,-sum(pnl.ccp_usd) as ccp 
--   ,-sum(pnl.coi_usd) as coi
--   ,sum(pnl.coi_interest_usd) as interes_coi
--   ,sum(pnl.customer_service_usd) as Customer_Service
--   ,sum(pnl.customer_claims_usd) as customer_claims
--   ,sum(pnl.errors_usd) as Errors
--   ,sum(pnl.frauds_usd) as Frauds
--   ,sum(pnl.loyalty_usd) as loyalty
--   ,sum(pnl.ott_usd) as ott
--   ,-sum(pnl.vendor_commission_usd) as Comisiones_Vendedores
--   ,sum(pnl.financial_result_usd) as Resultado_Financiero
--   ,sum(pr.dif_fx_usd+pr.dif_fx_air_usd) as dif_fx
--   ,sum (pr.currency_hedge_usd + pr.currency_hedge_air_usd) as currency_hedge
  ,ROUND(
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
  FROM analytics.bi_sales_fact_sales_recognition fh 
  LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
  ON fh.product_id = pnl.product_id 
  AND pnl.date_reservation_year_month >= '2024-01'
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
  AND partition_period >= '2024-01'
 -- AND month(fh.gestion_date) <= month(current_date)
  AND 
   fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
      --and fh.partner_code
      and bo.bo_status = 'EMITTED'
  GROUP BY 1,2,3--,4,5
  
  
  
  
  
  sales_bo as (
	select s.transaction_id as tx_code
	       ,p.transaction_id as product_id_original
	       ,max(cast(p.cancelled as date)) as cancelled
	       ,max(if(p.status='','ACTIVE',p.status)) as bo_status
	from data.lake.channels_bo_product p
	join data.lake.channels_bo_sale s on s.id = p.sale_id
	where s.created >= date('2023-01-01')
	and s.channel in (select cl.channel from raw.b2b_dim_channel_by_lob cl where cl.lob = 'B2B')
	group by 1,2
  
  --------------------------------------------------------------------------------------------------------------------------------------
  -------------------------------------------------------------------------------------------------------------------------------------
  
  
  ---Todos los registros
  
  
  WITH bo_tpc AS (
  SELECT 
  p.transaction_id AS product_id_original,
  /* Métricas */
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd -- Third Party Commission
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE('2024-01-01') 
  AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
  SELECT 
  gestion_date,
 -- case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
  case 
  when fh.country_code in ('BR', 'CL', 'CO' ,'MX' , 'PE' ,'AR' ,'EC') then  fh.country_code
  when fh.country_code in ('US' ,'UY','PA') and fh.parent_channel IN ('API','Agencias afiliadas') then 'GL'
  else 'OT'
  end as country_code,
  --case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb 
--   ,count(distinct bo.product_id_original) as bookings
--   ,sum(pnl.fee_net_usd) as fee
--   ,sum(pnl.commission_net_usd) as comision_conAA
--   ,-sum(pnl.discounts_net_usd) as descuentos
--   ,sum((pnl.fee_net_usd+pnl.commission_net_usd-pnl.discounts_net_usd)) as rmn
--   ,+sum(pnl.media_revenue_usd ) as Otros_Media_Rev
--   ,+sum(pnl.discounts_mkt_funds_usd) as mkt
--   ,sum(pnl.backend_non_air_usd)+sum(pnl.backend_air_usd) as backend_incentives
--   ,sum(pnl.other_incentives_air_usd) as otros_incentivos_air
--   ,sum(pnl.other_incentives_non_air_usd) as otros_incentivos_non_air
--   ,sum(pnl.revenue_taxes_usd) as revenue_tax
--   ,-sum(pnl.cancellations_usd) as cancelaciones
--   ,sum(pnl.breakage_revenue_usd) as breakage_rev
--   ,-sum(pnl.ccp_usd) as ccp 
--   ,-sum(pnl.coi_usd) as coi
--   ,sum(pnl.coi_interest_usd) as interes_coi
--   ,sum(pnl.customer_service_usd) as Customer_Service
--   ,sum(pnl.customer_claims_usd) as customer_claims
--   ,sum(pnl.errors_usd) as Errors
--   ,sum(pnl.frauds_usd) as Frauds
--   ,sum(pnl.loyalty_usd) as loyalty
--   ,sum(pnl.ott_usd) as ott
--   ,-sum(pnl.vendor_commission_usd) as Comisiones_Vendedores
--   ,sum(pnl.financial_result_usd) as Resultado_Financiero
--   ,sum(pr.dif_fx_usd+pr.dif_fx_air_usd) as dif_fx
--   ,sum (pr.currency_hedge_usd + pr.currency_hedge_air_usd) as currency_hedge
  ,ROUND(
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
  FROM analytics.bi_sales_fact_sales_recognition fh 
  LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
  ON fh.product_id = pnl.product_id 
  AND pnl.date_reservation_year_month >= '2024-01'
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
  left join sales_bo bo on bo.product_id_original = fv_prev.origin_product_id
  WHERE 
  1=1
  and fh.gestion_date >= DATE('2024-01-01')
  AND partition_period >= '2024-01'
  AND month(fh.gestion_date) <= month(current_date)
  AND 
   fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
      and fh.country_code = 'BR'
  GROUP BY 1,2,3--,4--,5
  
  ----------------------
  
  
  
  --develop
WITH bo_tpc AS (
  SELECT 
    p.transaction_id AS product_id_original,
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd,
    MAX(CAST(p.cancelled AS DATE)) AS cancelled,
    MAX(
      CASE 
        WHEN COALESCE(p.status, '') = '' THEN 'ACTIVE'
        ELSE p.status
      END
    ) AS bo_status
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s
    ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE '2024-01-01'
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY p.transaction_id
)
SELECT
  date_format(fh.gestion_date, '%Y-%m') AS anio_mes,
  ROUND(SUM(fh.gestion_gb), 2) AS gb
FROM analytics.bi_sales_fact_sales_recognition fh
JOIN analytics.bi_pnlop_fact_current_model pnl
  ON fh.product_id = pnl.product_id
  AND pnl.date_reservation_year_month >= '2024-01'
-- aquí agregamos la unión con el CTE
JOIN bo_tpc bo
  ON bo.product_id_original = fh.origin_product_id
WHERE
  fh.gestion_date       >= DATE('2024-01-01')
  AND fh.partition_period >= '2024-01'
  AND month(fh.gestion_date) <= month(current_date)
  AND fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
  AND pnl.line_of_business = 'B2B'
  AND fh.channel NOT IN ('bestday-wl-mobile','affiliate-sicoob')
  AND fh.partner_id IN ('AP12576','AP12577')
  AND bo.bo_status = 'EMITTED'    -- ahora esta columna sí existe
GROUP BY 1
ORDER BY 1;


------------------------------------------------------------------------------
  -----------------------------------------------------------------------------
  
  
  ---------
  ------ Metemos country corregidos
  
  
  WITH bo_tpc AS (
  SELECT 
  p.transaction_id AS product_id_original,
  /* Métricas */
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd -- Third Party Commission
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE('2024-01-01') 
  AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY 1
)
  SELECT 
  gestion_date,
  case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
CASE
  -- 1) Partner overrides a 'PY'
  WHEN fh.partner_id IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  -- 2) Todos los partner_id que originalmente iban a *_CTA → 'CTA'
  WHEN fh.partner_id IN (
    'P12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  -- 3) Si no hay override de partner, clasificar por country_code original:
  -- 3a) Mantener códigos tal cual para estos países
  WHEN fh.country_code IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN fh.country_code
  -- 3b) Estados Unidos, Panamá, España, Costa Rica → 'GL'
  WHEN fh.country_code IN ('US','PA','ES','CR') THEN 'GL'
  -- 3c) Uruguay o Bolivia → 'UY'
  WHEN fh.country_code IN ('UY','BO') THEN 'UY'
  -- 4) Resto de casos → 'GL'
  ELSE 'GL'
END
AS country_code,
  case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
  ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb 
--   ,count(distinct bo.product_id_original) as bookings
  ,ROUND(
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
  AND partition_period > '2023-01'
  AND month(fh.gestion_date) <= month(current_date)
  AND 
   fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
  GROUP BY 1,2,3,4,5
  
  
  
  
  
