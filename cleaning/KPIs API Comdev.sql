----------------------------------
                    
      -- ANTERIOR API enriquecida con bo_tpc.cancelled y bo_tpc.bo_status

WITH bo_tpc AS (
  SELECT 
    p.transaction_id AS product_id_original,
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd,
    MAX(CAST(p.cancelled AS DATE)) AS cancelled,
    MAX(
      CASE 
        WHEN COALESCE(p.status, '') = '' 
          THEN 'ACTIVE'
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
    tx.transaction_code           AS transaction_code,
    pr.product_id                 AS product_id,
    pr.is_latam_destination       AS is_latam_destination,
    -- Nuestras nuevas columnas agregadas
    MAX(brr.client_nationality)   AS client_nationality,
    MAX(bo.cancelled)             AS cancelled,
    MAX(bo.bo_status)             AS bo_status,
    MAX(tx.site)                  AS pais,
    MAX(pr.trip_type)             AS Viaje,
    CAST(MAX(tx.reservation_date) AS DATE) AS fecha_reserva,
    MAX(pr.checkin_date)          AS fecha_llegada,
    MAX(pr.checkout_date)         AS fecha_salida,
    MAX(pr.product_cancel_date)   AS fecha_cancelacion_directa,
    MAX(fv.confirmation_date)     AS fecha_confirmacion,
    MAX(fv.cancellation_end_date) AS fecha_cancelacion,
    MAX(fv.payment_type)          AS tipo_pago,
    MAX(fv.cost)                  AS cost,
    MAX(tx.purchase_type)         AS producto_original,
    MAX(pr.product_type)          AS producto,
    MAX(pr.hotel_name)            AS Hotel,
    MAX(pr.hotel_penalty_date)    AS fecha_penalidad,
    MAX(pr.hotel_chain_name)      AS Cadena,
    MAX(pr.anticipation)          AS Anticipacion,
    MAX(pr.destination_city)      AS Destino,
    MAX(ct.continent)             AS ContinentDestino,
    MAX(pr.destination_country_code) AS CodPaisDestino,
    MAX(
      CASE 
        WHEN tx.site = 'Colombia' 
         AND pr.status = 'Activo' 
         AND pr.payment_type = 'Prepago' 
          THEN ca.total 
        WHEN tx.channel LIKE '%hoteldo%' 
          THEN ca.total    
        ELSE (ca.gross_booking - ca.perceptions - tax_pais) 
      END
    )                              AS gb_usd, 
    ROUND(MAX(fv.gestion_gb), 2)    AS gb_s_gradiente,
    ROUND(MAX(fv.gestion_gb * fv.confirmation_gradient), 2) AS gb_cgx,   
         ROUND(MAX(fv.gb_in_local_currency),2) as gb_lc,
         ROUND(MAX(fv.gb_in_local_currency * fv.confirmation_gradient),2) as gestion_gb_lc,
          MAX(fv.currency_code) as currency_code,
    MAX(tx.channel)                AS channel,
    MAX(tx.parent_channel)         AS parent_channel,
    MAX(sp.estado_producto)        AS estado_producto,
    MAX(
      IF(tx.channel = 'expedia', 'expedia', tx.partner_data_id)
    )                              AS partner_id,
    MAX(tx.line_of_business)       AS lob,
    MAX(tx.brand)                  AS brand,
    MAX(pr.gateway)                AS gateway,
    MAX(pr.provider_code)          AS provider,
    MAX(pr.hotel_despegar_id)      AS hotelid,
    MAX(pr.effective_rate)         AS tarifaefectiva,
    MAX(pr.is_refundable)          AS is_refundable,
    MAX(pr.total_passengers_quantity) AS cantidad_pasajeros,
    MAX(ca.utility)                AS utilidad,
    MAX(ca.total)                  AS total_gb,
    MAX(ca.fee)                    AS fee_bruto,
    MAX(ca.commission)             AS commission_bruta,
    MAX(ca.discount)               AS descuento_bruto,
    SUM(b.fee_net_usd)             AS fee_neto,
    SUM(b.commission_net_usd)      AS comision_neta,
    SUM(-b.discounts_net_usd)      AS descuentos_neto,
    SUM(-b.affiliates_usd)         AS affiliates_usd,
    SUM(b.coi_interest_usd)        AS coi_interest_usd,
    SUM(-b.coi_usd)                AS coi_usd,
    SUM(-b.ccp_usd)                AS ccp_usd,
    SUM(b.other_incentives_air_usd)        AS other_incentives_air_usd,
    SUM(b.errors_usd)              AS errors_usd,
    SUM(b.frauds_usd)              AS frauds_usd,
    SUM(b.revenue_taxes_usd)       AS revenue_taxes_usd,
    SUM(b.ott_usd)                 AS ott_usd,
    SUM(b.backend_air_usd)         AS backend_air_usd,
    SUM(b.backend_non_air_usd)     AS backend_non_air_usd,
    SUM(b.bad_debt_usd)            AS bad_debt_usd,
    SUM(b.breakage_revenue_usd)    AS breakage_revenue_usd,
    SUM(b.agency_backend_usd)      AS agency_backend_usd,
    SUM(b.customer_claims_usd)     AS customer_claims_usd,
    SUM(b.other_incentives_non_air_usd) AS other_incentives_non_air_usd,
    SUM(b.customer_service_usd)    AS customer_service_usd,
    SUM(-b.cancellations_usd)      AS cancellations_usd,
    SUM(b.margin_net_usd)          AS margin_net_usd,
    SUM(b.margin_variable_net_usd) AS margen_var_neto,
    SUM(b.net_revenues_usd)        AS net_revenue_usd,
    SUM(b.npv_net_usd)             AS npv_net_usd,
    SUM(b.fee_dynamic_usd)         AS fee_dynamic_usd,
    SUM(CAST(b.b2b_gradient_gb AS DOUBLE))     AS b2b_gradient_gb,
    SUM(CAST(b.b2b_gradient_margin AS DOUBLE)) AS b2b_gradient_margin,
    SUM(b.financial_result_usd)    AS financial_result_usd,
    SUM(b.gb_without_distorted_taxes_usd)      AS gb_without_distorted_taxes_usd,
    SUM(b.discounts_mkt_funds_usd) AS discounts_mkt_funds_usd,
    SUM(b.media_revenue_usd)       AS media_revenue_usd,
    SUM(b.loyalty_usd)             AS loyalty_usd,
    SUM(b.fee_income_mkt_cmr_usd)  AS fee_income_mkt_cmr_usd,
    SUM(b.mkt_fee_cost_cmr_usd)    AS mkt_fee_cost_cmr_usd,
    SUM(b.variable_charges_without_mkt_usd)    AS variable_charges_without_mkt_usd,
    SUM(b.dif_fx_usd)              AS dif_fx_usd,
    SUM(b.dif_fx_air_usd)          AS dif_fx_air_usd,
    SUM(b.currency_hedge_usd)      AS currency_hedge_usd,
    SUM(b.currency_hedge_air_usd)  AS currency_hedge_air_usd,
    MAX(t.rule_id)                 AS rule_id,
    MAX(t.rule_name)               AS rule_name,
    MAX(t.closed_percentage_fee)   AS closed_percentage_fee,
    MAX(fv.product_is_confirmed_flg) AS is_confirmed_flg_recognition,
    MAX(fv.product_status)         AS product_status_recognition
FROM data.analytics.bi_transactional_fact_products pr
JOIN data.analytics.bi_transactional_fact_transactions tx
  ON tx.transaction_code = pr.transaction_code
  AND tx.reservation_year_month > DATE('2025-04-30')
LEFT JOIN data.analytics.bi_transactional_fact_charges ca
  ON pr.product_id = ca.product_id
  AND ca.reservation_year_month > DATE('2025-04-30')
LEFT JOIN (
    SELECT DISTINCT country_code, continent
    FROM data.analytics.mkt_users_dim_cities
) ct
  ON ct.country_code = pr.destination_country_code
LEFT JOIN (
    SELECT 
        producto.product_id,
        COALESCE(bi_transactional_fact_products_current_state.product_state, producto.status) AS estado_producto
    FROM analytics.bi_transactional_fact_products AS producto
    LEFT JOIN analytics.bi_transactional_fact_products_current_state  
      ON producto.product_id = bi_transactional_fact_products_current_state.product_id
    WHERE producto.reservation_year_month > DATE('2025-04-30')
) sp
  ON pr.product_id = sp.product_id
LEFT JOIN data.analytics.bi_sales_fact_sales_recognition fv
  ON fv.product_id = pr.product_id
  AND fv.partition_period > '2023-12'
LEFT JOIN data.lake.ch_bo_partner_partner partner
  ON partner.partner_code = tx.partner_data_id
left join data.lake.bookedia_rsv_reservation brr 
  ON pr.product_id = concat('20', brr.transaction_id)
LEFT JOIN data.analytics.bi_pnlop_fact_current_model b
  ON b.product_id = pr.product_id
  AND b.date_reservation_year_month > '2025-04-30'
LEFT JOIN (
    SELECT DISTINCT 
        x.transaction_code,
        rm.rule_id,
        rm.rule_name,
        rm.closed_percentage_fee
    FROM data.analytics.bi_transactional_fact_transactions x
    INNER JOIN data.lake.chewie_reservation r
      ON x.transaction_code = r.id 
    INNER JOIN data.lake.chewie_product p
      ON r.oid = p.reservation_id
    INNER JOIN data.lake.chewie_product_revenue_input_margin rm
      ON p.oid = rm.product_id 
    WHERE x.reservation_year_month > DATE('2025-04-30')
) t
  ON tx.transaction_code = t.transaction_code
-- AÑADIMOS AQUÍ EL JOIN AL CTE bo_tpc
  LEFT JOIN bo_tpc bo
   ON bo.product_id_original = fv.origin_product_id
WHERE 1=1
  AND pr.reservation_year_month > DATE('2025-04-30')
  AND pr.reservation_year_month < CURRENT_DATE
  AND tx.reservation_date     > DATE('2025-04-30')
  AND tx.reservation_date     < CURRENT_DATE
  AND tx.parent_channel = 'API'
  --and bo.bo_status = 'EMITTED'
GROUP BY
    1,2,3--,4
  
    
    ---- cartera de alojamiento
   
    
    WITH max_week AS (
  SELECT anio_semana
  FROM data.lake."bi_sourcing_cartera_alojamiento"
  where anio_semana is not null
  ORDER BY anio_semana DESC
  LIMIT 1
)
SELECT 
distinct(t.cadena), 
max(t.anio_semana) as partition_anio_semana
FROM data.lake.bi_sourcing_cartera_alojamiento t
JOIN max_week u ON t.anio_semana = u.anio_semana
where cadena is not null
and cadena <> 'HTL hoteles'
and cadena <> 'Casa hotéis'
and cadena <> 'Sirenis Hotels & resorts'
and cadena <> ''
and cadena <> 'Voa'
group by 1
order by cadena



 MAX(pr.hotel_name)            AS Hotel,
	    FROM data.analytics.bi_transactional_fact_products pr

----


---Cartera KAMs IVA:


    WITH max_partition_date AS (
  SELECT partition_date
  FROM lake.cartera_kam_ag 
  where partition_date is not null
  ORDER BY partition_date DESC
  LIMIT 1
)
select 
agency_code as agency_code,
ag_name as ag_name,
group_code as group_code,
group_name as group_name,
country as market,
CASE
  WHEN agency_code IN (
    'AP12142','AP12961','AP12767','AP12539','AP12792',
    'AP12149','AP12148','AG00015606','AP13029','AP13030',
    'AP13091','AP13104','AG00015611'
  ) THEN 'PY'
  WHEN agency_code IN (
    'AP12212','AP11666',    -- CR_CTA
    'AP12147','AP12854',   -- SV_CTA
    'AP12509','AP11813',   -- GT_CTA
    'AP12158',             -- PA_CTA
    'AP12213','AP11843',   -- HN_CTA
    'AP12439','AP12438','AP12449','AP12805',
    'AP12820','AP12900','AP12906','AP12896'  -- DO_CTA
  ) THEN 'CTA'
  WHEN country IN (
    'MX','BR','CO','AR','EC','PE','CL','PY'
  ) THEN country
  WHEN country IN ('US','PA','ES','CR') THEN 'GL'
  WHEN country IN ('UY','BO') THEN 'UY'
  ELSE 'GL'
END
AS country_code_corregido,
parent_channel as parent_channel,
region as region_html,
kam as kam,
director as lead,
manager as manager,
comdev.partition_date as partition_date
from  lake.cartera_kam_ag comdev
JOIN max_partition_date u ON comdev.partition_date = u.partition_date
where comdev.partition_date is not null


-----------------------------------------

SELECT DISTINCT 
	t.transaction_code,
	brr.client_nationality AS client_nationality
from data.analytics.bi_transactional_fact_products p
	left join data.analytics.bi_transactional_fact_transactions t on p.transaction_code = t.transaction_code
	left join data.lake.bookedia_rsv_reservation brr              on p.product_id = concat('20', brr.transaction_id)
where 1=1
	and t.reservation_year_month >= cast('2025-01-01' as date)
	and p.reservation_year_month >= cast('2025-01-01' as date)
	AND t.parent_channel = 'API'
	and t.reservation_date >= date '2025-06-27'
	
	
	   
	