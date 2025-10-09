---QBRs API basado en API KPIs PBI

---QBRs API basado en API KPIs PBI

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
  WHERE CAST(s.created AS DATE) > DATE '2023-12-31'
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY p.transaction_id
)
SELECT 
pr.product_id                 AS product_id,
 pr.is_latam_destination       AS is_latam_destination,
tx.transaction_code AS transaction_code,
tx.site                  AS pais,
    brr.client_nationality   AS client_nationality,
  --  bo.cancelled             AS cancelled,
    bo.bo_status             AS bo_status,
    pr.trip_type            AS Viaje,
    cast(tx.reservation_date AS DATE) AS fecha_reserva,
    pr.checkin_date        AS fecha_llegada,
 --   MAX(pr.checkout_date)         AS fecha_salida,
 --   MAX(pr.product_cancel_date)   AS fecha_cancelacion_directa,
--    MAX(fv.confirmation_date)     AS fecha_confirmacion,
--    MAX(fv.cancellation_end_date) AS fecha_cancelacion,
--    MAX(fv.payment_type)          AS tipo_pago,
--    MAX(fv.cost)                  AS cost,
    tx.purchase_type         AS producto_original,
--    MAX(pr.product_type)          AS producto,
    pr.hotel_name            AS Hotel,
--    MAX(pr.hotel_penalty_date)    AS fecha_penalidad,
    pr.hotel_chain_name      AS Cadena,
--    MAX(pr.anticipation)          AS Anticipacion,
    pr.destination_city      AS Destino,
   -- ct.continent             AS ContinentDestino,
    pr.destination_country_code AS CodPaisDestino,
     fv.currency_code as currency_code,
 --   MAX(tx.channel)                AS channel,
    tx.parent_channel         AS parent_channel,
--    sp.estado_producto        AS estado_producto,
    IF(tx.channel = 'expedia', 'expedia', tx.partner_data_id)    AS partner_id,
    tx.line_of_business       AS lob,  
    tx.brand                  AS brand,
    IF(tx.channel = 'expedia', 'veronica.odetti@hoteldo.com', ca.director)   as director,
    pr.gateway                AS gateway,
 --   MAX(pr.provider_code)          AS provider,
    pr.hotel_despegar_id      AS hotelid,
    ----- METRICAS -----
    COUNT(DISTINCT tx.transaction_code) AS orders,
    SUM(
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
 --    SUM(fv.gestion_gb)   AS gb_s_gradiente,
--    SUM(fv.gestion_gb * fv.confirmation_gradient) AS gb_cgx,   
 --       SUM(fv.gb_in_local_currency) as gb_lc,
 --      SUM(fv.gb_in_local_currency * fv.confirmation_gradient) as gestion_gb_lc,
--    MAX(pr.effective_rate)         AS tarifaefectiva,
--    MAX(pr.is_refundable)          AS is_refundable,
--    MAX(pr.total_passengers_quantity) AS cantidad_pasajeros,
 --   MAX(ca.utility)                AS utilidad,
 --   MAX(ca.total)                  AS total_gb,
 --   MAX(ca.fee)                    AS fee_bruto,
 --   MAX(ca.commission)             AS commission_bruta,
--    MAX(ca.discount)               AS descuento_bruto,
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
    SUM(b.currency_hedge_air_usd)  AS currency_hedge_air_usd
--    MAX(t.rule_id)                 AS rule_id,
--    MAX(t.rule_name)               AS rule_name,
--    MAX(t.closed_percentage_fee)   AS closed_percentage_fee,
--    MAX(fv.product_is_confirmed_flg) AS is_confirmed_flg_recognition,
 --   MAX(fv.product_status)         AS product_status_recognition
FROM data.analytics.bi_transactional_fact_products pr
JOIN data.analytics.bi_transactional_fact_transactions tx
  ON tx.transaction_code = pr.transaction_code
  AND tx.reservation_year_month > DATE('2023-12-31')
LEFT JOIN data.analytics.bi_transactional_fact_charges ca
  ON pr.product_id = ca.product_id
  AND ca.reservation_year_month > DATE('2023-12-31')
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
    WHERE producto.reservation_year_month > DATE('2023-12-31')
) sp
  ON pr.product_id = sp.product_id
LEFT JOIN data.analytics.bi_sales_fact_sales_recognition fv
  ON fv.product_id = pr.product_id
  AND fv.partition_period > '2023-12'
LEFT JOIN data.lake.ch_bo_partner_partner partner
  ON partner.partner_code = tx.partner_data_id 
left join raw.cartera_b2b_v1 ca
  on ca.agency_code = tx.partner_data_id
left join data.lake.bookedia_rsv_reservation brr 
  ON pr.product_id = concat('20', brr.transaction_id)
LEFT JOIN data.analytics.bi_pnlop_fact_current_model b
  ON b.product_id = pr.product_id
  AND b.date_reservation_year_month > '2023-12-31'
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
    WHERE x.reservation_year_month > DATE('2023-12-31')
) t
  ON tx.transaction_code = t.transaction_code
-- AÑADIMOS AQUÍ EL JOIN AL CTE bo_tpc
  LEFT JOIN bo_tpc bo
   ON bo.product_id_original = fv.origin_product_id
WHERE 1=1
  AND pr.reservation_year_month > DATE('2023-12-31')
  AND pr.reservation_year_month < CURRENT_DATE
  AND tx.reservation_date     > DATE('2023-12-31')
  AND tx.reservation_date     < CURRENT_DATE
 AND tx.parent_channel in ('API', 'Agencias afiliadas')
--- AND tx.partner_data_id in ('AP12576', 'AP12577', 'AP12425') --------- partrner
 --AND pr.reservation_year_month > DATE('2025-09-20')  ---- fecha
 --------> filtro Globales + Brasil
AND (
        ca.director = 'veronica.odetti@hoteldo.com'
    OR tx.channel = 'expedia'
    OR (
           (ca.director = 'TBD' OR ca.director IS NULL)
           AND tx.site IN ('Usa', 'Panama', 'España')
       )
    OR (tx.site IN ('Brasil') AND tx.parent_channel in ('API'))
)
  ----------------
 -- and tx.site in ('Brasil')
 --and bo.bo_status = 'EMITTED'
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
    
    

select *
from data.analytics.bi_pnlop_fact_current_model b
where date_reservation_year_month is not null





------------------------------
--- subconsulta

-- QBRs API basado en API KPIs PBI (envuelto en subconsulta)
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
  WHERE CAST(s.created AS DATE) > DATE'2023-12-31'
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY p.transaction_id
),
base AS (
  SELECT 
       tx.site                                  AS pais,
       brr.client_nationality                   AS client_nationality,
       bo.bo_status                             AS bo_status,
       pr.trip_type                             AS viaje,
       CAST(tx.reservation_date AS DATE)        AS fecha_reserva,
       pr.checkin_date                          AS fecha_llegada,
       tx.purchase_type                         AS producto_original,
       pr.hotel_name                            AS hotel,
       pr.hotel_chain_name                      AS cadena,
       pr.destination_city                      AS destino,
       pr.destination_country_code              AS codpaisdestino,
       fv.currency_code                         AS currency_code,
       tx.parent_channel                        AS parent_channel,
       IF(tx.channel = 'expedia', 'expedia', tx.partner_data_id) AS partner_id,
       tx.line_of_business                      AS lob,  
       tx.brand                                 AS brand,
       IF(tx.channel = 'expedia', 'veronica.odetti@hoteldo.com', car.director) AS director,
       pr.gateway                               AS gateway,
       pr.hotel_despegar_id                     AS hotelid,
       -- MÉTRICAS
       COUNT(DISTINCT tx.transaction_code)      AS orders,
       SUM(
         CASE 
           WHEN tx.site = 'Colombia' 
             AND pr.status = 'Activo' 
             AND pr.payment_type = 'Prepago' 
             THEN ch.total
           WHEN tx.channel LIKE '%hoteldo%' 
             THEN ch.total    
           ELSE (ch.gross_booking - ch.perceptions - ch.tax_pais)
         END
       )                                        AS gb_usd, 
       ROUND(SUM(fv.gestion_gb), 2)             AS gb_s_gradiente,
       ROUND(SUM(fv.gestion_gb * fv.confirmation_gradient), 2)    AS gb_cgx,   
       ROUND(SUM(fv.gb_in_local_currency), 2)                     AS gb_lc,
       ROUND(SUM(fv.gb_in_local_currency * fv.confirmation_gradient), 2) AS gestion_gb_lc,
       SUM(b.fee_net_usd)                       AS fee_neto,
       SUM(b.commission_net_usd)                AS comision_neta,
       SUM(-b.discounts_net_usd)                AS descuentos_neto,
       SUM(-b.affiliates_usd)                   AS affiliates_usd,
       SUM(b.coi_interest_usd)                  AS coi_interest_usd,
       SUM(-b.coi_usd)                          AS coi_usd,
       SUM(-b.ccp_usd)                          AS ccp_usd,
       SUM(b.other_incentives_air_usd)          AS other_incentives_air_usd,
       SUM(b.errors_usd)                        AS errors_usd,
       SUM(b.frauds_usd)                        AS frauds_usd,
       SUM(b.revenue_taxes_usd)                 AS revenue_taxes_usd,
       SUM(b.ott_usd)                           AS ott_usd,
       SUM(b.backend_air_usd)                   AS backend_air_usd,
       SUM(b.backend_non_air_usd)               AS backend_non_air_usd,
       SUM(b.bad_debt_usd)                      AS bad_debt_usd,
       SUM(b.breakage_revenue_usd)              AS breakage_revenue_usd,
       SUM(b.agency_backend_usd)                AS agency_backend_usd,
       SUM(b.customer_claims_usd)               AS customer_claims_usd,
       SUM(b.other_incentives_non_air_usd)      AS other_incentives_non_air_usd,
       SUM(b.customer_service_usd)              AS customer_service_usd,
       SUM(-b.cancellations_usd)                AS cancellations_usd,
       SUM(b.margin_net_usd)                    AS margin_net_usd,
       SUM(b.margin_variable_net_usd)           AS margen_var_neto,
       SUM(b.net_revenues_usd)                  AS net_revenue_usd,
       SUM(b.npv_net_usd)                       AS npv_net_usd,
       SUM(b.fee_dynamic_usd)                   AS fee_dynamic_usd,
       SUM(CAST(b.b2b_gradient_gb AS DOUBLE))   AS b2b_gradient_gb,
       SUM(CAST(b.b2b_gradient_margin AS DOUBLE)) AS b2b_gradient_margin,
       SUM(b.financial_result_usd)              AS financial_result_usd,
       SUM(b.gb_without_distorted_taxes_usd)    AS gb_without_distorted_taxes_usd,
       SUM(b.discounts_mkt_funds_usd)           AS discounts_mkt_funds_usd,
       SUM(b.media_revenue_usd)                 AS media_revenue_usd,
       SUM(b.loyalty_usd)                       AS loyalty_usd,
       SUM(b.fee_income_mkt_cmr_usd)            AS fee_income_mkt_cmr_usd,
       SUM(b.mkt_fee_cost_cmr_usd)              AS mkt_fee_cost_cmr_usd,
       SUM(b.variable_charges_without_mkt_usd)  AS variable_charges_without_mkt_usd,
       SUM(b.dif_fx_usd)                        AS dif_fx_usd,
       SUM(b.dif_fx_air_usd)                    AS dif_fx_air_usd,
       SUM(b.currency_hedge_usd)                AS currency_hedge_usd,
       SUM(b.currency_hedge_air_usd)            AS currency_hedge_air_usd
  FROM data.analytics.bi_transactional_fact_products pr
  JOIN data.analytics.bi_transactional_fact_transactions tx
    ON tx.transaction_code = pr.transaction_code
   AND tx.reservation_date > DATE'2023-12-31'
   AND tx.reservation_date < CURRENT_DATE
  LEFT JOIN data.analytics.bi_transactional_fact_charges ch
    ON pr.product_id = ch.product_id
   AND ch.reservation_year_month > '2023-12-31'
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
      WHERE producto.reservation_year_month > DATE'2023-12-31'
  ) sp
    ON pr.product_id = sp.product_id
  LEFT JOIN data.analytics.bi_sales_fact_sales_recognition fv
    ON fv.product_id = pr.product_id
   AND fv.partition_period > '2023-12'
  LEFT JOIN data.lake.ch_bo_partner_partner partner
    ON partner.partner_code = tx.partner_data_id 
  LEFT JOIN raw.cartera_b2b_v1 car
    ON car.agency_code = tx.partner_data_id
  LEFT JOIN data.lake.bookedia_rsv_reservation brr 
    ON pr.product_id = CONCAT('20', brr.transaction_id)
  LEFT JOIN data.analytics.bi_pnlop_fact_current_model b
    ON b.product_id = pr.product_id
   AND b.date_reservation_year_month > DATE'2023-12-31'
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
      WHERE x.reservation_year_month > DATE'2023-12-31'
  ) t
    ON tx.transaction_code = t.transaction_code
  LEFT JOIN bo_tpc bo
    ON bo.product_id_original = fv.origin_product_id
  WHERE
      pr.reservation_year_month > DATE'2023-12-31'
      AND pr.reservation_year_month < CURRENT_DATE
  AND tx.parent_channel IN ('API')
  AND tx.partner_data_id IN ('AP12576')  -- partner
  -- Filtro Globales + Brasil
  AND (
        car.director = 'veronica.odetti@hoteldo.com'
     OR tx.channel = 'expedia'
     OR (
            (car.director = 'TBD' OR car.director IS NULL)
         AND tx.site IN ('Usa', 'Panama', 'España')
        )
     OR tx.site IN ('Brasil')
  )
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12,13,14,15,16,17,18,19
)
-- >>> Aquí eliges las columnas que quieras <<<
SELECT
  partner_id,
  sum(gb_usd) as gb
  -- pais, client_nationality, bo_status, viaje, fecha_reserva, fecha_llegada,
  -- producto_original, hotel, cadena, destino, codpaisdestino,
  -- currency_code, parent_channel, partner_id, lob, brand, director, gateway, hotelid,
  -- orders, gb_usd, gb_s_gradiente, gb_cgx, gb_lc, gestion_gb_lc,
  -- fee_neto, comision_neta, descuentos_neto, net_revenue_usd
FROM base
--where analytics.bi_transactional_fact_transactions.reservation_year_month is not null
group by 1
--ORDER BY fecha_reserva, fecha_llegada DESC

analytics.bi_transactional_fact_transactions
    ----- Validacion GB
    
    
    
    
    
    
    -- QBRs API: Total GB por país
SELECT
    tx.site AS pais,
    ROUND(SUM(
        CASE
            WHEN tx.site = 'Colombia'
             AND pr.status = 'Activo'
             AND pr.payment_type = 'Prepago'
                THEN ch.total
            WHEN tx.channel LIKE '%hoteldo%'
                THEN ch.total
            ELSE (ch.gross_booking - ch.perceptions - ch.tax_pais)  -- ajusta 'tax_pais' si el nombre difiere
        END
    ),0) AS gb_usd
FROM data.analytics.bi_transactional_fact_products         pr
JOIN data.analytics.bi_transactional_fact_transactions     tx
  ON tx.transaction_code = pr.transaction_code
 AND tx.reservation_year_month > DATE '2023-12-31'
LEFT JOIN data.analytics.bi_transactional_fact_charges     ch   -- antes 'ca'
  ON pr.product_id = ch.product_id
 AND ch.reservation_year_month > DATE '2023-12-31'
LEFT JOIN raw.cartera_b2b_v1                               car  -- antes también 'ca'; aquí va cartera
  ON car.agency_code = tx.partner_data_id
WHERE
      pr.reservation_year_month > DATE '2023-12-31'
  AND pr.reservation_year_month < CURRENT_DATE
  AND tx.reservation_date      > DATE '2024-12-31'
  AND tx.reservation_date      < CURRENT_DATE
  AND tx.parent_channel IN ('API')
  AND tx.partner_data_id in ('AP12576','AP12577')
   -- --------> filtro Globales + Brasil
  AND (
        car.director = 'veronica.odetti@hoteldo.com'
     OR tx.channel = 'expedia'
     OR ( (car.director = 'TBD' OR car.director IS NULL)
          AND tx.site IN ('Usa', 'Panama', 'España') )
     OR tx.site IN ('Brasil')
  )
GROUP BY
    tx.site
ORDER BY
    gb_usd DESC;

    
    
    
    
    
    
    
    
    
    
    
    
    
    --- Conteo filas
    
    
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
  WHERE CAST(s.created AS DATE) > DATE '2023-12-31'
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY p.transaction_id
),
base AS (
  SELECT 
       tx.site                  AS pais,
       brr.client_nationality   AS client_nationality,
   --    bo.cancelled             AS cancelled,
  --     bo.bo_status             AS bo_status,
       pr.trip_type             AS Viaje,
       CAST(tx.reservation_date AS DATE) AS fecha_reserva,
       pr.checkin_date          AS fecha_llegada,
       pr.destination_city      AS Destino,
       ct.continent             AS ContinentDestino,
       pr.destination_country_code AS CodPaisDestino,
  --     fv.currency_code         AS currency_code,
       tx.parent_channel        AS parent_channel,
   --    sp.estado_producto       AS estado_producto,
       IF(tx.channel = 'expedia', 'expedia', tx.partner_data_id) AS partner_id,
       tx.line_of_business      AS lob,  
       tx.brand                 AS brand,
       IF(tx.channel = 'expedia', 'veronica.odetti@hoteldo.com', ca.director) as director,
       pr.hotel_despegar_id     AS hotelid,
       COUNT(DISTINCT tx.transaction_code) AS orders,
       SUM(
         CASE 
           WHEN tx.site = 'Colombia' 
            AND pr.status = 'Activo' 
            AND pr.payment_type = 'Prepago' 
             THEN ca.total 
           WHEN tx.channel LIKE '%hoteldo%' 
             THEN ca.total    
           ELSE (ca.gross_booking - ca.perceptions - tax_pais) 
         END
       ) AS gb_usd
       -- ... resto de métricas y SUM(...) que ya tenías ...
FROM data.analytics.bi_transactional_fact_products pr
JOIN data.analytics.bi_transactional_fact_transactions tx
  ON tx.transaction_code = pr.transaction_code
  AND tx.reservation_year_month > DATE('2023-12-31')
LEFT JOIN data.analytics.bi_transactional_fact_charges ca
  ON pr.product_id = ca.product_id
  AND ca.reservation_year_month > DATE('2023-12-31')
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
    WHERE producto.reservation_year_month > DATE('2023-12-31')
) sp
  ON pr.product_id = sp.product_id
LEFT JOIN data.analytics.bi_sales_fact_sales_recognition fv
  ON fv.product_id = pr.product_id
  AND fv.partition_period > '2023-12'
LEFT JOIN data.lake.ch_bo_partner_partner partner
  ON partner.partner_code = tx.partner_data_id 
left join raw.cartera_b2b_v1 ca
  on ca.agency_code = tx.partner_data_id
left join data.lake.bookedia_rsv_reservation brr 
  ON pr.product_id = concat('20', brr.transaction_id)
LEFT JOIN data.analytics.bi_pnlop_fact_current_model b
  ON b.product_id = pr.product_id
  AND b.date_reservation_year_month > '2023-12-31'
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
    WHERE x.reservation_year_month > DATE('2023-12-31')
) t
  ON tx.transaction_code = t.transaction_code
-- AÑADIMOS AQUÍ EL JOIN AL CTE bo_tpc
  LEFT JOIN bo_tpc bo
   ON bo.product_id_original = fv.origin_product_id
WHERE 1=1
  AND pr.reservation_year_month > DATE('2023-12-31')
  AND pr.reservation_year_month < CURRENT_DATE
  AND tx.reservation_date     > DATE('2023-12-31')
  AND tx.reservation_date     < CURRENT_DATE
 AND tx.parent_channel in ('API')
  --------> filtro Globales
AND (
        ca.director = 'veronica.odetti@hoteldo.com'
     OR tx.channel = 'expedia'
     OR (
           (ca.director = 'TBD' OR ca.director IS NULL)
           AND tx.site IN ('Usa', 'Panama', 'España')
        )
  )
  ----------------
  GROUP BY
      1,2,3,4,5,6,7,8,9,10,
      11,12,13,14
)
SELECT COUNT(*) AS total_filas
FROM base;
