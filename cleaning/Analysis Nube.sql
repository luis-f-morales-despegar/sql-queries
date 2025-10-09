 --- Power BI "Copia Analysis nube" (tabla "Data") - Yas --- actual 2025-05-05 -- se añade LATAM flag --- fecha_reserva > 2023-12-31  -- 2025-06-11 se añade fix fvm de Omar

 
  
with bo_tpc as( 
            select
                p.transaction_id as product_id_original
                /*metricas*/
                ,max(p.net_commission_partner * p.conversion_rate) as tpc_usd --third party commission
            from data.lake.channels_bo_product p
            join data.lake.channels_bo_sale s on s.id = p.sale_id
            where cast(s.created as date) >= DATE('2023-01-01') 
            and cast(s.created as date) < CURRENT_DATE
            group by 1
),
bt_detail as (
select 
    tx.transaction_code as transaction_code,
    pr.product_id AS product_id,
    pr.is_latam_destination,
    max(tx.site) as pais,
    max(pr.trip_type) as Viaje,
    CAST(MAX(tx.reservation_date) AS DATE) AS fecha_reserva,
    max(pr.checkin_date) as fecha_llegada,
    max(pr.checkout_date) as fecha_salida,
    max(pr.product_cancel_date) as fecha_cancelacion_directa,
    max(fh.confirmation_date) as fecha_confirmacion,
    max(fh.cancellation_end_date) as fecha_cancelacion,
    max(fh.payment_type) as tipo_pago,
    max(fh.cost) as cost,
    max(tx.purchase_type) as producto_original,
    max(pr.product_type) as producto,
    max(pr.hotel_name) as Hotel,
    max(pr.hotel_penalty_date) as fecha_penalidad,
    max(pr.hotel_chain_name) as Cadena,
    max(pr.anticipation) as Anticipacion,
    max(pr.destination_city) as Destino,
    max(ct.continent) as ContinentDestino,
    max(pr.destination_country_code) as CodPaisDestino,
    max(case when (tx.site = 'Colombia' and pr.status = 'Activo' and pr.payment_type = 'Prepago') then ca.total 
             when (tx.channel like '%hoteldo%') then ca.total    
             else (ca.gross_booking-ca.perceptions-tax_pais) end) as gb_usd, 
    ROUND(max(fh.gestion_gb), 2) AS gb_s_gradiente,
    ROUND(max(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb_cgx,       
    max(tx.channel) as channel,
    max(tx.parent_channel) as parent_channel,
    max(sp.estado_producto) as estado_producto,
    MAX(IF(tx.channel = 'expedia', 'expedia', tx.partner_data_id)) AS partner_id,     ---- se añade Expedia para que matche partner.id = reference.id de cartera Agencias_MB
    max(tx.line_of_business) as lob,
    max(tx.brand) as brand,
    max(pr.gateway) as gateway,
    max(pr.provider_code) as provider,
    max(pr.hotel_despegar_id) as hotelid,
    max(pr.effective_rate) as tarifaefectiva,
    max(pr.is_refundable) as is_refundable,
    max(pr.total_passengers_quantity) as cantidad_pasajeros,
    max(ca.utility) as utilidad,
    max(ca.total) as total_gb,
    max(ca.fee) as fee_bruto,
    max(ca.commission) as commission_bruta,
    max(ca.discount) as descuento_bruto,
 --   max(fh.recognition_date) as recognition_date,
    max(partner.name) as name_partner,
    sum(b.fee_net_usd) as fee_neto,
    sum(b.commission_net_usd) as comision_neta,
    sum(-b.discounts_net_usd) as descuentos_neto,
    sum(-b.affiliates_usd) as affiliates_usd,
    sum(b.coi_interest_usd) as coi_interest_usd,
    sum(-b.coi_usd) as coi_usd,
    sum(-b.ccp_usd) as ccp_usd,
    sum(b.other_incentives_air_usd) as other_incentives_air_usd,
    sum(b.errors_usd) as errors_usd,
    sum(b.frauds_usd) as frauds_usd,
    sum(b.revenue_taxes_usd) as revenue_taxes_usd,
    sum(b.ott_usd) as ott_usd,
    sum(b.backend_air_usd) as backend_air_usd,
    sum(b.backend_non_air_usd) as backend_non_air_usd,
    sum(b.bad_debt_usd) as bad_debt_usd,
    sum(b.breakage_revenue_usd) breakage_revenue_usd,
    sum(b.agency_backend_usd)as agency_backend_usd ,
    sum(b.customer_claims_usd) as customer_claims_usd,
    sum(b.other_incentives_non_air_usd) as other_incentives_non_air_usd,
    sum(b.customer_service_usd) as customer_service_usd,
    sum(-b.cancellations_usd) as cancellations_usd,
    sum(b.margin_net_usd) as margin_net_usd,
    sum(b.margin_variable_net_usd) as margen_var_neto,
    sum(b.net_revenues_usd) as net_revenue_usd,
  --  sum(b.npv_net_usd) as npv_net_usd,
    sum(b.fee_dynamic_usd) as fee_dynamic_usd,
    sum(cast(b.b2b_gradient_gb as double)) as b2b_gradient_gb,
    sum(cast(b.b2b_gradient_margin as double)) as b2b_gradient_margin,
    sum(b.financial_result_usd) as financial_result_usd,
    sum(b.gb_without_distorted_taxes_usd) as gb_without_distorted_taxes_usd,
    sum(b.discounts_mkt_funds_usd) as discounts_mkt_funds_usd,
    sum(b.media_revenue_usd) as media_revenue_usd,
    sum(b.loyalty_usd) as loyalty_usd,
    sum(b.fee_income_mkt_cmr_usd) as fee_income_mkt_cmr_usd,
    sum(b.mkt_fee_cost_cmr_usd) as mkt_fee_cost_cmr_usd,
    sum(b.variable_charges_without_mkt_usd) as variable_charges_without_mkt_usd,
    sum(b.dif_fx_usd) as dif_fx_usd,
    sum(b.dif_fx_air_usd) as dif_fx_air_usd,
    sum(b.currency_hedge_usd) as currency_hedge_usd,
    sum(b.currency_hedge_air_usd) as currency_hedge_air_usd,
    max(t.rule_id) as rule_id,
    max(t.rule_name) as rule_name,
    max(t.closed_percentage_fee) as closed_percentage_fee,
    max(fh.product_is_confirmed_flg) as is_confirmed_flg_recognition,
    max(fh.product_status) as product_status_recognition,
    sum(b.npv_net_usd) as fvm,
	                 sum(b.margin_net_usd + b.variable_charges_without_mkt_usd + b.financial_result_usd + price.dif_fx_usd + price.dif_fx_air_usd + price.currency_hedge_usd + price.currency_hedge_air_usd) as fvm_calc,
	                 sum(((b.margin_net_usd + b.variable_charges_without_mkt_usd + b.financial_result_usd 
        		             + price.dif_fx_usd + price.dif_fx_air_usd + price.currency_hedge_usd + price.currency_hedge_air_usd
        		             + ( case when fh.parent_channel = 'Agencias afiliadas' and fh.buy_type_code in ('Hoteles','Alquileres') and fh.product_is_confirmed_flg = 0    -- Fix COI CCP - Promesas de Pago
        		                        then (b.coi_usd + b.ccp_usd - b.financial_result_usd + coalesce(fpp.coi_fix_con_gradiente,-b.coi_usd + b.financial_result_usd) + coalesce(fpp.ccp_fix_con_gradiente,-b.ccp_usd)) 
        		                        else 0 end ) 
        		             + b.affiliates_usd)    -- sumamos afiliadas
                           / if(b.b2b_gradient_margin = '1', 1, cast(b.b2b_gradient_margin as decimal(4,3)) ) -- quitar gradiente para obtener bruto
                         )
                         - coalesce(fh.tpc_fix_iva,0)     -- quitar tpc (en sustitucion de afiliadas)
                        )
                         * max(if(b.b2b_gradient_margin = '1', 1, cast(b.b2b_gradient_margin as decimal(4,3)))) 
                        as fix_fvm          
from ( select *
			            ,case when fv_prev.parent_channel = 'Agencias afiliadas' 
			   			then tpc_usd/(1+coalesce(cast(fix_iva.iva as decimal(4,3)),0)) 
			   			else tpc_usd 
			   	        end as tpc_fix_iva 
		                from analytics.bi_sales_fact_sales_recognition fv_prev
		                left join bo_tpc bo on bo.product_id_original = fv_prev.origin_product_id
		                left join raw.b2b_dim_html_iva_fix fix_iva on fix_iva.country = fv_prev.country_code
		                where fv_prev.partition_period > '2023-01'
	                     ) fh
left join data.analytics.bi_transactional_fact_products pr on fh.product_id = pr.product_id
join data.analytics.bi_transactional_fact_transactions tx on tx.transaction_code = pr.transaction_code and tx.reservation_year_month > date('2023-12-31')
left join data.analytics.bi_transactional_fact_charges ca on pr.product_id = ca.product_id and ca.reservation_year_month > date('2023-12-31')
LEFT JOIN (
    SELECT DISTINCT country_code, continent
    FROM data.analytics.mkt_users_dim_cities
) ct 
    ON ct.country_code = pr.destination_country_code
left join (
    select 
        producto.product_id,
        coalesce(bi_transactional_fact_products_current_state.product_state, producto.status) as estado_producto
    from analytics.bi_transactional_fact_products as producto  
    left join analytics.bi_transactional_fact_products_current_state  
        on producto.product_id = bi_transactional_fact_products_current_state.product_id
    where producto.reservation_year_month > date('2023-12-31')
) sp on pr.product_id = sp.product_id
left join data.lake.ch_bo_partner_partner partner on partner.partner_code = tx.partner_data_id
left join data.analytics.bi_pnlop_fact_current_model b on b.product_id = pr.product_id and b.date_reservation_year_month > '2023-12-31'
left join (
    select distinct 
        x.transaction_code,
        rm.rule_id,
        rm.rule_name,
        rm.closed_percentage_fee
    from data.analytics.bi_transactional_fact_transactions x
    inner join data.lake.chewie_reservation r on x.transaction_code = r.id 
    inner join data.lake.chewie_product p on r.oid = p.reservation_id
    inner join data.lake.chewie_product_revenue_input_margin rm on p.oid = rm.product_id 
    where x.reservation_year_month > date('2023-12-31')
) t on tx.transaction_code = t.transaction_code
left join analytics.bi_pnlop_fact_pricing_model price on price.product_id = fh.product_id and price.date_reservation_year_month >= '2021-01'
left join bo_tpc bo on bo.product_id_original = fh.origin_product_id
left join data.lake.b2b_fix_coi_ccp fpp on fpp.transaction_code = cast(fh.transaction_code as varchar) -- Fix Promesas Pago
where 1=1
and pr.reservation_year_month > date('2023-12-31') and pr.reservation_year_month < CURRENT_DATE
and tx.reservation_date > date('2023-12-31') and tx.reservation_date < CURRENT_DATE
and fh.gestion_date >= DATE('2023-01-01')
and fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
and fh.lob_gestion in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
and b.line_of_business = 'B2B'
and fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
and fh.partition_period > '2020-01'
--and tx.reservation_date >= date('2024-01-01') and tx.reservation_date < date('2024-01-31') 
and tx.parent_channel in ('API') ----------------------------------------------------------------- Parent Channel
--and tx.transaction_code = '396667285500'      ----------------
group by tx.transaction_code, pr.product_id, pr.is_latam_destination
             )
SELECT * 
FROM bt_detail 
where 1=1
--and channel = 'expedia'
--and partner_id in ('AP12904') --'AP12903'
--order by fecha_reserva asc
limit 100


    

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
  AND tx.parent_channel = 'API'
  --and bo.bo_status = 'EMITTED'
GROUP BY
    1,2,3--,4
-- ORDER BY fecha_reserva ASC  -- si lo deseas    



----------------------------------------------------------------------


---- ANTERIOR API:
                    
                    
                    --- Power BI "Copia Analysis nube" (tabla "Data") - Yas --- actual 2025-05-05 -- se añade LATAM flag --- fecha_reserva > 2023-12-31

select 
    tx.transaction_code as transaction_code,
    pr.product_id AS product_id,
    pr.is_latam_destination,
    max(tx.site) as pais,
    max(pr.trip_type) as Viaje,
    CAST(MAX(tx.reservation_date) AS DATE) AS fecha_reserva,
    max(pr.checkin_date) as fecha_llegada,
    max(pr.checkout_date) as fecha_salida,
    max(pr.product_cancel_date) as fecha_cancelacion_directa,
    max(fv.confirmation_date) as fecha_confirmacion,
    max(fv.cancellation_end_date) as fecha_cancelacion,
    max(fv.payment_type) as tipo_pago,
    max(fv.cost) as cost,
    max(tx.purchase_type) as producto_original,
    max(pr.product_type) as producto,
    max(pr.hotel_name) as Hotel,
    max(pr.hotel_penalty_date) as fecha_penalidad,
    max(pr.hotel_chain_name) as Cadena,
    max(pr.anticipation) as Anticipacion,
    max(pr.destination_city) as Destino,
    max(ct.continent) as ContinentDestino,
    max(pr.destination_country_code) as CodPaisDestino,
    max(case when (tx.site = 'Colombia' and pr.status = 'Activo' and pr.payment_type = 'Prepago') then ca.total 
             when (tx.channel like '%hoteldo%') then ca.total    
             else (ca.gross_booking-ca.perceptions-tax_pais) end) as gb_usd, 
    ROUND(max(fv.gestion_gb), 2) AS gb_s_gradiente,
    ROUND(max(fv.gestion_gb * fv.confirmation_gradient), 2) AS gb_cgx,       
    max(tx.channel) as channel,
    max(tx.parent_channel) as parent_channel,
    max(sp.estado_producto) as estado_producto,
    MAX(IF(tx.channel = 'expedia', 'expedia', tx.partner_data_id)) AS partner_id,     ---- se añade Expedia para que matche partner.id = reference.id de cartera Agencias_MB
    max(tx.line_of_business) as lob,
    max(tx.brand) as brand,
    max(pr.gateway) as gateway,
    max(pr.provider_code) as provider,
    max(pr.hotel_despegar_id) as hotelid,
    max(pr.effective_rate) as tarifaefectiva,
    max(pr.is_refundable) as is_refundable,
    max(pr.total_passengers_quantity) as cantidad_pasajeros,
    max(ca.utility) as utilidad,
    max(ca.total) as total_gb,
    max(ca.fee) as fee_bruto,
    max(ca.commission) as commission_bruta,
    max(ca.discount) as descuento_bruto,
 --   max(fv.recognition_date) as recognition_date,
    max(partner.name) as name_partner,
    sum(b.fee_net_usd) as fee_neto,
    sum(b.commission_net_usd) as comision_neta,
    sum(-b.discounts_net_usd) as descuentos_neto,
    sum(-b.affiliates_usd) as affiliates_usd,
    sum(b.coi_interest_usd) as coi_interest_usd,
    sum(-b.coi_usd) as coi_usd,
    sum(-b.ccp_usd) as ccp_usd,
    sum(b.other_incentives_air_usd) as other_incentives_air_usd,
    sum(b.errors_usd) as errors_usd,
    sum(b.frauds_usd) as frauds_usd,
    sum(b.revenue_taxes_usd) as revenue_taxes_usd,
    sum(b.ott_usd) as ott_usd,
    sum(b.backend_air_usd) as backend_air_usd,
    sum(b.backend_non_air_usd) as backend_non_air_usd,
    sum(b.bad_debt_usd) as bad_debt_usd,
    sum(b.breakage_revenue_usd) breakage_revenue_usd,
    sum(b.agency_backend_usd)as agency_backend_usd ,
    sum(b.customer_claims_usd) as customer_claims_usd,
    sum(b.other_incentives_non_air_usd) as other_incentives_non_air_usd,
    sum(b.customer_service_usd) as customer_service_usd,
    sum(-b.cancellations_usd) as cancellations_usd,
    sum(b.margin_net_usd) as margin_net_usd,
    sum(b.margin_variable_net_usd) as margen_var_neto,
    sum(b.net_revenues_usd) as net_revenue_usd,
    sum(b.npv_net_usd) as npv_net_usd,
    sum(b.fee_dynamic_usd) as fee_dynamic_usd,
    sum(cast(b.b2b_gradient_gb as double)) as b2b_gradient_gb,
    sum(cast(b.b2b_gradient_margin as double)) as b2b_gradient_margin,
    sum(b.financial_result_usd) as financial_result_usd,
    sum(b.gb_without_distorted_taxes_usd) as gb_without_distorted_taxes_usd,
    sum(b.discounts_mkt_funds_usd) as discounts_mkt_funds_usd,
    sum(b.media_revenue_usd) as media_revenue_usd,
    sum(b.loyalty_usd) as loyalty_usd,
    sum(b.fee_income_mkt_cmr_usd) as fee_income_mkt_cmr_usd,
    sum(b.mkt_fee_cost_cmr_usd) as mkt_fee_cost_cmr_usd,
    sum(b.variable_charges_without_mkt_usd) as variable_charges_without_mkt_usd,
    sum(b.dif_fx_usd) as dif_fx_usd,
    sum(b.dif_fx_air_usd) as dif_fx_air_usd,
    sum(b.currency_hedge_usd) as currency_hedge_usd,
    sum(b.currency_hedge_air_usd) as currency_hedge_air_usd,
    max(t.rule_id) as rule_id,
    max(t.rule_name) as rule_name,
    max(t.closed_percentage_fee) as closed_percentage_fee,
    max(fv.product_is_confirmed_flg) as is_confirmed_flg_recognition,
    max(fv.product_status) as product_status_recognition
from data.analytics.bi_transactional_fact_products pr 
join data.analytics.bi_transactional_fact_transactions tx on tx.transaction_code = pr.transaction_code and tx.reservation_year_month > date('2023-12-31')
left join data.analytics.bi_transactional_fact_charges ca on pr.product_id = ca.product_id and ca.reservation_year_month > date('2023-12-31')
LEFT JOIN (
    SELECT DISTINCT country_code, continent
    FROM data.analytics.mkt_users_dim_cities
) ct 
    ON ct.country_code = pr.destination_country_code
left join (
    select 
        producto.product_id,
        coalesce(bi_transactional_fact_products_current_state.product_state, producto.status) as estado_producto
    from analytics.bi_transactional_fact_products as producto  
    left join analytics.bi_transactional_fact_products_current_state  
        on producto.product_id = bi_transactional_fact_products_current_state.product_id
    where producto.reservation_year_month > date('2023-12-31')
) sp on pr.product_id = sp.product_id
left join data.analytics.bi_sales_fact_sales_recognition fv on fv.product_id = pr.product_id and fv.partition_period > '2023-12'
left join data.lake.ch_bo_partner_partner partner on partner.partner_code = tx.partner_data_id
left join data.analytics.bi_pnlop_fact_current_model b on b.product_id = pr.product_id and b.date_reservation_year_month > '2023-12-31'
left join (
    select distinct 
        x.transaction_code,
        rm.rule_id,
        rm.rule_name,
        rm.closed_percentage_fee
    from data.analytics.bi_transactional_fact_transactions x
    inner join data.lake.chewie_reservation r on x.transaction_code = r.id 
    inner join data.lake.chewie_product p on r.oid = p.reservation_id
    inner join data.lake.chewie_product_revenue_input_margin rm on p.oid = rm.product_id 
    where x.reservation_year_month > date('2023-12-31')
) t on tx.transaction_code = t.transaction_code
where 1=1
and pr.reservation_year_month > date('2023-12-31') and pr.reservation_year_month < CURRENT_DATE
and tx.reservation_date > date('2023-12-31') and tx.reservation_date < CURRENT_DATE
--and tx.reservation_date >= date('2024-01-01') and tx.reservation_date < date('2024-01-31') 
and tx.parent_channel = 'API' ----------------------------------------------------------------- Parent Channel
--and tx.channel = 'expedia'
--and tx.transaction_code = '396667285500'      ----------------
group by tx.transaction_code, pr.product_id, pr.is_latam_destination
--order by fecha_reserva asc

--------------------------------------------------------------------------------------------------------------

---- HTML

select 
 --   tx.transaction_code as transaction_code,
--    pr.product_id AS product_id,
    pr.is_latam_destination,
    max(tx.site) as pais,
    max(pr.trip_type) as Viaje,
    max(tx.reservation_date) as fecha_reserva,    
    max(pr.checkin_date) as fecha_llegada,
    max(pr.checkout_date) as fecha_salida,
    max(pr.product_cancel_date) as fecha_cancelacion_directa,
    max(fv.confirmation_date) as fecha_confirmacion,
    max(fv.cancellation_end_date) as fecha_cancelacion,
    max(fv.payment_type) as tipo_pago,
    max(fv.cost) as cost,
    max(tx.purchase_type) as producto_original,
    max(pr.product_type) as producto,
    max(pr.hotel_name) as Hotel,
    max(pr.hotel_penalty_date) as fecha_penalidad,
    max(pr.hotel_chain_name) as Cadena,
    max(pr.anticipation) as Anticipacion,
    max(pr.destination_city) as Destino,
    max(ct.continent) as ContinentDestino,
    max(pr.destination_country_code) as CodPaisDestino,
    max(case when (tx.site = 'Colombia' and pr.status = 'Activo' and pr.payment_type = 'Prepago') then ca.total 
             when (tx.channel like '%hoteldo%') then ca.total    
             else (ca.gross_booking-ca.perceptions-tax_pais) end) as gb_usd, 
    ROUND(max(fv.gestion_gb), 2) AS gb_s_gradiente,
    ROUND(max(fv.gestion_gb * fv.confirmation_gradient), 2) AS gb_cgx,       
    max(tx.channel) as channel,
    max(tx.parent_channel) as parent_channel,
    max(sp.estado_producto) as estado_producto,
    max(tx.partner_data_id) as partner_id,
    max(tx.line_of_business) as lob,
    max(tx.brand) as brand,
    max(pr.gateway) as gateway,
    max(pr.provider_code) as provider,
    max(pr.hotel_despegar_id) as hotelid,
    max(pr.effective_rate) as tarifaefectiva,
    max(pr.is_refundable) as is_refundable,
    max(pr.total_passengers_quantity) as cantidad_pasajeros,
    max(ca.utility) as utilidad,
    max(ca.total) as total_gb,
    max(ca.fee) as fee_bruto,
    max(ca.commission) as commission_bruta,
    max(ca.discount) as descuento_bruto,
 --   max(fv.recognition_date) as recognition_date,
    max(partner.name) as name_partner,
    COUNT(DISTINCT tx.transaction_code) AS orders,
    sum(b.fee_net_usd) as fee_neto,
    sum(b.commission_net_usd) as comision_neta,
    sum(-b.discounts_net_usd) as descuentos_neto,
    sum(-b.affiliates_usd) as affiliates_usd,
    sum(b.coi_interest_usd) as coi_interest_usd,
    sum(-b.coi_usd) as coi_usd,
    sum(-b.ccp_usd) as ccp_usd,
    sum(b.other_incentives_air_usd) as other_incentives_air_usd,
    sum(b.errors_usd) as errors_usd,
    sum(b.frauds_usd) as frauds_usd,
    sum(b.revenue_taxes_usd) as revenue_taxes_usd,
    sum(b.ott_usd) as ott_usd,
    sum(b.backend_air_usd) as backend_air_usd,
    sum(b.backend_non_air_usd) as backend_non_air_usd,
    sum(b.bad_debt_usd) as bad_debt_usd,
    sum(b.breakage_revenue_usd) breakage_revenue_usd,
    sum(b.agency_backend_usd)as agency_backend_usd ,
    sum(b.customer_claims_usd) as customer_claims_usd,
    sum(b.other_incentives_non_air_usd) as other_incentives_non_air_usd,
    sum(b.customer_service_usd) as customer_service_usd,
    sum(-b.cancellations_usd) as cancellations_usd,
    sum(b.margin_net_usd) as margin_net_usd,
    sum(b.margin_variable_net_usd) as margen_var_neto,
    sum(b.net_revenues_usd) as net_revenue_usd,
    sum(b.npv_net_usd) as npv_net_usd,
    sum(b.fee_dynamic_usd) as fee_dynamic_usd,
    sum(cast(b.b2b_gradient_gb as double)) as b2b_gradient_gb,
    sum(cast(b.b2b_gradient_margin as double)) as b2b_gradient_margin,
    sum(b.financial_result_usd) as financial_result_usd,
    sum(b.gb_without_distorted_taxes_usd) as gb_without_distorted_taxes_usd,
    sum(b.discounts_mkt_funds_usd) as discounts_mkt_funds_usd,
    sum(b.media_revenue_usd) as media_revenue_usd,
    sum(b.loyalty_usd) as loyalty_usd,
    sum(b.fee_income_mkt_cmr_usd) as fee_income_mkt_cmr_usd,
    sum(b.mkt_fee_cost_cmr_usd) as mkt_fee_cost_cmr_usd,
    sum(b.variable_charges_without_mkt_usd) as variable_charges_without_mkt_usd,
    sum(b.dif_fx_usd) as dif_fx_usd,
    sum(b.dif_fx_air_usd) as dif_fx_air_usd,
    sum(b.currency_hedge_usd) as currency_hedge_usd,
    sum(b.currency_hedge_air_usd) as currency_hedge_air_usd,
--    max(t.rule_id) as rule_id,
--    max(t.rule_name) as rule_name,
 --   max(t.closed_percentage_fee) as closed_percentage_fee,
    max(fv.product_is_confirmed_flg) as is_confirmed_flg_recognition,
    max(fv.product_status) as product_status_recognition
from data.analytics.bi_transactional_fact_products pr 
join data.analytics.bi_transactional_fact_transactions tx on tx.transaction_code = pr.transaction_code and tx.reservation_year_month > date('2023-12-31')
left join data.analytics.bi_transactional_fact_charges ca on pr.product_id = ca.product_id and ca.reservation_year_month > date('2023-12-31')
LEFT JOIN (
    SELECT DISTINCT country_code, continent
    FROM data.analytics.mkt_users_dim_cities
) ct 
    ON ct.country_code = pr.destination_country_code
left join (
    select 
        producto.product_id,
        coalesce(bi_transactional_fact_products_current_state.product_state, producto.status) as estado_producto
    from analytics.bi_transactional_fact_products as producto  
    left join analytics.bi_transactional_fact_products_current_state  
        on producto.product_id = bi_transactional_fact_products_current_state.product_id
    where producto.reservation_year_month > date('2023-12-31')
) sp on pr.product_id = sp.product_id
left join data.analytics.bi_sales_fact_sales_recognition fv on fv.product_id = pr.product_id and fv.partition_period > '2023-12'
left join data.lake.ch_bo_partner_partner partner on partner.partner_code = tx.partner_data_id
left join data.analytics.bi_pnlop_fact_current_model b on b.product_id = pr.product_id and b.date_reservation_year_month > '2023-12-31'
where pr.reservation_year_month > date('2023-12-31') and pr.reservation_year_month < CURRENT_DATE
and tx.reservation_date > date('2023-12-31') and tx.reservation_date < CURRENT_DATE
and tx.parent_channel = 'Agencias afiliadas' ----------------------------------------------------------------- Parent Channel
--and tx.transaction_code = '396667285500'      ----------------
group by tx.transaction_code, pr.product_id, pr.is_latam_destination
--order by fecha_reserva asc





select *
from data.analytics.bi_transactional_fact_products pr 
where pr.reservation_year_month >= date('2024-01-01') and pr.reservation_year_month < CURRENT_DATE
and product_type = 'Hoteles'
limit 100

hotel_despegar_id,
hotel_chain_name,
hotel_chain_brand_name,
hotel_type,
destination_city,
destination_country_code,
destination_hotel_market,
is_latam_destination_flg


WITH hotel_data AS (
  SELECT
    hotel_despegar_id,
    hotel_chain_name,
    hotel_chain_brand_name,
    hotel_type,
    destination_city,
    destination_country_code,
    destination_hotel_market,
    is_latam_destination_flg,
    ROW_NUMBER() OVER (PARTITION BY hotel_despegar_id ORDER BY hotel_despegar_id) AS rn
  FROM data.analytics.bi_transactional_fact_products
  WHERE reservation_year_month >= DATE '2024-01-01'
    AND reservation_year_month < CURRENT_DATE
    AND product_type = 'Hoteles'
)
SELECT
  hotel_despegar_id,
  hotel_chain_name,
  hotel_chain_brand_name,
  hotel_type,
  destination_city,
  destination_country_code,
  destination_hotel_market,
  is_latam_destination_flg
FROM hotel_data
WHERE rn = 1
--LIMIT 100;








------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------

 --- Power BI "Analysis nube" (tabla "Data") - Yas --- anterior

with bt_detail as (
select 
  --  count(distinct(tx.transaction_code)) as transaction_codes,
   -- pr.product_id AS product_id,
   max(tx.site) as pais,
 /*   max(case 
                        when tx.partner_data_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') then 'PY'
                        when tx.partner_data_id in ('P12212', 'AP11666') then 'CR_CTA'
                        when tx.partner_data_id = 'AP12147' then 'SV_CTA'
                        when tx.partner_data_id = 'AP12854' then 'SV_CTA'
                        when tx.partner_data_id in ('AP12509', 'AP11813') then 'GT_CTA'
                        when tx.partner_data_id = 'AP12158' then 'PA_CTA'
                        when tx.partner_data_id in ('AP12213', 'AP11843') then 'HN_CTA'
                        when tx.partner_data_id in ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') then 'DO_CTA'
                        else tx.country_code 
                        end) as pais_corregido,
        MAX(
        CASE 
            WHEN tx.partner_data_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'PY'
            WHEN tx.partner_data_id IN ('P12212', 'AP11666', 'AP12147', 'AP12854', 'AP12509', 'AP11813', 'AP12158', 'AP12213', 'AP11843', 
                                        'AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'CTA'
            WHEN tx.country_code = 'MX' THEN 'Mexico'
            WHEN tx.country_code = 'BR' THEN 'Brasil'
          	WHEN tx.country_code = 'CO' THEN 'Colombia'
    		WHEN tx.country_code = 'AR' THEN 'Argentina'
   		    when tx.country_code = 'EC' THEN 'Ecuador'
   			WHEN tx.country_code = 'PE' THEN 'Peru'
    		WHEN tx.country_code = 'CL' THEN 'Chile'
    		WHEN tx.country_code IN ('US', 'PA', 'ES', 'CR') THEN 'Globales'
    		WHEN tx.country_code = 'UY' THEN 'UY - BO'
   			 WHEN tx.country_code = 'BO' THEN 'UY - BO'
   			 WHEN tx.country_code = 'PY' THEN 'PY'
    ELSE 'Others'
    --        ELSE tx.site 
        END
    ) AS country_corregido, */
    max(pr.trip_type) as Viaje,
    max(tx.reservation_date) as fecha_reserva,   
    max(fv.gestion_date) as gestion_date,
 --   max(fv.booking_date) as booking_date,
   -- max(pr.checkin_date) as fecha_llegada,
    --max(pr.checkout_date) as fecha_salida,
    max(tx.purchase_type) as producto_original,
  --  max(pr.product_type) as producto,
    max(pr.hotel_name) as Hotel,
    max(pr.hotel_chain_name) as Cadena,
    max(pr.destination_city) as Destino,
    max(case when (tx.site = 'Colombia' and pr.status = 'Activo' and pr.payment_type = 'Prepago') then ca.total 
             when (tx.channel like '%hoteldo%') then ca.total    
             else (ca.gross_booking-ca.perceptions-tax_pais) end) as gb_usd,    
    sum(fv.gestion_gb * fv.confirmation_gradient) as gb_gestion_gc, 
    sum(b.gb_without_distorted_taxes_usd) as gb_without_distorted_taxes_usd,
    sum(ca.gross_booking_local_currency) AS gb_local,
    MAX(ca.currency_code) AS moneda_local,
    sum(cast(b.b2b_gradient_gb as double)) as b2b_gradient_gb,
    sum(b.net_revenues_usd) as net_revenue_usd,
    sum(case when fv.country_code = 'BR' and fv.product not in ('Vuelos')
                           		then (b.net_revenues_usd-b.affiliates_usd)
                           else b.net_revenues_usd
                       end) as fix_net_revenues,
             max(tx.channel) as channel,
    max(tx.parent_channel) as parent_channel,
    max(sp.estado_producto) as estado_producto,
    max(tx.partner_data_id) as partner_id,
    max(tx.line_of_business) as lob,
    max(tx.brand) as brand,
    max(pr.gateway) as gateway,
    max(pr.provider_code) as provider,
    max(pr.hotel_despegar_id) as hotelid,
   max(pr.effective_rate) as tarifaefectiva,
  --  max(pr.is_refundable) as is_refundable,
   -- max(pr.total_passengers_quantity) as cantidad_pasajeros,
    max(ca.utility) as utilidad,
    max(ca.fee) as fee_bruto,
    max(ca.commission) as commission_bruta,
    max(ca.discount) as descuento_bruto,
  --  max(fv.recognition_date) as recognition_date,
   max(partner.name) as name_partner,
    sum(b.fee_net_usd) as fee_neto,
    sum(b.commission_net_usd) as comision_neta,
    sum(-b.discounts_net_usd) as descuentos_neto,
    sum(-b.affiliates_usd) as affiliates_usd,
    sum(b.coi_interest_usd) as coi_interest_usd,
    sum(-b.coi_usd) as coi_usd,
    sum(-b.ccp_usd) as ccp_usd,
    sum(b.other_incentives_air_usd) as other_incentives_air_usd,
    sum(b.errors_usd) as errors_usd,
    sum(b.frauds_usd) as frauds_usd,
    sum(b.revenue_taxes_usd) as revenue_taxes_usd,
    sum(b.ott_usd) as ott_usd,
    sum(b.backend_air_usd) as backend_air_usd,
    sum(b.backend_non_air_usd) as backend_non_air_usd,
    sum(b.bad_debt_usd) as bad_debt_usd,
    sum(b.breakage_revenue_usd) breakage_revenue_usd,
    sum(b.agency_backend_usd)as agency_backend_usd ,
    sum(b.customer_claims_usd) as customer_claims_usd,
    sum(b.other_incentives_non_air_usd) as other_incentives_non_air_usd,
    sum(b.customer_service_usd) as customer_service_usd,
    sum(-b.cancellations_usd) as cancellations_usd,
    sum(b.margin_net_usd) as margin_net_usd,
    sum(b.margin_variable_net_usd) as margen_var_neto,
    sum(b.npv_net_usd) as npv_net_usd,
    sum(b.fee_dynamic_usd) as fee_dynamic_usd,
    sum(cast(b.b2b_gradient_margin as double)) as b2b_gradient_margin,
    sum(b.financial_result_usd) as financial_result_usd,
    sum(b.discounts_mkt_funds_usd) as discounts_mkt_funds_usd,
    sum(b.media_revenue_usd) as media_revenue_usd,
    sum(b.loyalty_usd) as loyalty_usd,
    sum(b.fee_income_mkt_cmr_usd) as fee_income_mkt_cmr_usd,
    sum(b.mkt_fee_cost_cmr_usd) as mkt_fee_cost_cmr_usd,
    sum(b.variable_charges_without_mkt_usd) as variable_charges_without_mkt_usd,
    sum(b.dif_fx_usd) as dif_fx_usd,
    sum(b.dif_fx_air_usd) as dif_fx_air_usd,
    sum(b.currency_hedge_usd) as currency_hedge_usd,
    sum(b.currency_hedge_air_usd) as currency_hedge_air_usd ---,
  --  max(t.rule_id) as rule_id,
  --  max(t.rule_name) as rule_name,
  --  max(t.closed_percentage_fee) as closed_percentage_fee
from data.analytics.bi_transactional_fact_products pr 
join data.analytics.bi_transactional_fact_transactions tx on tx.transaction_code = pr.transaction_code and tx.reservation_year_month >= date('2023-01-01')
left join data.analytics.bi_transactional_fact_charges ca on pr.product_id = ca.product_id and ca.reservation_year_month >= date('2023-01-01')
left join (
    select 
        producto.product_id,
        coalesce(bi_transactional_fact_products_current_state.product_state, producto.status) as estado_producto
    from analytics.bi_transactional_fact_products as producto  
    left join analytics.bi_transactional_fact_products_current_state  
        on producto.product_id = bi_transactional_fact_products_current_state.product_id
    where producto.reservation_year_month >= date('2023-01-01')
) sp on pr.product_id = sp.product_id
left join data.analytics.bi_sales_fact_sales_recognition fv on fv.product_id = pr.product_id and fv.partition_period >= '2023-01'
left join data.lake.ch_bo_partner_partner partner on partner.partner_code = tx.partner_data_id
left join data.analytics.bi_pnlop_fact_current_model b on b.product_id = pr.product_id and b.date_reservation_year_month >= '2023-01-01'
left join (
    select distinct 
        x.transaction_code,
        rm.rule_id,
        rm.rule_name,
        rm.closed_percentage_fee
    from data.analytics.bi_transactional_fact_transactions x
    inner join data.lake.chewie_reservation r on x.transaction_code = r.id 
    inner join data.lake.chewie_product p on r.oid = p.reservation_id
    inner join data.lake.chewie_product_revenue_input_margin rm on p.oid = rm.product_id 
    where x.reservation_year_month >= date('2024-01-01')
) t on tx.transaction_code = t.transaction_code
where pr.reservation_year_month >= date('2024-01-01')
--and tx.site = 'Brasil'
---and tx.reservation_date >= date('2024-10-01')
and (tx.line_of_business in ('B2B') )
--and (tx.parent_channel = 'API')
group by tx.transaction_code, pr.product_id)
select *
FROM bt_detail 
limit 100
-- partner_id in ('AP12907', 'AP12908', 'AP12910') 
--and fecha_reserva = date('2024-12-05')
--and hotelid = '1191624'
--group by country_corregido, pais_corregido, Hotel, fecha_reserva, parent_channel
--order by gb_gestion_gc desc
--and hotelid = '980781'
--limit 100



-----------------

left join data.analytics.bi_transactional_fact_charges ca on pr.product_id = ca.product_id and ca.reservation_year_month >= date('2023-01-01')
    MAX(ca.gross_booking_local_currency) AS gb_local,
    MAX(ca.currency_code) AS moneda_local,}
 
    
    ----
    
    -- Query: total gb_cgx for April 2025 --Validar con GPT para ver suma total de GBs 2025-05-05
-- Crea un dataset base idéntico a la consulta original pero filtrado a Abril 2025 y luego suma gb_cgx

WITH base AS (
    SELECT 
        tx.transaction_code AS transaction_code,
        pr.product_id AS product_iepaisd,
        MAX(tx.site) AS pais,
        MAX(pr.trip_type) AS Viaje,
        MAX(tx.reservation_date) AS fecha_reserva,
        MAX(pr.checkin_date) AS fecha_llegada,
        MAX(pr.checkout_date) AS fecha_salida,
        MAX(pr.product_cancel_date) AS fecha_cancelacion_directa,
        MAX(fv.confirmation_date) AS fecha_confirmacion,
        MAX(fv.cancellation_end_date) AS fecha_cancelacion,
        MAX(fv.payment_type) AS tipo_pago,
        MAX(fv.cost) AS cost,
        MAX(tx.purchase_type) AS producto_original,
        MAX(pr.product_type) AS producto,
        MAX(pr.hotel_name) AS Hotel,
        MAX(pr.hotel_penalty_date) AS fecha_penalidad,
        MAX(pr.hotel_chain_name) AS Cadena,
        MAX(pr.anticipation) AS Anticipacion,
        MAX(pr.destination_city) AS Destino,
        MAX(ct.continent) AS ContinentDestino,
        MAX(pr.destination_country_code) AS CodPaisDestino,
        MAX(CASE 
                WHEN (tx.site = 'Colombia' AND pr.status = 'Activo' AND pr.payment_type = 'Prepago') THEN ca.total 
                WHEN (tx.channel LIKE '%hoteldo%') THEN ca.total    
                ELSE (ca.gross_booking - ca.perceptions - tax_pais) 
            END) AS gb_usd,
        ROUND(MAX(fv.gestion_gb), 2) AS gb_s_gradiente,
        ROUND(MAX(fv.gestion_gb * fv.confirmation_gradient), 2) AS gb_cgx,
        MAX(tx.channel) AS channel,
        MAX(tx.parent_channel) AS parent_channel,
        MAX(sp.estado_producto) AS estado_producto,
        MAX(tx.partner_data_id) AS partner_id,
        MAX(tx.line_of_business) AS lob,
        MAX(tx.brand) AS brand,
        MAX(pr.gateway) AS gateway,
        MAX(pr.provider_code) AS provider,
        MAX(pr.hotel_despegar_id) AS hotelid,
        MAX(pr.effective_rate) AS tarifaefectiva,
        MAX(pr.is_refundable) AS is_refundable,
        MAX(pr.total_passengers_quantity) AS cantidad_pasajeros,
        MAX(ca.utility) AS utilidad,
        MAX(ca.total) AS total_gb,
        MAX(ca.fee) AS fee_bruto,
        MAX(ca.commission) AS commission_bruta,
        MAX(ca.discount) AS descuento_bruto,
        MAX(fv.recognition_date) AS recognition_date,
        MAX(partner.name) AS name_partner,
        SUM(b.fee_net_usd) AS fee_neto,
        SUM(b.commission_net_usd) AS comision_neta,
        SUM(-b.discounts_net_usd) AS descuentos_neto,
        SUM(-b.affiliates_usd) AS affiliates_usd,
        SUM(b.coi_interest_usd) AS coi_interest_usd,
        SUM(-b.coi_usd) AS coi_usd,
        SUM(-b.ccp_usd) AS ccp_usd,
        SUM(b.other_incentives_air_usd) AS other_incentives_air_usd,
        SUM(b.errors_usd) AS errors_usd,
        SUM(b.frauds_usd) AS frauds_usd,
        SUM(b.revenue_taxes_usd) AS revenue_taxes_usd,
        SUM(b.ott_usd) AS ott_usd,
        SUM(b.backend_air_usd) AS backend_air_usd,
        SUM(b.backend_non_air_usd) AS backend_non_air_usd,
        SUM(b.bad_debt_usd) AS bad_debt_usd,
        SUM(b.breakage_revenue_usd) AS breakage_revenue_usd,
        SUM(b.agency_backend_usd) AS agency_backend_usd ,
        SUM(b.customer_claims_usd) AS customer_claims_usd,
        SUM(b.other_incentives_non_air_usd) AS other_incentives_non_air_usd,
        SUM(b.customer_service_usd) AS customer_service_usd,
        SUM(-b.cancellations_usd) AS cancellations_usd,
        SUM(b.margin_net_usd) AS margin_net_usd,
        SUM(b.margin_variable_net_usd) AS margen_var_neto,
        SUM(b.net_revenues_usd) AS net_revenue_usd,
        SUM(b.npv_net_usd) AS npv_net_usd,
        SUM(b.fee_dynamic_usd) AS fee_dynamic_usd,
        SUM(CAST(b.b2b_gradient_gb AS DOUBLE)) AS b2b_gradient_gb,
        SUM(CAST(b.b2b_gradient_margin AS DOUBLE)) AS b2b_gradient_margin,
        SUM(b.financial_result_usd) AS financial_result_usd,
        SUM(b.gb_without_distorted_taxes_usd) AS gb_without_distorted_taxes_usd,
        SUM(b.discounts_mkt_funds_usd) AS discounts_mkt_funds_usd,
        SUM(b.media_revenue_usd) AS media_revenue_usd,
        SUM(b.loyalty_usd) AS loyalty_usd,
        SUM(b.fee_income_mkt_cmr_usd) AS fee_income_mkt_cmr_usd,
        SUM(b.mkt_fee_cost_cmr_usd) AS mkt_fee_cost_cmr_usd,
        SUM(b.variable_charges_without_mkt_usd) AS variable_charges_without_mkt_usd,
        SUM(b.dif_fx_usd) AS dif_fx_usd,
        SUM(b.dif_fx_air_usd) AS dif_fx_air_usd,
        SUM(b.currency_hedge_usd) AS currency_hedge_usd,
        SUM(b.currency_hedge_air_usd) AS currency_hedge_air_usd,
        MAX(t.rule_id) AS rule_id,
        MAX(t.rule_name) AS rule_name,
        MAX(t.closed_percentage_fee) AS closed_percentage_fee,
        MAX(fv.product_is_confirmed_flg) AS is_confirmed_flg_recognition,
        MAX(fv.product_status) AS product_status_recognition
    FROM data.analytics.bi_transactional_fact_products pr 
    JOIN data.analytics.bi_transactional_fact_transactions tx 
        ON tx.transaction_code = pr.transaction_code
        AND tx.reservation_year_month >= DATE('2024-01-01')
    LEFT JOIN data.analytics.bi_transactional_fact_charges ca 
        ON pr.product_id = ca.product_id 
        AND ca.reservation_year_month >= DATE('2024-01-01')
    LEFT JOIN (
        SELECT DISTINCT country_code, continent
        FROM data.analytics.mkt_users_dim_cities
    ) ct ON ct.country_code = pr.destination_country_code
    LEFT JOIN (
        SELECT 
            producto.product_id,
            COALESCE(bi_transactional_fact_products_current_state.product_state, producto.status) AS estado_producto
        FROM analytics.bi_transactional_fact_products AS producto  
        LEFT JOIN analytics.bi_transactional_fact_products_current_state  
            ON producto.product_id = bi_transactional_fact_products_current_state.product_id
        WHERE producto.reservation_year_month >= DATE('2024-01-01')
    ) sp ON pr.product_id = sp.product_id
    LEFT JOIN data.analytics.bi_sales_fact_sales_recognition fv 
        ON fv.product_id = pr.product_id 
        AND fv.partition_period >= '2024-01'
    LEFT JOIN data.lake.ch_bo_partner_partner partner 
        ON partner.partner_code = tx.partner_data_id
    LEFT JOIN data.analytics.bi_pnlop_fact_current_model b 
        ON b.product_id = pr.product_id 
        AND b.date_reservation_year_month >= '2024-01-01'
    LEFT JOIN (
        SELECT DISTINCT 
            x.transaction_code,
            rm.rule_id,
            rm.rule_name,
            rm.closed_percentage_fee
        FROM data.analytics.bi_transactional_fact_transactions x
        INNER JOIN data.lake.chewie_reservation r ON x.transaction_code = r.id 
        INNER JOIN data.lake.chewie_product p ON r.oid = p.reservation_id
        INNER JOIN data.lake.chewie_product_revenue_input_margin rm ON p.oid = rm.product_id 
        WHERE x.reservation_year_month >= DATE('2024-01-01')
    ) t ON tx.transaction_code = t.transaction_code
    WHERE 
        -- Filtro específico para Abril 2025
        pr.reservation_year_month >= DATE('2025-04-01')
        AND pr.reservation_year_month <  DATE('2025-05-01')
        AND tx.reservation_date >= DATE('2025-04-01')
        AND tx.reservation_date <  DATE('2025-05-01')
        AND tx.parent_channel = 'API'
    GROUP BY 
        tx.transaction_code, 
        pr.product_id
)
SELECT 
    ROUND(SUM(gb_cgx),2) AS total_gb_cgx,
    ROUND(SUM(gb_usd),2) as total_gb_usd
FROM base;







------------------

--- CTRIP



WITH sub_gb_sin_gc AS (
    -- Suma de GB sin GC por fecha, país destino, hotel y cadena
    SELECT
        CAST(tx.reservation_date AS DATE) AS fecha_reserva,
        tx.site AS pais_destino,
        pr.hotel_name AS Hotel,
        pr.hotel_chain_name AS Cadena,
        tx.partner_data_id AS partner_id,
        tx.brand AS brand,
        pr.destination_city AS Destino,
        pr.hotel_despegar_id AS hotelid,
        partner.name AS name_partner,
        SUM(
            -- GB sin GC: mismo cálculo que gb_usd del main query
            CASE
              WHEN tx.site = 'Colombia'
                   AND pr.status = 'Activo'
                   AND pr.payment_type = 'Prepago'
                THEN ca.total 
              WHEN tx.channel LIKE '%hoteldo%' 
                THEN ca.total    
              ELSE (ca.gross_booking - ca.perceptions - tax_pais)
            END
        ) AS gb_sin_gc
    FROM data.analytics.bi_transactional_fact_products pr
    JOIN data.analytics.bi_transactional_fact_transactions tx
      ON tx.transaction_code = pr.transaction_code
    LEFT JOIN data.analytics.bi_transactional_fact_charges ca
      ON pr.product_id = ca.product_id
         AND ca.reservation_year_month >= DATE('2024-01-01')
    LEFT JOIN data.lake.ch_bo_partner_partner partner
      ON partner.partner_code = tx.partner_data_id
    WHERE
      -- 1er trimestre 2024 ó 1er trimestre 2025
      (
        tx.reservation_date BETWEEN DATE('2024-01-01') AND DATE('2024-03-31')
        OR
        tx.reservation_date BETWEEN DATE('2025-01-01') AND DATE('2025-03-31')
      )
      AND tx.partner_data_id IN ('AP11615', 'AP11682', 'AP11683')
    GROUP BY
        CAST(tx.reservation_date AS DATE),
        tx.site,
        pr.hotel_name,
        pr.hotel_chain_name,
        tx.partner_data_id,
        tx.brand,
        pr.destination_city,
        pr.hotel_despegar_id,
        partner.name
)
-- Para usarla, simplemente haz JOIN con tu query principal:
SELECT
    m.*,
    s.gb_sin_gc
FROM (
    -- aquí va tu query principal
) AS m
LEFT JOIN sub_gb_sin_gc AS s
 ON m.partner_id   = s.partner_id
 AND m.hotelid      = s.hotelid
 AND m.fecha_reserva = s.fecha_reserva;




WITH sub_gb_sin_gc AS (
    SELECT
        CAST(tx.reservation_date AS DATE) AS fecha_reserva,
        tx.site AS pais_destino,
        pr.hotel_name AS Hotel,
        pr.hotel_chain_name AS Cadena,
        tx.partner_data_id AS partner_id,
        tx.brand AS brand,
        pr.destination_city AS Destino,
        pr.hotel_despegar_id AS hotelid,
        partner.name AS name_partner,
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
        ) AS gb_sin_gc
    FROM data.analytics.bi_transactional_fact_products pr
    JOIN data.analytics.bi_transactional_fact_transactions tx
      ON tx.transaction_code = pr.transaction_code
    LEFT JOIN data.analytics.bi_transactional_fact_charges ca
      ON pr.product_id = ca.product_id
    LEFT JOIN data.lake.ch_bo_partner_partner partner
      ON partner.partner_code = tx.partner_data_id
    WHERE
      (
        tx.reservation_date BETWEEN DATE('2024-01-01') AND DATE('2024-03-31')
        OR
        tx.reservation_date BETWEEN DATE('2025-01-01') AND DATE('2025-03-31')
      )
      AND tx.partner_data_id IN ('AP11615', 'AP11682', 'AP11683')
    GROUP BY
        CAST(tx.reservation_date AS DATE),
        tx.site,
        pr.hotel_name,
        pr.hotel_chain_name,
        tx.partner_data_id,
        tx.brand,
        pr.destination_city,
        pr.hotel_despegar_id,
        partner.name
)
SELECT * 
FROM sub_gb_sin_gc;




WITH sub_gb_sin_gc AS (
    SELECT
        CAST(tx.reservation_date AS DATE) AS fecha_reserva,
        CONCAT(
          CAST(year(tx.reservation_date)      AS varchar),
          '-',
          lpad(CAST(quarter(tx.reservation_date) AS varchar), 2, '0')
        ) AS Year_Quarter,
        pr.destination_country_code AS pais_destino,
        pr.hotel_name            AS Hotel,
        pr.hotel_chain_name      AS Cadena,
        tx.partner_data_id       AS partner_id,
        tx.brand                 AS brand,
        pr.destination_city      AS Destino,
        pr.hotel_despegar_id     AS hotelid,
        partner.name             AS name_partner,
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
        ) AS gb_sin_gc
    FROM data.analytics.bi_transactional_fact_products pr
    JOIN data.analytics.bi_transactional_fact_transactions tx
      ON tx.transaction_code = pr.transaction_code
    LEFT JOIN data.analytics.bi_transactional_fact_charges ca
      ON pr.product_id = ca.product_id
         AND ca.reservation_year_month >= DATE('2024-01-01')
    LEFT JOIN data.lake.ch_bo_partner_partner partner
      ON partner.partner_code = tx.partner_data_id
    WHERE
      pr.reservation_year_month   >= DATE('2024-01-01')
      AND tx.reservation_year_month >= DATE('2024-01-01')
      AND (
        tx.reservation_date BETWEEN DATE('2024-01-01') AND DATE('2024-03-31')
        OR
        tx.reservation_date BETWEEN DATE('2025-01-01') AND DATE('2025-03-31')
      )
      AND tx.partner_data_id IN ('AP11615', 'AP11682', 'AP11683')
    GROUP BY
        CAST(tx.reservation_date AS DATE),
        CONCAT(
          CAST(year(tx.reservation_date)      AS varchar),
          '-',
          lpad(CAST(quarter(tx.reservation_date) AS varchar), 2, '0')
        ),
        pr.destination_country_code,
        pr.hotel_name,
        pr.hotel_chain_name,
        tx.partner_data_id,
        tx.brand,
        pr.destination_city,
        pr.hotel_despegar_id,
        partner.name
)
SELECT *
FROM sub_gb_sin_gc

