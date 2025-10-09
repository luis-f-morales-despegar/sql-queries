
with consulta as (
select 
    tx.transaction_code as transaction_code,
    pr.product_id AS product_id,
    max(tx.site) as pais,
    max(pr.trip_type) as Viaje,
    max(tx.reservation_date) as fecha_reserva,    
    max(pr.checkin_date) as fecha_llegada,
    max(pr.checkout_date) as fecha_salida,
    max(pr.product_cancel_date) as fecha_cancelacion_directa,
    max(fv.confirmation_date) as fecha_confirmacion,
    max(fv.cancellation_end_date) as fecha_cancelacion,
    max(fv.payment_type) as tipo_pago,
  --  max(fv.cost) as cost,
    max(tx.purchase_type) as producto_original,
    max(pr.product_type) as producto,
    max(pr.hotel_name) as Hotel,
    max(pr.hotel_penalty_date) as fecha_penalidad,
    max(pr.hotel_chain_name) as Cadena,
    max(pr.destination_city) as Destino,
   -- MAX(ca.commission_local_currency) as comision_COP,
   -- MAX(ca.gross_booking_local_currency - ca.commission_local_currency) as gb_COP_menos_comision_COP,
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
    max(pr.is_refundable) as es_reembolsable,
    max(pr.total_passengers_quantity) as cantidad_pasajeros,
  --  max(ca.utility) as utilidad,
  --  max(ca.fee) as fee_bruto,
  --  max(ca.commission) as commission_bruta,
 --   max(ca.discount) as descuento_bruto,
    max(fv.recognition_date) as recognition_date,
    max(partner.name) as name_partner,
  --  sum(b.fee_net_usd) as fee_neto,
  --  sum(b.commission_net_usd) as comision_neta,
  --  sum(-b.discounts_net_usd) as descuentos_neto,
  MAX(ca.gross_booking_local_currency) as gb_COP,
  MAX(ca.currency_code) as moneda_local,
        max(case when (tx.site = 'Colombia' and pr.status = 'Activo' and pr.payment_type = 'Prepago') then ca.total 
             when (tx.channel like '%hoteldo%') then ca.total    
             else (ca.gross_booking-ca.perceptions-tax_pais) end) as gb_USD,                
    sum(-b.affiliates_usd) as affiliates_usd,
    sum(b.revenue_sharing_usd) as revenue_sharing_usd,
    sum(-b.affiliates_usd+b.revenue_sharing_usd) as comision_agencia_usd,
  --  sum(b.coi_interest_usd) as coi_interest_usd,
  --  sum(-b.coi_usd) as coi_usd,
  --  sum(-b.ccp_usd) as ccp_usd,
  --  sum(b.other_incentives_air_usd) as other_incentives_air_usd,
  --  sum(b.errors_usd) as errors_usd,
  --  sum(b.frauds_usd) as frauds_usd,
  --  sum(b.revenue_taxes_usd) as revenue_taxes_usd,
  --  sum(b.ott_usd) as ott_usd,
  --  sum(b.backend_air_usd) as backend_air_usd,
  --  sum(b.backend_non_air_usd) as backend_non_air_usd,
  --  sum(b.bad_debt_usd) as bad_debt_usd,
  --  sum(b.breakage_revenue_usd) breakage_revenue_usd,
  --  sum(b.agency_backend_usd)as agency_backend_usd ,
  --  sum(b.customer_claims_usd) as customer_claims_usd,
  --  sum(b.other_incentives_non_air_usd) as other_incentives_non_air_usd,
  --  sum(b.customer_service_usd) as customer_service_usd,
 --   sum(-b.cancellations_usd) as cancellations_usd,
  --  sum(b.margin_net_usd) as margin_net_usd,
 --   sum(b.margin_variable_net_usd) as margen_var_neto,
 --   sum(b.net_revenues_usd) as net_revenue_usd,
--    sum(b.npv_net_usd) as npv_net_usd,
--    sum(b.fee_dynamic_usd) as fee_dynamic_usd,
 --   sum(cast(b.b2b_gradient_gb as double)) as b2b_gradient_gb,
 --   sum(cast(b.b2b_gradient_margin as double)) as b2b_gradient_margin,
  ---  sum(b.financial_result_usd) as financial_result_usd,
 ---   sum(b.gb_without_distorted_taxes_usd) as gb_without_distorted_taxes_usd,
  --  sum(b.discounts_mkt_funds_usd) as discounts_mkt_funds_usd,
  --  sum(b.media_revenue_usd) as media_revenue_usd,
  --  sum(b.loyalty_usd) as loyalty_usd,
   -- sum(b.fee_income_mkt_cmr_usd) as fee_income_mkt_cmr_usd,
   -- sum(b.mkt_fee_cost_cmr_usd) as mkt_fee_cost_cmr_usd,
   -- sum(b.variable_charges_without_mkt_usd) as variable_charges_without_mkt_usd,
  --  sum(b.dif_fx_usd) as dif_fx_usd,
  --  sum(b.dif_fx_air_usd) as dif_fx_air_usd,
  --  sum(b.currency_hedge_usd) as currency_hedge_usd,
  --  sum(b.currency_hedge_air_usd) as currency_hedge_air_usd,
  --  max(t.rule_id) as rule_id,
 --   max(t.rule_name) as rule_name,
 --   max(t.closed_percentage_fee) as closed_percentage_fee,
    max(fv.product_is_confirmed_flg) as is_confirmed_flg_recognition,
    max(fv.product_status) as product_status_recognition
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
    where producto.reservation_year_month >= date('2025-01-01')
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
    where x.reservation_year_month >= date('2025-01-01')  --- FLTROS FECHA ----
) t on tx.transaction_code = t.transaction_code
where pr.reservation_year_month >= date('2025-01-01')
and tx.reservation_date >= date('2025-01-20')
and tx.reservation_date <= date('2025-02-20')
and (
    (pr.is_refundable = TRUE AND pr.checkin_date <= DATE('2025-02-25'))
    OR 
    (pr.is_refundable = FALSE AND pr.checkin_date <= DATE('2025-12-31'))
)
and (tx.parent_channel = 'Agencias afiliadas')  ---- FILTRO CANAL
--and tx.purchase_type = 'Hoteles'                ---- FILTRO PRODUCTO
and tx.site = 'Colombia'                        ---- FILTRO PAIS
and sp.estado_producto = 'Confirmado'
--and ca.gross_booking_local_currency - ca.commission_local_currency >= 500000
group by tx.transaction_code, pr.product_id)
select *
from consulta
where gb_USD + comision_agencia_usd >= 122.71
order by fecha_reserva asc
--limit 100



limit 1000


----

select * 
from data.analytics.bi_transactional_fact_charges ca 
where reservation_year_month >= date('2025-01-01')
limit 100

ca.gross_booking_local_currency - ca.commission_local_currency >= 500000
ca.currency_code = 'COP'
ca.commission_local_curency

---

select * 
from data.analytics.bi_pnlop_fact_current_model b
where date_reservation_year_month > '2025-01-01'
limit 100




-



WITH consulta AS (
SELECT 
    tx.transaction_code AS transaction_code,
    pr.product_id AS product_id,
    MAX(tx.site) AS pais,
    MAX(pr.trip_type) AS Viaje,
    MAX(tx.reservation_date) AS fecha_reserva,    
    MAX(pr.checkin_date) AS fecha_llegada,
    MAX(pr.checkout_date) AS fecha_salida,
    MAX(pr.product_cancel_date) AS fecha_cancelacion_directa,
    MAX(fv.confirmation_date) AS fecha_confirmacion,
    MAX(fv.cancellation_end_date) AS fecha_cancelacion,
    MAX(fv.payment_type) AS tipo_pago,
    MAX(tx.purchase_type) AS producto_original,
    MAX(pr.product_type) AS producto,
    MAX(pr.hotel_name) AS Hotel,
    MAX(pr.hotel_penalty_date) AS fecha_penalidad,
    MAX(pr.hotel_chain_name) AS Cadena,
    MAX(pr.destination_city) AS Destino,
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
    MAX(pr.is_refundable) AS es_reembolsable,
    MAX(pr.total_passengers_quantity) AS cantidad_pasajeros,
    MAX(fv.recognition_date) AS recognition_date,
    MAX(partner.name) AS name_partner,
    MAX(ca.gross_booking_local_currency) AS gb_COP,
    MAX(ca.currency_code) AS moneda_local,
    MAX(CASE WHEN (tx.site = 'Colombia' AND pr.status = 'Activo' AND pr.payment_type = 'Prepago') THEN ca.total 
             WHEN (tx.channel LIKE '%hoteldo%') THEN ca.total    
             ELSE (ca.gross_booking - ca.perceptions - tax_pais) END) AS gb_USD,                
    SUM(-b.affiliates_usd) AS affiliates_usd,
    SUM(b.revenue_sharing_usd) AS revenue_sharing_usd,
    SUM(-b.affiliates_usd + b.revenue_sharing_usd) AS comision_agencia_usd,
    MAX(fv.product_is_confirmed_flg) AS is_confirmed_flg_recognition,
    MAX(fv.product_status) AS product_status_recognition
FROM data.analytics.bi_transactional_fact_products pr 
JOIN data.analytics.bi_transactional_fact_transactions tx 
    ON tx.transaction_code = pr.transaction_code AND tx.reservation_year_month >= DATE('2023-01-01')
LEFT JOIN data.analytics.bi_transactional_fact_charges ca 
    ON pr.product_id = ca.product_id AND ca.reservation_year_month >= DATE('2023-01-01')
LEFT JOIN (
    SELECT 
        producto.product_id,
        COALESCE(bi_transactional_fact_products_current_state.product_state, producto.status) AS estado_producto
    FROM analytics.bi_transactional_fact_products AS producto  
    LEFT JOIN analytics.bi_transactional_fact_products_current_state  
        ON producto.product_id = bi_transactional_fact_products_current_state.product_id
    WHERE producto.reservation_year_month >= DATE('2025-01-01')
) sp ON pr.product_id = sp.product_id
LEFT JOIN data.analytics.bi_sales_fact_sales_recognition fv 
    ON fv.product_id = pr.product_id AND fv.partition_period >= '2023-01'
LEFT JOIN data.lake.ch_bo_partner_partner partner 
    ON partner.partner_code = tx.partner_data_id
LEFT JOIN data.analytics.bi_pnlop_fact_current_model b 
    ON b.product_id = pr.product_id AND b.date_reservation_year_month >= '2023-01-01'
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
    WHERE x.reservation_year_month >= DATE('2025-01-01')
) t ON tx.transaction_code = t.transaction_code
WHERE pr.reservation_year_month >= DATE('2025-01-01')
AND tx.reservation_date >= DATE('2025-01-20')
AND tx.reservation_date <= DATE('2025-02-20')
AND (
    (pr.is_refundable = TRUE AND pr.checkin_date <= DATE('2025-02-25'))
    OR 
    (pr.is_refundable = FALSE AND pr.checkin_date <= DATE('2025-12-31'))
)
AND tx.parent_channel = 'Agencias afiliadas'
AND tx.site = 'Colombia'
AND sp.estado_producto = 'Confirmado'
GROUP BY tx.transaction_code, pr.product_id)
SELECT *
FROM consulta
WHERE gb_USD + comision_agencia_usd >= 122.71
ORDER BY fecha_reserva ASC;
