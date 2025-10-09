
------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------

 --- Power BI "Analysis nube" (tabla "Data") - Yas --- ANITFRAUDE

with bt_detail as (
select 
    count(distinct(tx.transaction_code)) as transaction_codes,
   -- pr.product_id AS product_id,
  --  max(tx.site) as pais,
    max(case 
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
    ) AS country_corregido,
   /* CASE
    WHEN fv.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'Others'
    WHEN fv.partner_id IN ('P12212', 'AP11666') THEN 'Others'
    WHEN fv.partner_id IN ('AP12147', 'AP12854') THEN 'Others'
    WHEN fv.partner_id IN ('AP12509', 'AP11813') THEN 'Others'
    WHEN fv.partner_id = 'AP12158' THEN 'Others'
    WHEN fv.partner_id IN ('AP12213', 'AP11843') THEN 'Others'
    WHEN fv.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'Others'
    WHEN fv.country_code = 'MX' THEN 'Mexico'
    WHEN fv.country_code = 'BR' THEN 'Brasil'
    WHEN fv.country_code = 'CO' THEN 'Colombia'
    WHEN fv.country_code = 'AR' THEN 'Argentina'
    WHEN fv.country_code = 'EC' THEN 'Ecuador'
    WHEN fv.country_code = 'PE' THEN 'Peru'
    WHEN fv.country_code = 'CL' THEN 'Chile'
    WHEN fv.country_code IN ('US', 'PA', 'ES', 'CR') THEN 'USA/ROW'
    WHEN fv.country_code = 'UY' THEN 'Others'
    WHEN fv.country_code = 'BO' THEN 'Others'
    ELSE 'Others'
END AS country_metas */
   -- max(pr.trip_type) as Viaje,
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
  --  max(pr.effective_rate) as tarifaefectiva,
  --  max(pr.is_refundable) as is_refundable,
   -- max(pr.total_passengers_quantity) as cantidad_pasajeros,
    max(ca.utility) as utilidad,
    max(ca.fee) as fee_bruto,
    max(ca.commission) as commission_bruta,
    max(ca.discount) as descuento_bruto,
  --  max(fv.recognition_date) as recognition_date,
   -- max(partner.name) as name_partner,
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
    where x.reservation_year_month >= date('2023-01-01')
) t on tx.transaction_code = t.transaction_code
where pr.reservation_year_month >= date('2023-01-01')
--and tx.site = 'Brasil'
---and tx.reservation_date >= date('2024-10-01')
and (tx.line_of_business in ('B2B') )
--and (tx.parent_channel = 'API')
group by tx.transaction_code, pr.product_id)
select
pais_corregido,
country_corregido,
parent_channel,
Hotel,
fecha_reserva,
ROUND(sum(gb_gestion_gc),2) as gb_gestion_gc,
sum(transaction_codes) as transacciones,
ROUND(sum(gb_usd),2) as gb
FROM bt_detail 
--where fecha_reserva = date('2024-12-05')
--and hotelid = '1191624'
--group by country_corregido, pais_corregido, Hotel, fecha_reserva, parent_channel
order by gb desc
limit 100