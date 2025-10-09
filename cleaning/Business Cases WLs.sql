---Query original Omar a Fran Business Cases WLs

select 
     t.reservation_date as fecha_reserva
	,t.confirmation_date as fecha_confirmacion
	,p.is_confirmed_flg
    ,date_format(t.confirmation_date, '%Y%m') as anio_mes
    ,year(t.confirmation_date) as anio
    ,week(t.confirmation_date) as semana
    ,b.transaction_code as trx_code
    ,b.product_id
    ,p.pnr
    ,p.payment_type
    ,b.brand
    ,p.provider_code
    ,p.provider_description
    ,p.origin_city
    ,p.origin_city_code
    ,p.origin_country_code
    ,p.destination_city
    ,p.destination_city_code
    ,p.arrival_airport_code
    ,p.destination_country_code
    ,p.hotel_chain_name
    ,p.hotel_name
    ,p.hotel_despegar_id
    ,p.hotel_chain_brand_name
    ,p.hotel_is_expedia_chain_flg
    ,p.hotel_rate_condition
    ,p.hotel_sale_promo
    ,p.hotel_contract_type
    ,p.flight_validatin_carrier
    ,p.flight_fare_type
    ,p.flight_type
    ,p.flight_fare_family
    ,p.flight_polcom_id
    ,p.total_passengers_quantity
    ,p.anticipation
    ,p.duration
    ,t.parity_code
    ,t.line_of_business  as lob
    ,if( if( (t.purchase_type = 'Vuelos' and p.product_type = 'Asistencia al viajero') or (t.purchase_type in ('Bundles', 'Escapadas', 'Carrito') and p.attach_stage = 'CHECKOUT' and p.product_type = 'Asistencia al viajero')
                , 'Asistencia al viajero'
                , t.purchase_type
            ) in ('Bundles', 'Escapadas')
        ,'Carrito'
        , if( (t.purchase_type = 'Vuelos' and p.product_type = 'Asistencia al viajero') or (t.purchase_type in ('Bundles', 'Escapadas', 'Carrito') and p.attach_stage = 'CHECKOUT' and p.product_type = 'Asistencia al viajero')
                ,'Asistencia al viajero'
                ,t.purchase_type
            )
        ) as productooriginal
    ,p.product_type
    ,if( t.country_code in ('AR','CO','EC','PE','BR','CL','MX')
        ,t.country_code
        ,'OT'
        ) as pais
    ,p.trip_type as viaje
    ,t.channel
    ,t.parent_channel
    ,t.partner_data_id as Partner
/***************************/
/*****    METRICAS     *****/
/***************************/
    ,sum(cast(C.total - C.tax_pais - C.tax_afip_rg4815 as decimal(18,2))) as gb_sin_fisco_usd
    ,sum(b.gb_without_distorted_taxes_usd) as gb_taxes_usd
    ,sum( if(t.country_code = 'AR' and p.trip_type = 'Int'
                ,C.total_local_currency - C.tax_pais_local_currency - C.tax_afip_rg4815_local_currency
                ,C.total_local_currency
            )
        ) as gb_ml
    ,sum(c.tax_remesas) as imp_remesas_usd
/*** Margen Neto - Tx Monetarios ***/
    ,sum(b.fee_usd) as fee
    ,sum(b.fee_dynamic_usd) fee_dinamico
    ,sum(b.fee_net_usd) as fee_neto
    ,sum(b.commission_usd) as comision
    ,sum(b.commission_net_usd) as comision_neta
    ----
    --,sum(costomonedalocal) costo_moneda_local
    ,sum(cost) costo_usd
    ,-sum(b.discounts_usd) as descuentos
    ,-sum(b.discounts_net_usd) as descuentos_neto
    ,-sum(b.discounts_commercial_usd)  as descto_comercial
    ,-sum(b.discounts_operational_usd)  as descto_operacional
    ,sum(C.flight_base_fare_local_currency) tarifa_base_moneda_local
    ,sum(C.flight_base_fare) tarifa_base_usd
    ,sum(C.flight_fare_local_currency) tarifa_moneda_local
    ,sum(C.flight_base_fare) tarifa_usd
    ,sum(C.taxes_local_currency) impuestos_moneda_local
    ,sum(C.taxes) impuestos_usd
    ,sum(C.flight_markdown) markdown_usd
    ,sum(C.flight_markup) markup_usd
    ,sum(C.flight_over) over_usd
    ,sum(C.tax_remesas) impuesto_remesas_ml
    ,sum(C.tax_remesas_local_currency) impuesto_remesas_usd
/****************************************************************************************************/
    --,-max(case when b.brand = 'Despegar' then 0 else case when b.commission_net_usd > 0 then if(b.discounts_net_usd = b.discounts_usd and b.commission_net_usd < b.commission_usd, b.discounts_usd / (b.commission_usd / b.commission_net_usd), b.discounts_net_usd) else 0 end end) as descuentos_fix
    --,-sum(if(round(b.discounts_net_usd,0)=round(b.discounts_usd,0) and round(b.commission_net_usd,0)<round(b.commission_usd,0), b.discounts_usd/(b.commission_usd/b.commission_net_usd),b.discounts_net_usd)) as descuentos_fix
    ,-sum(case when b.brand = 'Despegar' then 0 else case when b.commission_net_usd > 0 then if(round(b.discounts_net_usd,0)=round(b.discounts_usd,0) and round(b.commission_net_usd,0)<round(b.commission_usd,0), b.discounts_usd/(b.commission_usd/b.commission_net_usd),b.discounts_net_usd) else 0 end end) as descuentos_fix
    ,-sum(b.affiliates_usd) as afiliadas
    ,sum(C.agency_fee_total) as fee_agencia
    ,-sum(case when t.purchase_type='Carrito' then b.affiliates_usd else c.agency_fee_total end) as comision_agencia
    ,+sum(b.discounts_mkt_funds_usd) as mkt_discounts /* alias desc_partner */
/*** Cargos Variables ***/
/* COI-CCP */
    ,-sum(b.ccp_usd) as ccp
    ,-sum(b.coi_usd) as coi
    ,sum(b.coi_interest_usd) as interes_coi
/* One Revenue - Backoffice */
    ,sum(b.backend_air_usd) as backend_air
    ,sum(b.backend_non_air_usd) as backend_nonair
    ,sum(b.other_incentives_air_usd) as other_incentives_air
    ,sum(b.other_incentives_non_air_usd) as other_incentives_nonair
    -- sum(b.otros_usd) as otros, -- no continua en el modelo
    ,sum(b.errors_usd) as errors
    ,-sum(b.frauds_usd) as frauds
    ,-sum(b.loyalty_usd) as loyalty
    ,-sum(b.ott_usd) as ott
    ,-sum(b.revenue_taxes_usd) as revenue_tax
    ,+sum(media_revenue_usd ) as media_revenue
    ,-sum(b.vendor_commission_usd) as vendor_commission /* vendedor - call - islas */
    ,-sum(b.revenue_sharing_usd) as revenue_sharing /* comision asociados - b2b2c - islas liverpool */
    ,-sum(b.customer_claims_usd) as customer_claims
    ,-sum(b.customer_service_usd) as customer_service
    ,-sum(b.cancellations_usd) as cancelaciones
    ,sum(b.breakage_revenue_usd) as breakage_revenue
/* One Revenue - Resultado Financiero */
    ,sum(b.van_collection_usd - b.collection_amount_usd) as diferencia_cobro
    ,sum(-b.van_payment_usd + b.payment_amount_usd) as diferencia_pago    
/*Margen Neto = Fee + Comision - Descuentos*/
    ,sum(b.margin_net_usd) as margen_neto
    ,sum(b.fee_net_usd + b.commission_net_usd - b.discounts_net_usd) as margen_neto_calc
/*Cargos Variables = backendair + backendnonair + other_incentives_air + otros_usd - ccp - coi + interes_coi - customer_service - errors + afiliadas - frauds - loyalty - ott - revtaxes - mktfunds_discounts - comision_vendedor_callcenter - cancellations + MediaRevenue - comisionb2b2c  */
    ,sum(b.variable_charges_usd) as cargos_variables
    ,sum(b.variable_charges_without_mkt_usd) as cargos_variables_sin_mkt
--sum(b.backendair_usd + B.backendnonair_usd + b.otherincentivesair_usd + b.otros_usd - b.ccpxproducto_usd - b.coixproducto_usd + b.interescoixproducto_usd - b.customerservice_usd - b.errors_usd - b.afiliadas_usd - b.frauds_usd - b.loyalty_usd - b.ott_usd - b.revtaxes_usd + b.mktfunds_discounts_usd - b.comisionvendedorcallcenter_usd - b.cancellations_usd + b.MediaRevenue_USD - b.comisionb2b2c_usd - b.comisiondeasociado_usd - b.comisionvendedorislas_usd - b.revenuesharingislas_usd) as cargos_variables_calc,
/***** Margen Variable = Margen Neto - Cargos Variables *****/
    ,sum(b.margin_variable_net_usd) as margen_var_neto
    ,sum(b.margin_variable_net_usd+if(a.correccion_be is null, 0, a.correccion_be)+if(funds.mktg_funds_usd is null, 0, funds.mktg_funds_usd)) as mgvar
/***** Resultados Financieros *****/    
    ,sum(b.collection_amount_usd) as collection_amount_usd
    ,sum(b.payment_amount_usd) as payment_amount_usd
    ,sum(b.van_collection_usd) as van_collection_usd
    ,sum(b.van_payment_usd) as van_payment_usd
    ,sum(b.financial_result_usd) as financial_result_usd
    ,sum(b.margin_variable_net_usd) as margin_variable_net_usd
    ,sum(b.npv_net_usd) as npv_net_usd
    ,sum(b.net_revenues_usd) as net_revenue
    ,sum(t.conversion_rate) as tipo_cambio
    ,sum(b.dif_fx_usd) as dif_fx
    ,sum(b.dif_fx_air_usd) as dif_fx_air
    ,sum(b.currency_hedge_usd) as currency_hedge
    ,sum(b.currency_hedge_air_usd) as currency_hedge_air
from data.analytics.bi_transactional_fact_products p
join data.analytics.bi_transactional_fact_transactions t on t.transaction_code = p.transaction_code and t.reservation_year_month >= date('2021-07-01')
left join data.analytics.bi_transactional_fact_charges c on p.product_id = c.product_id and c.reservation_year_month >= date('2021-07-01')
join data.analytics.bi_pnlop_fact_current_model b on p.product_id=b.product_id and b.date_reservation_year_month > '2024-01-01'
left join data.tmp.correccion_BE a on cast(a.product_id as varchar)=b.product_id
left join data.tmp.mktg_funds funds on funds.product_id = b.product_id
where p.reservation_year_month >= date('2023-01-01')
and ( t.confirmation_date >= {{fecha_ini}} and t.confirmation_date <= {{fecha_fin}})
and t.channel  in ( select tx.channel from data.analytics.bi_transactional_fact_transactions tx where tx.line_of_business in ('B2B2C') and tx.reservation_year_month >= date('2021-07-01'))
--and t.country_code = 'BR'
--and p.tipoviaje = 'Int'
and p.status = 'Confirmado'
--and b.brand = 'Despegar'
--and t.tipodecompra in ('Vuelos')
--and t.channel != 'latam-airlines'
--and t.channel in ('latam-airlines','latam-airlines-canje','latam-airlines-off')
--and partner_id = 'bancolombia'
--and t.channel = 'bestday-call-liverpool'
--and partner_id in ('Didi')
--and t.tipodecompra = 'Hoteles'
--and parentchannel = 'Agencias Afiliadas'
--and gateway = 'EXP'
--and b.brand = 'Best Day'
--and p.tipoviaje = 'Nac'
--and t.pais_codigo = 'CO'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45
limit 100