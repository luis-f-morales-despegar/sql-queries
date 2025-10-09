WITH cvr as (
SELECT
--ranking,
bwt.parent_channel,
    case when bwt.country in ('BR') then bwt.country
         else ('Otros') end as site_agrupado,
    plataforma,
    case when bwt.producto_fenix in ('Hoteles', 'Vuelos') then bwt.producto_fenix
         when bwt.producto_fenix in ('Actividades','Buses','Autos','Cruceros','Asistencia al viajero','Traslados') then 'ONA'
         when bwt.producto_fenix in ('Carrito', 'Bundles', 'Gateways', 'Escapadas') then 'COMBINED_PRODUCT'
         else bwt.producto_fenix end as producto,
        bwt.channel,
        routetype,
        bwt.country,
--CASE WHEN ranking <=20 THEN rp.partner_id
--ELSE 'Otros Partners' END AS partner_id_agrupado,
--CASE WHEN ranking <=20 THEN channel
--ELSE 'Otros channels' END AS channel_agrupado,
    count(distinct bwt.userid) as usuarios,
    year(cast(date as date)) as year,
    week(cast(date as date)) as semana,
    count(distinct if(bwt.flow = 'HOME', bwt.userid, NULL)) as usuarios_home,
    count(distinct if(bwt.flow = 'LANDING', bwt.userid, NULL)) as usuarios_landing,
    count(distinct if(bwt.flow = 'SEARCH', bwt.userid, NULL)) as usuarios_searchers,
    count(distinct if(bwt.flow = 'DETAIL', bwt.userid, NULL)) as usuarios_detail,
    count(distinct if(bwt.flow = 'INTER-XS', bwt.userid, NULL)) as usuarios_PI,
    count(distinct if(bwt.flow = 'CHECKOUT', bwt.userid, NULL)) as usuarios_checkouters,
    count(distinct if(bwt.flow = 'THANKS', bwt.userid, NULL)) as usuarios_thankers,
    count(distinct transaction_code) as Bookings
FROM data.lake.bi_web_traffic bwt /*left join rp on bwt.partner_id = rp.partner_id*/
left join data.lake.ch_bo_partner_partner pp on cast(pp.partner_code as varchar) = bwt.partner_id
WHERE date >= '2024-01-01'
--and bwt.country = 'BR'
/*and cast(date as date)  between {{fecha_desde}} and {{fecha_hasta}} 
[[and pp.name = {{Fantasy_Name}}]]
[[and bwt.channel = {{Channel}}]]*/
and bwt.partner_id IS NOT NULL
and bwt.parent_channel in ('White Label', 'API', 'Agencias afiliadas')
and is_bot = 0
and ispageview = 1
and flg_detalle_cp = 0
and routetype <> ''
--and bwt.channel = 'latam-airlines'
and bwt.plataforma <> 'App'
GROUP BY 1,2,3,4,5,6, 7, year(cast(date as date)), week(cast(date as date))
)
select distinct(parent_channel) 
from cvr
limit 100


----------------------------------------------------------
----------------------------------------------------------

--- GB PnL

WITH bt_detail AS (
    SELECT
        fh.gestion_date AS Fecha, 
        fh.brand AS Marca,
     --   fh.site AS site,
  /*      CASE 
            WHEN fh.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'PY'
            WHEN fh.partner_id IN ('P12212', 'AP11666') THEN 'CR_CTA'
            WHEN fh.partner_id = 'AP12147' THEN 'SV_CTA'
            WHEN fh.partner_id = 'AP12854' THEN 'SV_CTA'
            WHEN fh.partner_id IN ('AP12509', 'AP11813') THEN 'GT_CTA'
            WHEN fh.partner_id = 'AP12158' THEN 'PA_CTA'
            WHEN fh.partner_id IN ('AP12213', 'AP11843') THEN 'HN_CTA'
            WHEN fh.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'DO_CTA'
            ELSE fh.country_code 
        END AS pais_corregido,
     */   CASE 
    WHEN fh.brand IN ('Best Day', 'Despegar') THEN
        CASE 
            WHEN fh.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'OT'
            WHEN fh.partner_id IN ('P12212', 'AP11666') THEN 'OT'
            WHEN fh.partner_id IN ('AP12147', 'AP12854') THEN 'OT'
            WHEN fh.partner_id IN ('AP12509', 'AP11813') THEN 'OT'
            WHEN fh.partner_id = 'AP12158' THEN 'OT'
            WHEN fh.partner_id IN ('AP12213', 'AP11843') THEN 'OT'
            WHEN fh.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'OT'
            WHEN fh.country_code IN ('MX', 'BR', 'CO', 'AR', 'EC', 'PE', 'CL') THEN fh.country_code
            WHEN fh.country_code IN ('US', 'PA', 'ES', 'CR') THEN 'GL'
            WHEN fh.country_code IN ('UY', 'BO') THEN 'OT'
            ELSE 'OT'
        END
    ELSE 'OT'
END AS country_metas,
        CASE 
            WHEN fh.channel IN (
                'hoteldo-html-classic', 'hoteldo-html-silver', 'hoteldo-html-platinum', 
                'hoteldo-html-gold', 'travel_agency', 'travel-agency-whitelabel', 
                'travel-agency-bo'
            ) THEN 'HTML'
            WHEN fh.channel IN (
                'hoteldo-api-g1', 'hoteldo-api-g2', 'hoteldo-api-g3', 'hoteldo-api-g4', 
                'hoteldo-api-g5', 'hoteldo-api-g1-block', 'hoteldo-api-g2-block', 
                'hoteldo-api-g3-block', 'hoteldo-api-g4-block', 'hoteldo-api-g5-block', 
                'hoteldo-api-g1-public', 'hoteldo-api-g2-public', 'hoteldo-api-g3-public', 
                'hoteldo-api-g4-public', 'hoteldo-api-g5-public', 'hoteldo-api-g1-public-block', 
                'hoteldo-api-g2-public-block', 'hoteldo-api-g3-public-block', 
                'hoteldo-api-g4-public-block', 'hoteldo-api-g5-public-block', 
                'agency-pam-pp-ctrip', 'expedia'
            ) THEN 'API'
            ELSE NULL
        END AS parent_channel_metas,
        CASE 
            WHEN fh.brand = 'Best Day' AND fh.parent_channel = 'Agencias afiliadas' THEN 'HTML HDO'
            WHEN fh.brand = 'Despegar' AND fh.parent_channel = 'Agencias afiliadas' THEN 'AAFF BY HDO'
            WHEN fh.brand = 'Best Day' AND fh.parent_channel = 'API' THEN 'API HDO'
            WHEN fh.brand = 'Despegar' AND fh.parent_channel = 'API' THEN 'API D!'
            ELSE NULL
        END AS channel_metas,
        fh.buy_type_code AS buy_type, 
     --   fh.channel,
      --  fh.trip_type_code AS viaje, 
        sum(fh.gestion_gb) AS gb_gestion,
        SUM(fh.gestion_gb * fh.confirmation_gradient) AS gb_grad, 
        SUM(
            CASE 
                WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') THEN (pnl.net_revenues_usd - pnl.affiliates_usd)
                ELSE pnl.net_revenues_usd
            END
        ) AS net_revenues_usd,
        SUM(pnl.npv_net_usd) AS npv_net_usd
    FROM analytics.bi_sales_fact_sales_recognition fh 
    LEFT JOIN data.analytics.bi_pnlop_fact_current_model pnl 
        ON fh.product_id = pnl.product_id 
        AND pnl.date_reservation_year_month > '2020-01'
    WHERE 
        fh.lob_gestion IN ('stg__sales_b2bnohoteldo', 'stg_sales__b2bhoteldo')
        AND pnl.line_of_business = 'B2B'
        and fh.gestion_date >= DATE('2023-01-01')
        and fh.partition_period > '2021-01'
        and fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
    GROUP BY 1, 2, 3, 4, 5, 6
)
select 
/*CASE 
        WHEN Marca IN ('Best Day', 'Despegar') THEN
            CASE 
                WHEN pais_corregido IN ('MX', 'BR', 'CO', 'AR', 'EC', 'PE', 'CL') THEN pais_corregido
                WHEN pais_corregido IN ('PY', 'CR_CTA', 'SV_CTA', 'GT_CTA', 'PA_CTA', 'HN_CTA', 'DO_CTA') THEN 'OT'
                WHEN pais_corregido IN ('US', 'PA', 'ES', 'CR') THEN 'GL'
                WHEN pais_corregido IN ('UY', 'BO') THEN 'OT'
                ELSE 'OT'
            END
        ELSE 'OT'
    END AS*/  country_metas,
    fecha,
    marca,
  --  site,
  --  pais_corregido,
    parent_channel_metas,
    channel_metas,
    buy_type,
    gb_gestion,
    gb_grad,
    net_revenues_usd,
    npv_net_usd
FROM bt_detail  
--where  fecha >= DATE('2024-11-01')
--AND pais_corregido = 'BR'
--and buy_type = 'Hoteles'
--and channel_metas = 'API HDO'
ORDER BY 2, 1, 7, 8 --1, 2, 3, 4, 5, 6, 7, 8, 9, 10;


----------------------------------------------------------------------------
-----------------------------------------------------------------------------

-- RECONOCIMIENTO DE INGRESO

with bt_detail as (
               select
               --      fv.transaction_code as tx_code
              --   	,fv.product_id
              --     	,fv.origin_product_id
                   	fv.line_of_business_code as lob
                   	,fv.brand                   	
                  -- 	,case fv.site when 'Mexico' 	then '01-Mexico'
			        --      		  when 'Brasil' 	then '02-Brasil'
			          --    		  when 'Argentina' 	then '03-Argentina'
			            --  		  when 'Chile' 		then '04-Chile'
  			              --		  when 'Colombia' 	then '05-Colombia'
  			              	--	  when 'Peru' 		then '06-Peru'
  			              		--  else '07-Global'			  
               		--	end as region
            --   		,fv.site	
            /*   		,case 
                        when fv.partner_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') then 'PY'
                        when fv.partner_id in ('P12212', 'AP11666') then 'CR_CTA'
                        when fv.partner_id = 'AP12147' then 'SV_CTA'
                        when fv.partner_id = 'AP12854' then 'SV_CTA'
                        when fv.partner_id in ('AP12509', 'AP11813') then 'GT_CTA'
                        when fv.partner_id = 'AP12158' then 'PA_CTA'
                        when fv.partner_id in ('AP12213', 'AP11843') then 'HN_CTA'
                        when fv.partner_id in ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') then 'DO_CTA'
                        else fv.country_code 
                        end as pais_corregido
           */--    		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                  --     		country_code,'OT') as country
                  -- 	,fv.parent_channel
                  -- 	,fv.channel as channel
               --    	,case when length(fv.partner_id) > 0 then fv.partner_id else fv.channel end as agency_code
                   --	,fv.agency_name
                   --	,fv.product_status
                  -- 	,fv.product_is_confirmed_flg as is_confirmed_flg
                 --  	,fv.trip_type_code as trip_type
                   	,fv.buy_type_code as buy_type
               --    	,fv.product
                   	-- detail
                   	-- hotel_id
                   	--,fv.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fv.recognition_date
           		--	,fv.booking_date
           		--	,fv.confirmation_date
           		--	,fv.checkin_date
           		--	,fv.checkout_date
 ,CASE 
        WHEN fv.channel IN ('hoteldo-html-classic', 'hoteldo-html-silver', 'hoteldo-html-platinum', 
                         'hoteldo-html-gold', 'travel_agency', 'travel-agency-whitelabel', 
                         'travel-agency-bo') THEN 'HTML'
        WHEN fv.channel IN ('hoteldo-api-g1', 'hoteldo-api-g2', 'hoteldo-api-g3', 'hoteldo-api-g4', 
                         'hoteldo-api-g5', 'hoteldo-api-g1-block', 'hoteldo-api-g2-block', 
                         'hoteldo-api-g3-block', 'hoteldo-api-g4-block', 'hoteldo-api-g5-block', 
                         'hoteldo-api-g1-public', 'hoteldo-api-g2-public', 'hoteldo-api-g3-public', 
                         'hoteldo-api-g4-public', 'hoteldo-api-g5-public', 'hoteldo-api-g1-public-block', 
                         'hoteldo-api-g2-public-block', 'hoteldo-api-g3-public-block', 
                         'hoteldo-api-g4-public-block', 'hoteldo-api-g5-public-block', 
                         'agency-pam-pp-ctrip', 'expedia') THEN 'API'
        ELSE NULL
    END AS parent_channel_metas,
        CASE 
        WHEN fv.brand = 'Best Day' AND fv.parent_channel = 'Agencias afiliadas' THEN 'HTML HDO'
        WHEN fv.brand = 'Despegar' AND fv.parent_channel = 'Agencias afiliadas' THEN 'AAFF BY HDO'
        WHEN fv.brand = 'Best Day' AND fv.parent_channel = 'API' THEN 'API HDO'
        WHEN fv.brand = 'Despegar' AND fv.parent_channel = 'API' THEN 'API D!'
        ELSE NULL
    END AS channel_metas,
    CASE 
    WHEN fv.brand IN ('Best Day', 'Despegar') THEN
        CASE 
            WHEN fv.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'OT'
            WHEN fv.partner_id IN ('P12212', 'AP11666') THEN 'OT'
            WHEN fv.partner_id IN ('AP12147', 'AP12854') THEN 'OT'
            WHEN fv.partner_id IN ('AP12509', 'AP11813') THEN 'OT'
            WHEN fv.partner_id = 'AP12158' THEN 'OT'
            WHEN fv.partner_id IN ('AP12213', 'AP11843') THEN 'OT'
            WHEN fv.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'OT'
            WHEN fv.country_code IN ('MX', 'BR', 'CO', 'AR', 'EC', 'PE', 'CL') THEN fv.country_code
            WHEN fv.country_code IN ('US', 'PA', 'ES', 'CR') THEN 'GL'
            WHEN fv.country_code IN ('UY', 'BO') THEN 'OT'
            ELSE 'OT'
        END
    ELSE 'OT'
END AS country_metas,
           			-->>>>><<<<<----
           			--> Metricas <--
                    sum(fv.gestion_gb) as gb_RI 
             --       ,max(pnl.b2b_gradient_margin) as gradiente_margen
            --        ,(sum(pnl.commission_net_usd)/sum(ch.comision_desp)) as gradiente_margen_calc
                    --> Revenue Margin <--
           --         ,sum(pnl.fee_net_usd) as fee_neto
 			--		,sum(pnl.commission_net_usd) as comision_neta
			--		,-sum(pnl.discounts_net_usd) as descuentos_neto
    				--> Afiliadas <--					
    		--		,sum(c.agency_fee_total) as fee_agencia
    		--		,-sum(ch.comision_ch) as comision_agencia_channels
    		--		,-sum(case when fv.buy_type_code='Carrito' then pnl.affiliates_usd else c.agency_fee_total end) as comision_agencia
					--> *** PROFIT *** <--
    			--	,sum(pnl.backend_air_usd) as backend_air
    			--	,sum(pnl.backend_non_air_usd) as backend_nonair
    			--	,sum(pnl.other_incentives_air_usd) as other_incentives_air
    		--		,sum(pnl.other_incentives_non_air_usd) as other_incentives_nonair   				
    		--		,sum(pnl.breakage_revenue_usd) as breakage_revenue
    		--		,+sum(pnl.media_revenue_usd ) as media_revenue    				
    		--		,+sum(pnl.discounts_mkt_funds_usd) as mkt_discounts /* alias desc_partner */
    				--> *** LOSS *** <--
					--> COI-CCP <--
    		--		,-sum(pnl.ccp_usd) as ccp
    		--		,-sum(pnl.coi_usd) as coi
    			--	,sum(pnl.coi_interest_usd) as interes_coi
    				--> Cargos Variables <--
				--	,sum(pnl.customer_service_usd) as customer_service
    			--	,sum(pnl.errors_usd) as errors 
				--	,-sum(pnl.affiliates_usd) as afiliadas
				--	,sum(pnl.frauds_usd) as frauds				
				--    ,-sum(pnl.loyalty_usd) as loyalty
    			--	,-sum(pnl.ott_usd) as ott
    			--	,-sum(pnl.revenue_taxes_usd) as revenue_tax
				  --  ,-sum(pnl.cancellations_usd) as cancelaciones
	    			--,-sum(pnl.customer_claims_usd) as customer_claims				
    				---,sum(pnl.revenue_sharing_usd) as revenue_sharing /* comision asociados - b2b2c - islas liverpool */	    			
    				--,sum(pnl.vendor_commission_usd) as vendor_commission /* vendedor - call - islas */
				--	,-sum(pnl.mkt_cost_net_usd) as mkt_cost
				---	,sum(pnl.agency_backend_usd) as overs_api
    				--> *** Resultado Financiero *** <--
                    ,sum( case when fv.country_code = 'BR' and fv.product not in ('Vuelos')
                           		then (pnl.net_revenues_usd-pnl.affiliates_usd)
                           else pnl.net_revenues_usd
                       end) as fix_net_revenues
                    ,sum(pnl.net_revenues_usd)as net_revenues
                    ,sum(pnl.npv_net_usd) as npv_RI
                --    ,sum(pr.dif_fx_usd + pr.dif_fx_air_usd) as dif_fx
                --    ,sum(pr.currency_hedge_usd + pr.currency_hedge_air_usd) as hedge
                --    ,sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as npv_calc
             from analytics.bi_sales_fact_sales_recognition fv 
             left join analytics.bi_pnlop_fact_current_model pnl on fv.product_id = pnl.product_id and pnl.date_reservation_year_month > '2023-01'
             left join analytics.bi_transactional_fact_charges c on fv.product_id = c.product_id and c.reservation_year_month >= date'2023-01-01'
             left join analytics.bi_transactional_fact_products p on fv.product_id = p.product_id and p.reservation_year_month >= date'2023-01-01'
             left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fv.product_id and pr.date_reservation_year_month >= '2023-01'
             left join 
                (select cast(p.transaction_id as varchar) as product_id, payment_methods, p.status,max(p.penalty), max(conversion_rate*net_commission_partner) as comision_ch,max(conversion_rate*net_commission_despegar) as comision_desp, avg(conversion_rate) as tipo_cambio
                    from data.lake.channels_bo_product p
                    inner join data.lake.channels_bo_sale s on p.sale_id = s.id
                group by 1,2,3) as ch
                on cast(ch.product_id as varchar) = p.reference_id
             where fv.recognition_date >= DATE('2023-01-01')
             and fv.lob_gestion  in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
              and pnl.line_of_business = 'B2B'
              and fv.partition_period >= '2023-01'
              and fv.parent_channel in ('API', 'Agencias afiliadas')
              --and p.transaction_code = '803280257900'
              group by 1,2,3,4,5,6 ,7--,12,13,14--15,16,17--,18,19,20,21, 22 --,22,23,24
 )
 select 
 /*CASE 
        WHEN brand IN ('Best Day', 'Despegar') THEN
            CASE 
                WHEN pais_corregido IN ('MX', 'BR', 'CO', 'AR', 'EC', 'PE', 'CL') THEN pais_corregido
                WHEN pais_corregido IN ('PY', 'CR_CTA', 'SV_CTA', 'GT_CTA', 'PA_CTA', 'HN_CTA', 'DO_CTA') THEN 'OT'
                WHEN pais_corregido IN ('US', 'PA', 'ES', 'CR') THEN 'GL'
                WHEN pais_corregido IN ('UY', 'BO') THEN 'OT'
                ELSE 'OT'
            END
        ELSE 'OT'
    END AS*/ country_metas,
-- tx_code,
 recognition_date,
 brand,
-- site,
 --pais_corregido,
-- parent_channel,
-- channel,
 parent_channel_metas,
 channel_metas,
-- agency_code,
 buy_type,
-- booking_date,
-- confirmation_date,
-- checkin_date,
-- checkout_date,
 gb_RI,
 --gradiente_margen_calc,
 fix_net_revenues,
npv_RI 
from bt_detail
--where  recognition_date = DATE('2023-11-08')
--AND pais_corregido = 'BR'
--and buy_type = 'Hoteles'
--and channel_metas = 'API HDO'
order by recognition_date, country_metas, channel_metas, buy_type
