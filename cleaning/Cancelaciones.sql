---  PnL API & HTML ----------------------------------


--  Mail Diario (Venta PnL Emitida) + adecuaciones a terminología Budget / RR desde RI de cosecha ---> Cosecha Venta Emitida
 
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
                     fh.transaction_code as tx_code
                  	,fh.product_id
                   	,fh.origin_product_id
                   	,fh.line_of_business_code as lob
                   	,CASE 
   					 WHEN fh.brand = 'Despegar' THEN 'D!' 
    					WHEN fh.brand = 'Best Day' THEN 'BD!' 
    					ELSE NULL
						END AS Marca                	
                   	/*,case fv.site when 'Mexico' 	then '01-Mexico'
			              		  when 'Brasil' 	then '02-Brasil'
			              		  when 'Argentina' 	then '03-Argentina'
			              		  when 'Chile' 		then '04-Chile'
  			              		  when 'Colombia' 	then '05-Colombia'
  			              		  when 'Peru' 		then '06-Peru'
  			              		  else '07-Global'			  
               			end as region
               		,fv.site	
               		,if(fv.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                       		country_code,'OT') as country */
                     ,case 
                        when fh.partner_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') then 'PY'
                        when fh.partner_id in ('P12212', 'AP11666') then 'CR_CTA'
                        when fh.partner_id = 'AP12147' then 'SV_CTA'
                        when fh.partner_id = 'AP12854' then 'SV_CTA'
                        when fh.partner_id in ('AP12509', 'AP11813') then 'GT_CTA'
                        when fh.partner_id = 'AP12158' then 'PA_CTA'
                        when fh.partner_id in ('AP12213', 'AP11843') then 'HN_CTA'
                        when fh.partner_id in ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') then 'DO_CTA'
                        else fh.country_code 
                        end as pais_corregido
                        ,CASE
    WHEN fh.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'Others'
    WHEN fh.partner_id IN ('P12212', 'AP11666') THEN 'Others'
    WHEN fh.partner_id IN ('AP12147', 'AP12854') THEN 'Others'
    WHEN fh.partner_id IN ('AP12509', 'AP11813') THEN 'Others'
    WHEN fh.partner_id = 'AP12158' THEN 'Others'
    WHEN fh.partner_id IN ('AP12213', 'AP11843') THEN 'Others'
    WHEN fh.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'Others'
    WHEN fh.country_code = 'MX' THEN 'Mexico'
    WHEN fh.country_code = 'BR' THEN 'Brasil'
    WHEN fh.country_code = 'CO' THEN 'Colombia'
    WHEN fh.country_code = 'AR' THEN 'Argentina'
    WHEN fh.country_code = 'EC' THEN 'Ecuador'
    WHEN fh.country_code = 'PE' THEN 'Peru'
    WHEN fh.country_code = 'CL' THEN 'Chile'
    WHEN fh.country_code IN ('US', 'PA', 'ES', 'CR') THEN 'USA/ROW'
    WHEN fh.country_code = 'UY' THEN 'Others'
    WHEN fh.country_code = 'BO' THEN 'Others'
    ELSE 'Others'
                     END AS country_metas
                    ,CASE
           				WHEN fh.brand = 'Best Day' AND fh.parent_channel = 'API' THEN 'MAY'
    					WHEN fh.brand = 'Despegar' AND fh.parent_channel = 'API' THEN 'API'
    					WHEN (fh.brand = 'Best Day' OR fh.brand = 'Despegar') AND fh.parent_channel = 'Agencias afiliadas' THEN 'MIN'
    					ELSE NULL -- 
					END AS channel_metas
                   --	,fv.parent_channel
                   --  ,fh.channel
					,case when length(fh.partner_id) > 0 then fh.partner_id else fh.channel end as agency_code
                  	,fh.agency_name
                   	,fh.product_status
                   	,fh.product_is_confirmed_flg as is_confirmed_flg
                   	,CASE
       				 WHEN fh.trip_type_code = 'Nac' THEN 'NAC'
       				 WHEN fh.trip_type_code = 'Int' THEN 'INT'
        		   ELSE NULL 
    				END AS viaje
                   	,fh.buy_type_code as buy_type
                   	,fh.product
                   	-- detail
                   	-- hotel_id
                   	,fh.main_airline_code as iata
                   	-- airline_name
                   	-- tarifa_efectiva
                   --	,split_part(fv.destination, ', ', 2) as destination_city
           			--,split_part(fv.destination, ', ', 1) as destination_country
           			,fh.recognition_date
           			,fh.booking_date
           			,fh.confirmation_date
           			,fh.checkin_date
           			,fh.checkout_date
           			-->>>>><<<<<----
           			--> Metricas <--
    					,ROUND(sum(fh.gestion_gb),2) as gb_s_gx,
                        ROUND(sum(fh.gestion_gb * fh.confirmation_gradient),2) as gb_c_gx, 
                        ROUND(sum( case when fh.country_code = 'BR' and fh.product not in ('Vuelos') then (pnl.net_revenues_usd - bo.tpc_usd)
                               when fh.channel = 'expedia' then (pnl.net_revenues_usd - bo.tpc_usd)    
                               else pnl.net_revenues_usd
                            end
                        ),2) as net_revenues_usd,
                       ROUND(sum(pnl.net_revenues_usd),2) as net_revenues_usd_s_ajuste,
                        ROUND(sum(pnl.npv_net_usd),2) as npv_net_usd,
                        ROUND(sum(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
        		             + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
        		             + pnl.affiliates_usd)    -- sumamos afiliadas
                           / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)) ) -- quitar gradiente
                         )
                         - coalesce(bo.tpc_usd,0)     -- quitar tpc (en sustitucion de afiliadas)
                        )
                         * max(if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)))),2) 
                        as fix_npv  
                    from analytics.bi_sales_fact_sales_recognition fh 
                    left join analytics.bi_pnlop_fact_current_model pnl on fh.product_id = pnl.product_id and pnl.date_reservation_year_month > '2021-01'
                    left join analytics.bi_transactional_fact_charges c on fh.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
                    left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fh.product_id and pr.date_reservation_year_month >= '2021-01'
                    left join bo_tpc bo on bo.product_id_original = fh.origin_product_id
                    where fh.gestion_date >= DATE('2024-12-01')
        			and fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
        				and fh.lob_gestion in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        				and pnl.line_of_business = 'B2B'
        				and fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
        				and partition_period > '2020-01'
          			--	and partner_id IN ('AG00073753', 'AG00037023')
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21)
SELECT 
    pais_corregido, 
    ROUND(SUM(gb_c_gx), 2) AS total_gb_c_gx,
    SUM(CASE WHEN product_status = 'Cancelado' THEN gb_c_gx ELSE 0 END) AS gb_cancelado,
    SUM(CASE WHEN product_status = 'Activo' THEN gb_c_gx ELSE 0 END) AS gb_activo,
    SUM(CASE WHEN product_status = 'Confirmado' THEN gb_c_gx ELSE 0 END) AS gb_confirmado,
    SUM(CASE WHEN product_status = 'Cancelado' THEN gb_c_gx ELSE 0 END) * 1.0 
    / NULLIF(SUM(gb_c_gx), 0) AS tasa_de_cancelacion
FROM 
    bt_detail
WHERE 
    pais_corregido IN ('UY', 'PY', 'AR')
    AND channel_metas = 'MAY'
    AND booking_date >= DATE '2024-01-01' 
    AND booking_date <= DATE '2024-12-31'
GROUP BY 
    pais_corregido



------

pais_corregido,    
COUNT(DISTINCT tx_code) AS total_distinct_tx_code,
    COUNT(DISTINCT CASE WHEN product_status = 'Cancelado' THEN tx_code END) AS distinct_cancelado,
    COUNT(DISTINCT CASE WHEN product_status = 'Activo' THEN tx_code END) AS distinct_activo,
    COUNT(DISTINCT CASE WHEN product_status = 'Confirmado' THEN tx_code END) AS distinct_confirmado,
    COUNT(DISTINCT CASE WHEN product_status = 'Cancelado' THEN tx_code END) * 1.0 
    / NULLIF(COUNT(DISTINCT tx_code), 0) AS tasa_de_cancelacion
FROM 
    bt_detail
WHERE 
    pais_corregido in ('UY', 'PY', 'AR')
    and booking_date >= date'2024-01-01' and booking_date <= date'2024-12-31'
 group by pais_corregido
   
 -----
 
 
 SELECT *
FROM bt_detail 
where country_metas = 'Others'
order by tx_code
--limit 1000

 
---

select * 
from analytics.b2b_fact_hotel_requests
where request_date is not null
order by request_date
limit 100
 
 
 
 
 
   
   
   