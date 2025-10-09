

--- fact_GB
--- Mail Diario Excel ---  Se añade fix_fvm 2025-06-05  -- se añaden orders y # hoteles 2025-06-12

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
                        month(fh.gestion_date) AS mes_reserva,
                        year(fh.gestion_date) AS anio_reserva,
                        fh.brand as Marca,
                     -- fh.country_code as pais,
                        case 
                       when fh.partner_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148', 'AG00015606', 'AP13029', 'AP13030') then 'PY'
                        when fh.partner_id in ('P12212', 'AP11666') then 'CR_CTA'
                        when fh.partner_id = 'AP12147' then 'SV_CTA'
                        when fh.partner_id = 'AP12854' then 'SV_CTA'
                        when fh.partner_id in ('AP12509', 'AP11813') then 'GT_CTA'
                        when fh.partner_id = 'AP12158' then 'PA_CTA'
                        when fh.partner_id in ('AP12213', 'AP11843') then 'HN_CTA'
                        when fh.partner_id in ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') then 'DO_CTA'
                        else fh.country_code 
                        end as pais_corregido,
                       -- if( fh.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                        --country_code,'OT') as pais,
                   --     fh.buy_type_code as productooriginal, 
                        fh.partner_id,
                       fh.agency_name,
   						CASE 
       						 WHEN fh.partner_id IN ('AG00073753', 'AG00037023') THEN 'API'
        					ELSE fh.parent_channel
    					END AS parent_channel_corregido,
    			--		fh.transaction_code as tx_code,
    			--		fh.product_status,
                       fh.parent_channel as parent_channel_original,
                     --  fh.channel,
                     --   fh.trip_type_code as viaje, 
                       COUNT(DISTINCT prs.hotel_despegar_id) AS hoteles_vendidos,
    					COUNT(DISTINCT fh.transaction_code) AS orders,
                       ROUND(sum(fh.gestion_gb),2) as gb_s_gradiente,
                        ROUND(sum(fh.gestion_gb * fh.confirmation_gradient),2) as gb, 
                        ROUND(sum(case when fh.country_code = 'BR' and fh.product not in ('Vuelos') then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) ) )
        	                   when fh.channel = 'expedia' then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) ) )	
                               else pnl.net_revenues_usd
                            end
                        )) as fix_net_revenues,
                     ROUND(sum(pnl.net_revenues_usd),2) as net_revenues_usd_s_ajuste,
                     sum(pnl.npv_net_usd) as fvm,
	                 sum(pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd) as fvm_calc,
	                 sum(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
        		             + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
        		             + ( case when fh.parent_channel = 'Agencias afiliadas' and fh.buy_type_code in ('Hoteles','Alquileres') and fh.product_is_confirmed_flg = 0    -- Fix COI CCP - Promesas de Pago
        		                        then (pnl.coi_usd + pnl.ccp_usd - pnl.financial_result_usd + coalesce(fpp.coi_fix_con_gradiente,-pnl.coi_usd+pnl.financial_result_usd) + coalesce(fpp.ccp_fix_con_gradiente,-pnl.ccp_usd)) 
        		                        else 0 end ) 
        		             + pnl.affiliates_usd)    -- sumamos afiliadas
                           / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)) ) -- quitar gradiente para obtener bruto
                         )
                         - coalesce(fh.tpc_fix_iva,0)     -- quitar tpc (en sustitucion de afiliadas)
                        )
                         * max(if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(4,3)))) 
                        as fix_fvm
                    from ( select *
			            ,case when fv_prev.parent_channel = 'Agencias afiliadas' 
			   			then tpc_usd/(1+coalesce(cast(fix_iva.iva as decimal(4,3)),0)) 
			   			else tpc_usd 
			   	        end as tpc_fix_iva 
		                from analytics.bi_sales_fact_sales_recognition fv_prev
		                left join bo_tpc bo on bo.product_id_original = fv_prev.origin_product_id
		                left join raw.b2b_dim_html_iva_fix fix_iva on fix_iva.country = fv_prev.country_code
		                where fv_prev.partition_period > '2020-01'
	                     ) fh
                    left join analytics.bi_pnlop_fact_current_model pnl on fh.product_id = pnl.product_id and pnl.date_reservation_year_month > '2020-01'
                    left join analytics.bi_transactional_fact_charges c on fh.product_id = c.product_id and c.reservation_year_month >= date'2020-01-01'
                    left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fh.product_id and pr.date_reservation_year_month >= '2020-01'
                    left join bo_tpc bo on bo.product_id_original = fh.origin_product_id
                    left join data.lake.b2b_fix_coi_ccp fpp on fpp.transaction_code = cast(fh.transaction_code as varchar) -- Fix Promesas Pago
                    left join analytics.bi_transactional_fact_products prs on prs.product_id = fh.product_id and prs.reservation_year_month > date'2020-12-31'
                    where fh.gestion_date > DATE('2020-12-31')
        			and fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
        				and fh.lob_gestion in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        				and pnl.line_of_business = 'B2B'
        				and fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
        				and partition_period > '2020-01'
          			--	and partner_id IN ('AG00073753', 'AG00037023')
						group by 1,2,3,4,5,6,7,8)
SELECT * 
FROM bt_detail 



 --fv.transaction_code as tx_code
  --from analytics.bi_sales_fact_sales_recognition fv 
            			 ,max(pr.hotel_name) as Hotel
           			 
           			 ,max(pr.hotel_chain_name) as Cadena
         left join analytics.bi_transactional_fact_products pr on pr.product_id = fv.product_id and pr.reservation_year_month > date'2023-12-31'