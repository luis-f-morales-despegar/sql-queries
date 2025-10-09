-------


--- Mail Diario Excel ---

with bo_tpc as( 
            select
                p.transaction_id as product_id_original
                /*metricas*/
                ,max(p.net_commission_partner * p.conversion_rate) as tpc_usd --third party commission
            from data.lake.channels_bo_product p
            join data.lake.channels_bo_sale s on s.id = p.sale_id
            where cast(s.created as date) >= DATE('2024-10-01') 
            and cast(s.created as date) < DATE('2024-12-26')
            group by 1
),
bt_detail as (
                    select
                        fh.gestion_date as Fecha, 
                        fh.brand as Marca,
                     -- fh.country_code as pais,
                   /*     case 
                        when fh.partner_id in ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') then 'PY'
                        when fh.partner_id in ('P12212', 'AP11666') then 'CR_CTA'
                        when fh.partner_id = 'AP12147' then 'SV_CTA'
                        when fh.partner_id = 'AP12854' then 'SV_CTA'
                        when fh.partner_id in ('AP12509', 'AP11813') then 'GT_CTA'
                        when fh.partner_id = 'AP12158' then 'PA_CTA'
                        when fh.partner_id in ('AP12213', 'AP11843') then 'HN_CTA'
                        when fh.partner_id in ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') then 'DO_CTA'
                        else fh.country_code 
                        end as pais_corregido,*/
                       -- if( fh.country_code in ('AR','CO','EC','PE','BR','CL','MX'),
                        --country_code,'OT') as pais,
                                                CASE
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
                     END AS country_metas,
                        fh.buy_type_code as productooriginal, 
                     --   fh.partner_id,
                    --    fh.agency_name,
   						CASE 
       						 WHEN fh.partner_id IN ('AG00073753', 'AG00037023') THEN 'API'
        					ELSE fh.parent_channel
    					END AS parent_channel_corregido,
    					fh.transaction_code as tx_code,
    					fh.product_status,
    					gateway_code,               ------- GATEWAY
                       -- fh.parent_channel,
                    --    fh.channel,
                     --   fh.trip_type_code as viaje, 
    					ROUND(sum(fh.gestion_gb),2) as gb_s_gradiente,
                        ROUND(sum(fh.gestion_gb * fh.confirmation_gradient),2) as gb, 
                        ROUND(sum(case when fh.country_code = 'BR' and fh.product not in ('Vuelos') then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)) ) ) )
        	                   when fh.channel = 'expedia' then (pnl.net_revenues_usd - (bo.tpc_usd * if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)) ) ) )	
                               else pnl.net_revenues_usd
                            end
                        )) as fix_net_revenues,
                     ROUND(sum(pnl.net_revenues_usd),2) as net_revenues_usd_s_ajuste,
                        ROUND(sum(pnl.npv_net_usd),2) as npv_net_usd,
                       ROUND(sum(((pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
        		             + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
        		             + pnl.affiliates_usd)    -- sumamos afiliadas
                           / if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)) ) -- quitar gradiente
                         )
                         - coalesce(bo.tpc_usd,0)     -- quitar tpc (en sustitucion de afiliadas)
                        )
                         * max(if(pnl.b2b_gradient_margin = '1', 1, cast(pnl.b2b_gradient_margin as decimal(2,2)))) 
                       ) as fix_npv  
                    from analytics.bi_sales_fact_sales_recognition fh 
                    left join analytics.bi_pnlop_fact_current_model pnl on fh.product_id = pnl.product_id and pnl.date_reservation_year_month > '2021-01'
                    left join analytics.bi_transactional_fact_charges c on fh.product_id = c.product_id and c.reservation_year_month >= date'2021-01-01'
                    left join analytics.bi_pnlop_fact_pricing_model pr on pr.product_id = fh.product_id and pr.date_reservation_year_month >= '2021-01'
                    left join bo_tpc bo on bo.product_id_original = fh.origin_product_id
                    where fh.gestion_date >= DATE('2024-06-01')
        			and fh.gestion_date <= current DAY -- EXLUYE HASTA HOY
        				and fh.lob_gestion in ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
        				and pnl.line_of_business = 'B2B'
        				and fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
        				and partition_period > '2020-01'
          			--	and partner_id IN ('AG00073753', 'AG00037023')
						group by 1,2,3,4,5,6,7,8)
SELECT * 
FROM bt_detail 
--where product_status = 'Confirmado'
--WHERE Fecha >= DATE('2024-01-01')
--and productooriginal = 'Hoteles'
--and Marca = 'Best Day'
limit 100
--order by 1,2,3,4,5,6
--  select fecha, sum(gb)gb, sum(net_revenues_usd) net_revenues_usd , sum(npv_net_usd)npv_net_usd
--  from bt_detail
--  group by 1
--  order by 1 desc
