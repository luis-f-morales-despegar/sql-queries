WITH bt_detail AS (
        SELECT
            fv.transaction_code AS tx_code,
            fv.product_id,
            fv.origin_product_id,
            fv.line_of_business_code AS lob,
            fv.brand,
            CASE 
                WHEN fv.site = 'Mexico' THEN '01-Mexico'
                WHEN fv.site = 'Brasil' THEN '02-Brasil'
                WHEN fv.site = 'Argentina' THEN '03-Argentina'
                WHEN fv.site = 'Chile' THEN '04-Chile'
                WHEN fv.site = 'Colombia' THEN '05-Colombia'
                WHEN fv.site = 'Peru' THEN '06-Peru'
                ELSE '07-Global'
            END AS region,
            fv.site,
            CASE 
                WHEN fv.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148') THEN 'PY'
                WHEN fv.partner_id IN ('P12212', 'AP11666') THEN 'CR_CTA'
                WHEN fv.partner_id = 'AP12147' THEN 'SV_CTA'
                WHEN fv.partner_id = 'AP12854' THEN 'SV_CTA'
                WHEN fv.partner_id IN ('AP12509', 'AP11813') THEN 'GT_CTA'
                WHEN fv.partner_id = 'AP12158' THEN 'PA_CTA'
                WHEN fv.partner_id IN ('AP12213', 'AP11843') THEN 'HN_CTA'
                WHEN fv.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'DO_CTA'
                ELSE fv.country_code
            END AS pais_corregido,
            IF(fv.country_code IN ('AR','CO','EC','PE','BR','CL','MX'), fv.country_code, 'OT') AS country,
            fv.parent_channel,
            fv.channel AS channel,
            CASE 
                WHEN LENGTH(fv.partner_id) > 0 THEN fv.partner_id
                ELSE fv.channel 
            END AS agency_code,
            fv.product_status,
            fv.product_is_confirmed_flg AS is_confirmed_flg,
            fv.trip_type_code AS trip_type,
            fv.buy_type_code AS buy_type,
            fv.product,
            fv.recognition_date,
            fv.booking_date,
            fv.confirmation_date,
            fv.checkin_date,
            fv.checkout_date,
            CASE 
                WHEN channel IN (
                    'hoteldo-html-classic', 'hoteldo-html-silver', 'hoteldo-html-platinum', 
                    'hoteldo-html-gold', 'travel_agency', 'travel-agency-whitelabel', 'travel-agency-bo'
                ) THEN 'HTML'
                WHEN channel IN (
                    'hoteldo-api-g1', 'hoteldo-api-g2', 'hoteldo-api-g3', 'hoteldo-api-g4', 
                    'hoteldo-api-g5', 'hoteldo-api-g1-block', 'hoteldo-api-g2-block', 
                    'hoteldo-api-g3-block', 'hoteldo-api-g4-block', 'hoteldo-api-g5-block'
                ) THEN 'API'
                ELSE NULL
            END AS parent_channel_metas,
            CASE 
                WHEN fv.brand = 'Best Day' AND parent_channel = 'Agencias afiliadas' THEN 'HTML HDO'
                WHEN fv.brand = 'Despegar' AND parent_channel = 'Agencias afiliadas' THEN 'AAFF BY HDO'
                WHEN fv.brand = 'Best Day' AND parent_channel = 'API' THEN 'API HDO'
                WHEN fv.brand = 'Despegar' AND parent_channel = 'API' THEN 'API D!'
                ELSE NULL
            END AS channel_metas,
            SUM(fv.gestion_gb) AS gb_RI,
            MAX(pnl.b2b_gradient_margin) AS gradiente_margen,
            SUM(pnl.fee_net_usd) AS fee_neto,
            SUM(pnl.commission_net_usd) AS comision_neta,
            -SUM(pnl.discounts_net_usd) AS descuentos_neto,
            SUM(pnl.backend_air_usd) AS backend_air,
            SUM(pnl.backend_non_air_usd) AS backend_nonair,
            SUM(pnl.breakage_revenue_usd) AS breakage_revenue,
            SUM(pnl.media_revenue_usd) AS media_revenue,
            -SUM(pnl.ccp_usd) AS ccp,
            -SUM(pnl.coi_usd) AS coi,
            SUM(pnl.customer_service_usd) AS customer_service,
            SUM(pnl.errors_usd) AS errors,
            -SUM(pnl.loyalty_usd) AS loyalty,
            SUM(pnl.net_revenues_usd) AS net_revenues
        FROM analytics.bi_sales_fact_sales_recognition fv
        LEFT JOIN analytics.bi_pnlop_fact_current_model pnl
            ON fv.product_id = pnl.product_id AND pnl.date_reservation_year_month > '2023-01'
        LEFT JOIN analytics.bi_transactional_fact_charges c
            ON fv.product_id = c.product_id AND c.reservation_year_month >= DATE '2023-01-01'
        WHERE fv.recognition_date >= DATE '2023-01-01'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
    )
    SELECT
        recognition_date,
        brand,
        site,
        pais_corregido,
        parent_channel_metas,
        channel_metas,
        buy_type,
        gb_RI,
        fix_net_revenues,
        npv
    FROM bt_detail
    WHERE parent_channel IN ('API', 'Agencias afiliadas')