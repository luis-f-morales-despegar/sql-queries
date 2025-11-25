WITH agencias_mb AS (
    SELECT
        pp.name,
        pp.type,
        pp.reference_id,
        pp.status,
        IF(pp.country IN ('AR','CO','EC','PE','BR','CL','MX'), pp.country, 'OT') AS pais,
        pp.partner_code,
        DATE_FORMAT(TRY_CAST(pp.created AS TIMESTAMP), '%M %d, %Y') AS created,
        DATE_FORMAT(TRY_CAST(pp.last_modification_date AS TIMESTAMP), '%M %d, %Y') AS last_modification_date,
        pp.current_account_status,
        pp.business,
        CASE WHEN ag.name IS NULL THEN pp.name ELSE ag.name END AS nombre_final,
        ch.channel_name,
        ctr.conector
    FROM data.lake.ch_bo_partner_partner pp
    LEFT JOIN data.lake.ch_bo_partner_partner_intermediary pi ON pp.partner_intermediary_id = pi.id
    LEFT JOIN data.lake.ch_bo_partner_partner_joiner pj ON pj.attached = pp.id
    LEFT JOIN data.lake.ch_bo_partner_partner ag ON pj.joiner = ag.id
    LEFT JOIN data.lake.ch_bo_partner_channel ch ON pp.id = ch.id_partner
    LEFT JOIN data.raw.dim_table_hoteldo_api_partner_wrapper_conector ctr ON ctr.partner_code = pp.partner_code
    WHERE
        (pp.business IN ('hoteldo','hoteldo_affiliated')
         OR (pp.business = 'despegar' AND channel_name IN ('expedia','agency-pam-pp-ctrip')))
        AND conector LIKE '%TRAVELGATE%'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
),
ventas AS (
    SELECT
        fh.partner_id,
        fh.gestion_date AS Fecha_Emision,
        fh.recognition_date AS Fecha_Reconocimiento,
        fh.brand AS Marca,
        YEAR(fh.recognition_date) AS Anio,
        fh.line_of_business_code AS LOB,
        CASE
            WHEN fh.partner_id IN (
                'AP12142','AP12961','AP12767','AP12539','AP12792',
                'AP12149','AP12148','AG00015606','AP13029','AP13030',
                'AP13091','AP13104','AG00015611'
            ) THEN 'Paraguay'
            WHEN fh.country_code IN ('MX','BR','CO','AR','EC','PE','CL','UY') THEN CASE fh.country_code
                WHEN 'MX' THEN 'Mexico'
                WHEN 'BR' THEN 'Brasil'
                WHEN 'CO' THEN 'Colombia'
                WHEN 'AR' THEN 'Argentina'
                WHEN 'EC' THEN 'Ecuador'
                WHEN 'PE' THEN 'Peru'
                WHEN 'CL' THEN 'Chile'
                WHEN 'UY' THEN 'Uruguay'
            END
            ELSE 'Other Countries'
        END AS pais,
        CASE
            WHEN fh.buy_type_code = 'Actividades' THEN 'Dest. Serv.'
            WHEN fh.buy_type_code = 'Alquileres' THEN 'Vacation Rentals'
            WHEN fh.buy_type_code = 'Asistencia al viajero' THEN 'Insurance'
            WHEN fh.buy_type_code = 'Autos' THEN 'Cars'
            WHEN fh.buy_type_code = 'Carrito' THEN 'Packages General'
            WHEN fh.buy_type_code = 'Hoteles' THEN 'Hotels'
            WHEN fh.buy_type_code = 'Traslados' THEN 'Dest. Serv.'
            WHEN fh.buy_type_code = 'Vuelos' THEN 'Flights'
            WHEN fh.buy_type_code = 'Circuito' THEN 'Dest. Serv.'
            WHEN fh.buy_type_code = 'Servicios en Destino' THEN 'Dest. Serv.'
            ELSE fh.buy_type_code
        END AS productooriginal,
        fh.parent_channel AS parent_channel,
        CASE
            WHEN fh.trip_type_code = 'Nac' THEN 'Domestic'
            WHEN fh.trip_type_code = 'Int' THEN 'International'
            ELSE fh.trip_type_code
        END AS viaje,
        MAX(fh.confirmation_gradient) AS gradient,
        SUM(fh.gestion_gb) AS gross_bookings,
        COUNT(DISTINCT t.transaction_code) AS orders,
        SUM(pnl.commission_net_usd / fh.confirmation_gradient) AS up_front_incentives,
        SUM((pnl.fee_net_usd + pnl.coi_interest_usd) / fh.confirmation_gradient) AS fees,
        -SUM(pnl.discounts_net_usd / fh.confirmation_gradient) AS commercial_Discounts,
        -SUM(pnl.cancellations_usd / fh.confirmation_gradient) AS cancellations,
        SUM((pnl.backend_air_usd + pnl.backend_non_air_usd) / fh.confirmation_gradient) AS back_end_incentives,
        SUM(pnl.breakage_revenue_usd / fh.confirmation_gradient) AS breakage_revenue,
        SUM((pnl.discounts_mkt_funds_usd + pnl.media_revenue_usd - pnl.mkt_fee_cost_cmr_usd + pnl.fee_income_mkt_cmr_usd) / fh.confirmation_gradient) AS media_other_revenue,
        SUM((pnl.other_incentives_air_usd + pnl.other_incentives_non_air_usd) / fh.confirmation_gradient) AS other_incentives,
        -SUM(pnl.loyalty_usd / fh.confirmation_gradient) AS loyalty_usd,
        SUM(pnl.revenue_taxes_usd / fh.confirmation_gradient) AS revenue_taxes,
        -SUM(pnl.coi_usd / fh.confirmation_gradient) AS cost_of_Installments,
        -SUM(pnl.ccp_usd / fh.confirmation_gradient) AS credit_card_Processing,
        -SUM(pnl.affiliates_usd / fh.confirmation_gradient) AS affiliates,
        -SUM(pnl.revenue_sharing_usd / fh.confirmation_gradient) AS white_labels_api,
        -SUM(pnl.mkt_cost_net_usd / fh.confirmation_gradient) AS mkt_usd,
        SUM(pnl.frauds_usd / fh.confirmation_gradient) * 0 AS frauds,
        SUM(pnl.errors_usd / fh.confirmation_gradient) AS errors,
        SUM(pnl.ott_usd / fh.confirmation_gradient) AS other_transactional_taxes,
        SUM(pnl.customer_claims_usd / fh.confirmation_gradient) AS customer_claims,
        SUM(pnl.customer_service_usd / fh.confirmation_gradient) AS customer_service,
        -SUM(pnl.vendor_commission_usd / fh.confirmation_gradient) AS channels_expenses,
        SUM(fh.gestion_gb * 0) AS intercompany_USD,
        SUM(pnl.financial_result_usd / fh.confirmation_gradient) AS efecto_financiero,
        SUM((pnl.dif_fx_usd + pnl.dif_fx_air_usd) / fh.confirmation_gradient) AS dif_fx,
        SUM((pnl.currency_hedge_usd + pnl.currency_hedge_air_usd) / fh.confirmation_gradient) AS currency_hedge,
        SUM(pnl.net_revenues_usd / fh.confirmation_gradient) AS net_revenues,
        SUM(
            pnl.commission_net_usd / fh.confirmation_gradient +
            pnl.fee_net_usd / fh.confirmation_gradient -
            pnl.discounts_net_usd / fh.confirmation_gradient +
            pnl.mkt_discount_net_amount / fh.confirmation_gradient +
            pnl.coi_interest_usd / fh.confirmation_gradient -
            pnl.coi_usd / fh.confirmation_gradient -
            pnl.ccp_usd / fh.confirmation_gradient +
            pnl.mkt_cost_net_usd / fh.confirmation_gradient +
            pnl.other_incentives_air_usd / fh.confirmation_gradient +
            pnl.frauds_usd / fh.confirmation_gradient +
            pnl.errors_usd / fh.confirmation_gradient +
            pnl.revenue_taxes_usd / fh.confirmation_gradient +
            pnl.ott_usd / fh.confirmation_gradient +
            pnl.backend_air_usd / fh.confirmation_gradient +
            pnl.backend_non_air_usd / fh.confirmation_gradient -
            pnl.cancellations_usd / fh.confirmation_gradient +
            pnl.breakage_revenue_usd / fh.confirmation_gradient +
            pnl.customer_claims_usd / fh.confirmation_gradient +
            pnl.other_incentives_non_air_usd / fh.confirmation_gradient +
            pnl.customer_service_usd / fh.confirmation_gradient -
            pnl.affiliates_usd / fh.confirmation_gradient +
            pnl.discounts_mkt_funds_usd / fh.confirmation_gradient -
            pnl.vendor_commission_usd / fh.confirmation_gradient -
            pnl.revenue_sharing_usd / fh.confirmation_gradient +
            pnl.fee_income_mkt_cmr_usd / fh.confirmation_gradient +
            pnl.media_revenue_usd / fh.confirmation_gradient -
            pnl.mkt_fee_cost_cmr_usd / fh.confirmation_gradient +
            pnl.financial_result_usd / fh.confirmation_gradient +
            pnl.dif_fx_usd / fh.confirmation_gradient +
            pnl.dif_fx_air_usd / fh.confirmation_gradient +
            pnl.currency_hedge_usd / fh.confirmation_gradient +
            pnl.currency_hedge_air_usd / fh.confirmation_gradient
        ) AS NPV
    FROM analytics.bi_sales_fact_sales_recognition fh
    LEFT JOIN data.analytics.bi_pnlop_fact_current_model pnl ON fh.product_id = pnl.product_id
    LEFT JOIN data.analytics.bi_transactional_fact_transactions t ON t.transaction_code = CAST(pnl.transaction_code AS VARCHAR)
    LEFT JOIN data.tmp.correccion_be be ON CAST(be.product_id AS VARCHAR) = CAST(pnl.product_id AS VARCHAR)
    LEFT JOIN data.tmp.mktg_funds d ON CAST(d.product_id AS VARCHAR) = CAST(pnl.product_id AS VARCHAR)
    LEFT JOIN data.tmp.mkt_funds_bd1 mkt ON mkt.product_id = fh.product_id
    WHERE fh.recognition_date >= CAST('2025-11-01' AS DATE)
      AND fh.partition_period > '2024-01-01'
      AND fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND pnl.line_of_business = 'B2B'
      AND fh.parent_channel = 'API'
      AND t.reservation_year_month > CAST('2024-01-01' AS DATE)
      AND pnl.date_reservation_year_month > '2024-01'
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT
    v.*,
    ag.name AS agencia_nombre,
    ag.type AS agencia_tipo,
    ag.status AS agencia_status,
    ag.pais AS pais_agencia,
    ag.partner_code,
    ag.created AS agencia_created,
    ag.last_modification_date AS agencia_last_modification_date,
    ag.current_account_status,
    ag.business,
    ag.nombre_final,
    ag.channel_name,
    ag.conector
FROM ventas v
LEFT JOIN agencias_mb ag ON v.partner_id = ag.reference_id;
