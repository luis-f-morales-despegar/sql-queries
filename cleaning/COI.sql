------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------

 --- COI Brasil ---

SELECT 
    --count(DISTINCT t.transaction_code)
    --, count(*)
    --, sum(t.gb_net)
t.transaction_code,
    t.product_line as product,
    t.trip_type,
    t.booking_month AS month,
    t.booking_week AS week,
    SUM(t.orders) AS orders,
    SUM(t.gb) AS gb_gestion,
    SUM(t.tpc_rs) AS tpc_rs,
    SUM(t.net_rm_sin_tpc) AS net_rm_sin_tpc,
    ROUND(SUM(t.cuotas_prom / t.code_count)) AS cuotas_prom,
    SUM(t.coi_net) AS coi_net,
    SUM(t.ccp) AS ccp,
    SUM(t.other_revenue) AS other_revenue,
    SUM(t.other_costs) AS other_costs,
    SUM(t.npv) AS npv
FROM (
    SELECT
        s.transaction_code,
        s.buy_type_code AS product_line,
        p.trip_type,
        YEAR(m.date_reservation) AS booking_year,
        MONTH(m.date_reservation) AS booking_month,
        WEEK(m.date_reservation) AS booking_week,
        m.date_reservation AS date,
        count(s.transaction_code) as transacciones,
        SUM(total_registros)  AS registros,
        COUNT(DISTINCT s.transaction_code / total_registros) AS orders,
        SUM(s.gestion_gb / total_registros) AS gb,
        SUM(s.gestion_gb * s.confirmation_gradient / total_registros) AS gb_net,
        SUM(x.tpc_usd / total_registros) AS tpc_rs,
        SUM(((m.fee_net_usd + m.commission_net_usd - m.discounts_net_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2))) - x.tpc_usd) / total_registros) AS net_rm_sin_tpc,
        SUM(cob.installments) AS cuotas,
        SUM(cob.amount) AS cant_cuota,
        SUM(((-m.coi_usd + coi_interest_usd + m.financial_result_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2)))) / total_registros) AS coi_net,
        SUM(((-m.ccp_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2)))) / total_registros) AS ccp,
        SUM((m.backend_air_usd + m.backend_non_air_usd + m.other_incentives_air_usd + m.other_incentives_non_air_usd + m.breakage_revenue_usd + m.media_revenue_usd + m.discounts_mkt_funds_usd) / total_registros) AS other_revenue,
        SUM((m.customer_service_usd + m.errors_usd + m.frauds_usd + m.loyalty_usd + m.ott_usd + m.revenue_taxes_usd - m.cancellations_usd + m.customer_claims_usd - m.vendor_commission_usd + m.mkt_cost_net_usd) / total_registros) AS other_costs,
        SUM(((m.margin_net_usd + m.variable_charges_without_mkt_usd + m.financial_result_usd + m.affiliates_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2))) - COALESCE(x.tpc_usd, 0)) / total_registros) AS npv,
        SUM(cob.installments * cob.amount / cob.total_transaction_amount) AS cuotas_prom,
        COUNT(s.transaction_code) AS code_count,
        SUM(cob.installments) as inst,
        SUM(cob.amount) as am,
        SUM(cob.total_transaction_amount) as tot
FROM 
        analytics.bi_sales_fact_sales_recognition s
        INNER JOIN analytics.bi_pnlop_fact_current_model m 
            ON s.product_id = m.product_id
        LEFT JOIN analytics.bi_transactional_fact_products p 
            ON s.product_id = p.product_id
        LEFT JOIN data.analytics.bi_transactional_fact_charges l 
            ON p.product_id = l.product_id
        LEFT JOIN (
            SELECT transaction_id AS reference_id, MAX(net_commission_partner * conversion_rate) AS tpc_usd
            FROM data.lake.channels_bo_product
            GROUP BY 1) x 
            ON p.reference_id = x.reference_id
        LEFT JOIN analytics.bi_transactional_fact_products_current_state cs 
            ON s.product_id = cs.product_id
        INNER JOIN analytics.bi_transactional_fact_charges c 
            ON s.product_id = c.product_id
        LEFT JOIN (
            SELECT 
                transaction_code,
                amount,
                reservation_year_month,
                installments,
                count(collection_id) OVER (PARTITION BY transaction_code) AS total_registros,
                SUM(installments) OVER (PARTITION BY transaction_code) AS total_cuotas,
                SUM(amount) OVER (PARTITION BY transaction_code) AS total_transaction_amount
            FROM analytics.bi_transactional_fact_collections
            WHERE reservation_year_month >= DATE '2024-06-01'
            AND state = 'OK') cob 
            ON s.transaction_code = CAST(cob.transaction_code AS BIGINT)
    WHERE 
        s.partition_period >= '2023-01'
        AND m.date_reservation_year_month >= '2023-01-01'
        AND p.reservation_year_month >= CAST('2023-01-01' AS DATE)
        AND l.reservation_year_month >= CAST('2023-01-01' AS DATE)
        AND c.reservation_year_month >= DATE '2023-01-01'
        AND s.line_of_business_code = 'B2B'
        AND s.parent_channel = 'Agencias afiliadas'
        AND s.country_code = 'BR'
        AND YEAR(s.booking_date) >= 2024
        AND MONTH(s.booking_date) >= 6
        AND s.booking_date < CURRENT_DATE
        AND IF(s.buy_type_code IN ('Hoteles', 'Alquileres'), 'Confirmado', COALESCE(cs.product_state, s.product_status)) = 'Confirmado'
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7
) t
WHERE
    t.orders > 0
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1





------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------

 --- COI Mexico ---


SELECT 
    --count(DISTINCT t.transaction_code)
    --, count(*)
    --, sum(t.gb_net)
t.transaction_code,
    t.product_line as product,
    t.trip_type,
    t.booking_month AS month,
    t.booking_week AS week,
    SUM(t.orders) AS orders,
    SUM(t.gb) AS gb_gestion,
    SUM(t.tpc_rs) AS tpc_rs,
    SUM(t.net_rm_sin_tpc) AS net_rm_sin_tpc,
    ROUND(SUM(t.cuotas_prom / t.code_count)) AS cuotas_prom,
    SUM(t.coi_net) AS coi_net,
    SUM(t.ccp) AS ccp,
    SUM(t.other_revenue) AS other_revenue,
    SUM(t.other_costs) AS other_costs,
    SUM(t.npv) AS npv
FROM (
    SELECT
        s.transaction_code,
        s.country_code,
        s.buy_type_code AS product_line,
        p.trip_type,
        YEAR(m.date_reservation) AS booking_year,
        MONTH(m.date_reservation) AS booking_month,
        WEEK(m.date_reservation) AS booking_week,
        m.date_reservation AS date,
        count(s.transaction_code) as transacciones,
        SUM(total_registros)  AS registros,
        COUNT(DISTINCT s.transaction_code / total_registros) AS orders,
        SUM(s.gestion_gb / total_registros) AS gb,
        SUM(s.gestion_gb * s.confirmation_gradient / total_registros) AS gb_net,
        SUM(x.tpc_usd / total_registros) AS tpc_rs,
        SUM(((m.fee_net_usd + m.commission_net_usd - m.discounts_net_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2))) - x.tpc_usd) / total_registros) AS net_rm_sin_tpc,
        SUM(cob.installments) AS cuotas,
        SUM(cob.amount) AS cant_cuota,
        SUM(((-m.coi_usd + coi_interest_usd + m.financial_result_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2)))) / total_registros) AS coi_net,
        SUM(((-m.ccp_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2)))) / total_registros) AS ccp,
        SUM((m.backend_air_usd + m.backend_non_air_usd + m.other_incentives_air_usd + m.other_incentives_non_air_usd + m.breakage_revenue_usd + m.media_revenue_usd + m.discounts_mkt_funds_usd) / total_registros) AS other_revenue,
        SUM((m.customer_service_usd + m.errors_usd + m.frauds_usd + m.loyalty_usd + m.ott_usd + m.revenue_taxes_usd - m.cancellations_usd + m.customer_claims_usd - m.vendor_commission_usd + m.mkt_cost_net_usd) / total_registros) AS other_costs,
        SUM(((m.margin_net_usd + m.variable_charges_without_mkt_usd + m.financial_result_usd + m.affiliates_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2))) - COALESCE(x.tpc_usd, 0)) / total_registros) AS npv,
        SUM(cob.installments * cob.amount / cob.total_transaction_amount) AS cuotas_prom,
        COUNT(s.transaction_code) AS code_count,
        SUM(cob.installments) as inst,
        SUM(cob.amount) as am,
        SUM(cob.total_transaction_amount) as tot
FROM 
        analytics.bi_sales_fact_sales_recognition s
        INNER JOIN analytics.bi_pnlop_fact_current_model m 
            ON s.product_id = m.product_id
        LEFT JOIN analytics.bi_transactional_fact_products p 
            ON s.product_id = p.product_id
        LEFT JOIN data.analytics.bi_transactional_fact_charges l 
            ON p.product_id = l.product_id
        LEFT JOIN (
            SELECT transaction_id AS reference_id, MAX(net_commission_partner * conversion_rate) AS tpc_usd
            FROM data.lake.channels_bo_product
            GROUP BY 1) x 
            ON p.reference_id = x.reference_id
        LEFT JOIN analytics.bi_transactional_fact_products_current_state cs 
            ON s.product_id = cs.product_id
        INNER JOIN analytics.bi_transactional_fact_charges c 
            ON s.product_id = c.product_id
        LEFT JOIN (
            SELECT 
                transaction_code,
                amount,
                reservation_year_month,
                installments,
                count(collection_id) OVER (PARTITION BY transaction_code) AS total_registros,
                SUM(installments) OVER (PARTITION BY transaction_code) AS total_cuotas,
                SUM(amount) OVER (PARTITION BY transaction_code) AS total_transaction_amount
            FROM analytics.bi_transactional_fact_collections
            WHERE reservation_year_month >= DATE '2024-06-01'
            AND state = 'OK') cob 
            ON s.transaction_code = CAST(cob.transaction_code AS BIGINT)
    WHERE 
        s.partition_period >= '2023-01'
        AND m.date_reservation_year_month >= '2023-01-01'
        AND p.reservation_year_month >= CAST('2023-01-01' AS DATE)
        AND l.reservation_year_month >= CAST('2023-01-01' AS DATE)
        AND c.reservation_year_month >= DATE '2023-01-01'
        AND s.line_of_business_code = 'B2B'
        AND s.parent_channel = 'Agencias afiliadas'
        AND s.country_code = 'MX'                       ------------------------ PAIS
        AND YEAR(s.booking_date) >= 2024
        AND MONTH(s.booking_date) >= 6
        AND s.booking_date < CURRENT_DATE
        AND IF(s.buy_type_code IN ('Hoteles', 'Alquileres'), 'Confirmado', COALESCE(cs.product_state, s.product_status)) = 'Confirmado'
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8
) t
WHERE
    t.orders > 0
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1





