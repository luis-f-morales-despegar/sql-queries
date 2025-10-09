--- COI MX 3 agregando Banco y Tarjeta + coi.usd + coi_interest
--- Eliminamos financial_result de coi_net
--- Falta agregar compras por metodo de pago "Créditos"? R: No aplica ya que no produce COI

SELECT 
    --count(DISTINCT t.transaction_code)
    --, count(*)
    --, sum(t.gb_net)
    t.country_code,
    t.transaction_code,
    t.product_line as product,
    t.trip_type,
    t.flight_validatin_carrier,
    card_bank,
    card_brand,
    type,
    channel_id,
    merchant_ownership,
    merchant_id,
    t.booking_month AS month,
    t.booking_week AS week,
    SUM(t.cuotas_indiv) AS cuotas,
    ROUND(SUM(t.cuotas_prom / t.code_count)) AS cuotas_prom,
    SUM(t.orders) AS orders,
    SUM(t.gb) AS gb,
    SUM(t.gb_net) AS gb_grad,
    SUM(t.tpc_rs) AS tpc_rs,
    SUM(t.net_rm_sin_tpc) AS net_rm_sin_tpc,
    SUM(t.coi_usd) as coi_usd,
    SUM(t.coi_interest_usd) as coi_interest_usd,
    SUM(t.coi_net) AS coi_net,
    SUM(t.ccp) AS ccp,
    SUM(t.other_revenue) AS other_revenue,
    SUM(t.other_costs) AS other_costs,
    SUM(t.npv) AS npv
FROM (
    SELECT
        s.country_code,
        s.transaction_code,
        s.buy_type_code AS product_line,
        p.trip_type,
        p.flight_validatin_carrier,
        cob.card_brand,
        cob.type,
        cob.channel_id,
        cob.merchant_ownership,
        cob.merchant_id,
        cob.card_bank,
        YEAR(m.date_reservation) AS booking_year,
        MONTH(m.date_reservation) AS booking_month,
        WEEK(m.date_reservation) AS booking_week,
        m.date_reservation AS date,
        count(s.transaction_code) as transacciones,
SUM(total_registros)  AS registros,
        COUNT(DISTINCT s.transaction_code) * SUM(cob.amount) / SUM(cob.total_transaction_amount) AS orders,
        SUM(s.gestion_gb * cob.amount / cob.total_transaction_amount) AS gb,
        SUM(s.gestion_gb * s.confirmation_gradient * cob.amount / cob.total_transaction_amount) AS gb_net,
        SUM(x.tpc_usd * cob.amount / cob.total_transaction_amount) AS tpc_rs,
        SUM(((m.fee_net_usd + m.commission_net_usd - m.discounts_net_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2))) - x.tpc_usd) * cob.amount / cob.total_transaction_amount) AS net_rm_sin_tpc,
        SUM(cob.installments) AS cuotas,
        SUM(cob.amount) AS cant_cuota,
        SUM(-m.coi_usd) as coi_usd,
        SUM(coi_interest_usd) as coi_interest_usd,
        SUM(((-m.coi_usd + coi_interest_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2)))) * cob.amount / cob.total_transaction_amount) AS coi_net,
        SUM(((-m.ccp_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2)))) * cob.amount / cob.total_transaction_amount) AS ccp,
        SUM((m.backend_air_usd + m.backend_non_air_usd + m.other_incentives_air_usd + m.other_incentives_non_air_usd + m.breakage_revenue_usd + m.media_revenue_usd + m.discounts_mkt_funds_usd) * cob.amount / cob.total_transaction_amount) AS other_revenue,
        SUM((m.customer_service_usd + m.errors_usd + m.frauds_usd + m.loyalty_usd + m.ott_usd + m.revenue_taxes_usd - m.cancellations_usd + m.customer_claims_usd - m.vendor_commission_usd + m.mkt_cost_net_usd) * cob.amount / cob.total_transaction_amount) AS other_costs,
        SUM(((m.margin_net_usd + m.variable_charges_without_mkt_usd + m.financial_result_usd + m.affiliates_usd) / IF(m.b2b_gradient_margin = '1', 1, CAST(m.b2b_gradient_margin AS DECIMAL(2,2))) - COALESCE(x.tpc_usd, 0)) * cob.amount / cob.total_transaction_amount) AS npv,
        MAX(cob.installments) AS cuotas_indiv,
        SUM(cob.total_cuotas / cob.total_registros) AS cuotas_prom,
        COUNT(s.transaction_code) AS code_count
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
                card_brand,
                card_bank,
                type,
                channel_id,
                merchant_ownership,
                merchant_id,
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
        AND s.country_code in ('MX')
        AND YEAR(s.booking_date) >= 2024
        --AND MONTH(s.booking_date) >= 1
        AND s.booking_date < CURRENT_DATE
        AND IF(s.buy_type_code IN ('Hoteles', 'Alquileres'), 'Confirmado', COALESCE(cs.product_state, s.product_status)) = 'Confirmado'
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
) t
WHERE
    t.orders > 0
   -- and t.transaction_code = 47529282100
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
--ORDER BY 1







--------------

select * 
from data.lake.tywin_penalty_breakdown
where 1=1 
limit 100

---
select * 
from data.analytics.bi_transactional_fact_charges  
where 1=1
and reservation_year_month is not null
and financed_interest > 0
limit 100
