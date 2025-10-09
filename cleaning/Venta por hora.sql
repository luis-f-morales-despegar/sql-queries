select *
from analytics.pln_sales_fact_nrt_sales
where 1=1
and line_of_business = 'B2B'
--and partner_data_id = 'AP13048'
and reservation_Date >= date('2025-10-01	')
order by 3 desc
--limit 1000




	select *
from analytics.pln_sales_fact_nrt_sales
where 1=1
and line_of_business = 'B2B'
and partner_data_id = 'AP13048'
and reservation_Date >= date('2025-07-21')
order by 3 desc





select *
FROM data.analytics.bi_transactional_fact_products pr
where 1=1
and pr.reservation_year_month is not null
and product_type = 'Hoteles'

transaction_code
ir_refundable
status,
effective_rate
gateway,
hotel_penalty_Date


select *
FROM data.analytics.bi_transactional_fact_transactions tx
where 1=1
and tx.reservation_year_month is not null
--and product_type = 'Hoteles'

transaction_code, 
cancelation_date
status,
is_flexible_cancellation


select *
FROM data.analytics.bi_transactional_fact_charges ca
where 1=1
and ca.reservation_year_month is not null




select *
FROM data.analytics.bi_transactional_fact_products pr
JOIN data.analytics.bi_transactional_fact_transactions tx
  ON tx.transaction_code = pr.transaction_code
  AND tx.reservation_year_month > DATE('2025-04-30')
  and pr.reservation_year_month is not null
  
  select *
  from data.lake.bookedia_rsv_reservation brr 
  where 1=1
  and checkin_date > date('2025-05-01')
 
  
  id,
  transaction_id
  meal_plan,
  status,
  tr
  
  select *
  from data.lake.hrm_room_type
  
  hotel_oid,
  oid,
  real_name
  
  select *
from data.analytics.bi_hotel_dim_rate_plans


  select *
  from data.lake.bookedia_rsv_reservation brr 
  left join data.lake.hrm_room_type rt on rt.oid = brr.id
  where 1=1
  and brr.checkin_date > date('2025-05-01')
  
  
  
  
LEFT JOIN data.analytics.bi_transactional_fact_charges ca
  ON pr.product_id = ca.product_id
  AND ca.reservation_year_month > DATE('2025-04-30')
LEFT JOIN (
    SELECT DISTINCT country_code, continent
    FROM data.analytics.mkt_users_dim_cities
) ct
  ON ct.country_code = pr.destination_country_code
LEFT JOIN (
    SELECT 
        producto.product_id,
        COALESCE(bi_transactional_fact_products_current_state.product_state, producto.status) AS estado_producto
    FROM analytics.bi_transactional_fact_products AS producto
    LEFT JOIN analytics.bi_transactional_fact_products_current_state  
      ON producto.product_id = bi_transactional_fact_products_current_state.product_id
    WHERE producto.reservation_year_month > DATE('2025-04-30')
) sp
  ON pr.product_id = sp.product_id
LEFT JOIN data.analytics.bi_sales_fact_sales_recognition fv
  ON fv.product_id = pr.product_id
  AND fv.partition_period > '2023-12'
LEFT JOIN data.lake.ch_bo_partner_partner partner
  ON partner.partner_code = tx.partner_data_id
left join data.lake.bookedia_rsv_reservation brr 
  ON pr.product_id = concat('20', brr.transaction_id)
LEFT JOIN data.analytics.bi_pnlop_fact_current_model b
  ON b.product_id = pr.product_id
  AND b.date_reservation_year_month > '2025-04-30'
LEFT JOIN (
    SELECT DISTINCT 
        x.transaction_code,
        rm.rule_id,
        rm.rule_name,
        rm.closed_percentage_fee
    FROM data.analytics.bi_transactional_fact_transactions x
    INNER JOIN data.lake.chewie_reservation r
      ON x.transaction_code = r.id 
    INNER JOIN data.lake.chewie_product p
      ON r.oid = p.reservation_id
    INNER JOIN data.lake.chewie_product_revenue_input_margin rm
      ON p.oid = rm.product_id 
    WHERE x.reservation_year_month > DATE('2025-04-30')
) t
  ON tx.transaction_code = t.transaction_code
-- AÑADIMOS AQUÍ EL JOIN AL CTE bo_tpc
  LEFT JOIN bo_tpc bo
   ON bo.product_id_original = fv.origin_product_id
WHERE 1=1
  AND pr.reservation_year_month > DATE('2025-04-30')
  AND pr.reservation_year_month < CURRENT_DATE
  AND tx.reservation_date     > DATE('2025-04-30')
  AND tx.reservation_date     < CURRENT_DATE
  AND tx.parent_channel = 'API'
  --and bo.bo_status = 'EMITTED'
