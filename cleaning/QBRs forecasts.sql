--- Query R por partner -> validar uso booking date o checkin date; mismo tema filtros


WITH bo_tpc AS (
  SELECT 
    p.transaction_id AS product_id_original,
    MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd,
    MAX(CAST(p.cancelled AS DATE)) AS cancelled,
    MAX(
      CASE 
        WHEN COALESCE(p.status, '') = '' THEN 'ACTIVE'
        ELSE p.status
      END
    ) AS bo_status
  FROM data.lake.channels_bo_product p
  JOIN data.lake.channels_bo_sale s
    ON s.id = p.sale_id
  WHERE CAST(s.created AS DATE) >= DATE '2023-01-01'
    AND CAST(s.created AS DATE) < CURRENT_DATE
  GROUP BY p.transaction_id
)
SELECT
 -- bo.cancelled,
 -- bo.bo_status,
CASE 
    WHEN ca.group_code IS NULL THEN ca.agency_code 
    ELSE ca.group_code 
END AS group_code,
CASE 
    WHEN ca.group_name IS NULL THEN ca.ag_name 
    ELSE ca.group_name 
END AS group_name,
  ca.director as director,
  fh.checkin_date as gestion_date,  -------------- filtro booking_date / checkin_date
  case when fh.parent_channel = 'API' then pnl.brand else 'Best Day' end as brand,
  case when pnl.line_of_business = 'B2B2C' then 'B2B2C'
 	else fh.parent_channel
  end as parent_channel,
  case 
  when fh.country_code in ('BR', 'CL', 'CO' ,'MX' , 'PE' ,'AR' ,'EC') then  fh.country_code
  when fh.country_code in ('US' ,'UY','PA') and fh.parent_channel IN ('API','Agencias afiliadas') then 'GL'
  else 'OT'
  end as country_code,
  case when fh.buy_type_code in ('Hoteles', 'Vuelos', 'Carrito', 'Traslados' ) then  fh.buy_type_code  else 'DS' end AS productooriginal,
--  fh.product_status as product_status,
 -- fh.product_is_confirmed_flg as product_is_confirmed_flg,
    ROUND(SUM(fh.gestion_gb), 2) AS gb, 
  --ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb_cgx
--   ,count(distinct bo.product_id_original) as bookings
  ROUND(
    SUM(
      CASE 
      WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') 
      THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                                      IF(pnl.b2b_gradient_margin = '1', 1, 
                                         CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
      WHEN fh.channel = 'expedia' 
      THEN (pnl.net_revenues_usd - (bo.tpc_usd * 
                                      IF(pnl.b2b_gradient_margin = '1', 1, 
                                         CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))))
      ELSE pnl.net_revenues_usd
      END
    )
    ,2) AS nr,
  --ROUND(SUM(pnl.npv_net_usd), 2) AS npv_net_usd,
  ROUND(
    SUM(
      (
        (pnl.margin_net_usd + pnl.variable_charges_without_mkt_usd + pnl.financial_result_usd 
         + pr.dif_fx_usd + pr.dif_fx_air_usd + pr.currency_hedge_usd + pr.currency_hedge_air_usd
         + pnl.affiliates_usd) -- Sumamos afiliadas
        / IF(pnl.b2b_gradient_margin = '1', 1, 
             CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5))) -- Quitar gradiente
      ) 
      - COALESCE(bo.tpc_usd,0) -- Quitar TPC (en sustitución de afiliadas)
    ) 
    * MAX(IF(pnl.b2b_gradient_margin = '1', 1, 
             CAST(pnl.b2b_gradient_margin AS DECIMAL(6,5)))) 
  ) AS fvm  
  FROM analytics.bi_sales_fact_sales_recognition fh 
  LEFT JOIN analytics.bi_pnlop_fact_current_model pnl 
  ON fh.product_id = pnl.product_id 
  AND pnl.date_reservation_year_month > '2022-12'
  LEFT JOIN analytics.bi_transactional_fact_charges c 
  ON fh.product_id = c.product_id 
  AND c.reservation_year_month > DATE '2022-12-31'
  LEFT JOIN data.analytics.bi_transactional_fact_products prod 
  ON fh.product_id = prod.product_id 
  AND prod.reservation_year_month > DATE '2022-12-31'
  LEFT JOIN analytics.bi_pnlop_fact_pricing_model pr 
  ON pr.product_id = fh.product_id 
  AND pr.date_reservation_year_month > '2022-12'
LEFT JOIN raw.cartera_b2b_v1 ca
    ON ca.agency_code = CASE
                          WHEN fh.channel = 'expedia' THEN 'expedia'
                          ELSE fh.partner_id
                        END
  LEFT JOIN bo_tpc bo 
  ON bo.product_id_original = fh.origin_product_id
  WHERE 
  fh.gestion_date > DATE('2022-12-31')
  AND partition_period > '2022-12'
--  AND month(fh.gestion_date) <= month(current_date)
   and fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND pnl.line_of_business = 'B2B'
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
   AND (
    fh.partner_id IN (
        'AP11809',
        'AP12822',
        'AP12226',
        'AP11683',
        'AP13048',
        'AP12956',
        'AP12955',
        'AP12877',
        'AP12876',
        'AP12875',
        'AP12874',
        'AP11761',
        'AP11759',
        'AP12622',
        'AP11760',
        'AP11676',
        'AP11674',
        'AP12908',
        'AP12907',
        'AP12910',
        'AP13154',
        'AP11697',
        'AP11699',
        'AP11698',
        'AP12678',
        'AP13002',
        'AP11651',
        'AP12679',
        'AP12408',
        'AP11686',
        'AP12769',
        'AP12770',
        'AP12771',
        'AP12772',
        'AP12691',
        'AP12692',
        'AP11744',
        'AP12380',
        'AP12557',
        'AP11810',
        'AP12718',
        'AP12236',
        'AP11615',
        'AP12549',
        'AG72472'
    )
    OR fh.channel = 'expedia'
)
    --  and ca.group_code = 'AG72472'
      and bo.bo_status = 'EMITTED'
    --  and gestion_date > DATE'2023-12-31'
      and fh.checkin_date > DATE('2022-12-31')
      and fh.checkin_date < current_Date
      GROUP BY 1,2,3,4,5,6,7,8
      order by 4 asc
      
      
      
      
       checkin_date
      
      select *
      FROM analytics.bi_sales_fact_sales_recognition fh 
      where partition_period is not null
      and line_of_business_code = 'B2B'
      and channel like '%expedia%'
      
      ----
      
      select *
      from raw.comdev_tendencias_partners_mensual