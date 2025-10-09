--- Flujo actual 2025-09-10 
-- agencias con venta desde 2024 + agencias sin venta pero dadas de alta en 2025

WITH agencias AS (
    SELECT
        CASE
            WHEN channel.channel_name = 'expedia' THEN 'expedia'
            ELSE p.partner_code
        END AS agency_code,
        MAX(p.created)  AS created,
        MAX(gr.agency_group_code)  AS group_code,
        MAX(gr.agency_group_name)  AS group_name,
        MAX(
          CASE 
            WHEN channel.channel_name = 'expedia' THEN 'Expedia'
            ELSE COALESCE(
                    NULLIF(
                      TRIM( CASE WHEN p.name IS NULL THEN ag.name ELSE p.name END ),
                      ''
                    ),
                    NULLIF(TRIM(l.legal_name), ''),
                    p.name
                 )
          END
        ) AS ag_name,
        MAX(p.country)  AS agency_country,
        MAX(p.type)     AS agency_type,
        MAX(p.segment)  AS agency_segment,CASE
  WHEN MAX( CASE WHEN channel.channel_name = 'expedia' THEN 1 ELSE 0 END ) = 1
       THEN 'ACTIVE'                     
  ELSE MAX(p.status)                     
END AS status,
        MAX(kk.kam)     AS kam,
        MAX(kk.manager) AS manager,
        MAX(kk.director)AS director,
        MAX(kk.lead)    AS lead,
        MAX(kk.region)  AS region
    FROM data.lake.ch_bo_partner_partner p
    INNER JOIN data.lake.ch_bo_partner_channel channel ON p.id = channel.id_partner
    LEFT JOIN data.lake.ch_bo_partner_legal_info l ON l.partner_id = p.id
    LEFT JOIN raw.seed_b2b_kams k ON k.agency_code = CASE WHEN channel.channel_name = 'expedia' THEN 'expedia' ELSE p.partner_code END
    LEFT JOIN (SELECT * FROM raw.cartera_b2b_v1 WHERE partition_date = (SELECT MAX(partition_date) FROM raw.cartera_b2b_v1)) kk ON kk.agency_code = p.partner_code
    LEFT JOIN data.lake.ch_bo_partner_partner_joiner pj ON pj.attached = p.id
    LEFT JOIN data.lake.ch_bo_partner_partner ag ON pj.joiner = ag.id
    LEFT JOIN raw.b2b_dim_cluster_agencies gr ON gr.agency_code = p.partner_code
    WHERE channel.channel_name IN (SELECT cl.channel FROM raw.b2b_dim_channel_by_lob cl WHERE cl.lob IN ('B2B'))
    GROUP BY 1
),
fact_sales AS (
    SELECT
        CASE WHEN s.channel = 'expedia' THEN 'expedia' ELSE s.partner_id END AS agency_code,
        s.country_code,
        s.gestion_date,
        CASE WHEN COALESCE(s.parent_channel, cl.parent_channel) = 'Agencias afiliadas' THEN 'AGENCY' ELSE COALESCE(s.parent_channel, cl.parent_channel) END AS parent_channel,
        s.buy_type_code AS original_product,
        s.gestion_gb * s.confirmation_gradient AS gb
    FROM analytics.bi_sales_fact_sales_recognition s
    JOIN analytics.bi_pnlop_fact_current_model pnl ON s.product_id = pnl.product_id AND pnl.date_reservation_year_month >= '2024-01'
    JOIN analytics.bi_transactional_fact_transactions tx ON tx.transaction_code = CAST(s.transaction_code AS VARCHAR) AND tx.reservation_year_month_period >= '2024-01'
    JOIN raw.b2b_dim_channel_by_lob cl ON cl.channel = s.channel AND cl.lob = 'B2B'
    WHERE s.partition_period >= '2024-01'
      AND s.gestion_date >= DATE '2024-01-01'
      AND tx.reservation_year_month >= DATE '2023-01-01'
      AND s.line_of_business_code = 'B2B'
      AND pnl.line_of_business = 'B2B'
),
base AS (
    SELECT
        a.agency_code,                       -- 🔁 CHANGE: ahora partimos de AGENCIAS
        a.ag_name,
        a.status,
        CAST(a.created AS DATE) AS created,
        a.group_code,
        a.group_name,
        s.parent_channel,
        COALESCE(a.agency_country, s.country_code) AS country,
        COALESCE(a.kam,      'TBD') AS kam_name,
        COALESCE(a.manager,  'TBD') AS kam_manager,
        COALESCE(a.lead,     'TBD') AS kam_lead,
        COALESCE(a.director, 'TBD') AS director,
        COALESCE(a.region,   'TBD') AS region,
        ROUND(SUM(CASE
            WHEN s.gestion_date BETWEEN date_add('day', -31, current_date) AND date_add('day', -1, current_date)
                 THEN s.gb ELSE 0 END),2) AS gb_l30d,
        ROUND(SUM(CASE
            WHEN s.gestion_date >= date_trunc('year', current_date)
                 THEN s.gb ELSE 0 END),2) AS gb_ytd,
        ROUND(SUM(CASE
            WHEN s.gestion_date >= date_trunc('year', date_add('year', -1, current_date))
             AND s.gestion_date <  date_trunc('year', current_date)
                 THEN s.gb ELSE 0 END),2) AS gb_ytd_ly,
        ROUND(SUM(CASE
            WHEN s.gestion_date >= date_trunc('year', date_add('year', -1, current_date))
            AND s.gestion_date <  date_trunc('year', current_date)
                 THEN s.gb ELSE 0 END),2) AS gb_total_ly,
        MAX(s.gestion_date) AS last_sale
    FROM agencias a
    LEFT JOIN fact_sales s ON a.agency_code = s.agency_code   -- 🔁 CHANGE: LEFT JOIN para incluir agencias sin venta
    WHERE 1=1
 --   and a.status = 'ACTIVE' OR a.status IS NULL
    GROUP BY
        a.agency_code,a.ag_name,a.status,a.created,
        a.group_code,a.group_name,s.parent_channel,
        COALESCE(a.agency_country, s.country_code),
        COALESCE(a.kam, 'TBD'),COALESCE(a.manager, 'TBD'),
        COALESCE(a.lead, 'TBD'),COALESCE(a.director, 'TBD'),
        COALESCE(a.region, 'TBD')
),
base_ranked AS (
    SELECT b.*,ROW_NUMBER() OVER (PARTITION BY b.agency_code ORDER BY b.last_sale DESC NULLS LAST) AS rn
    FROM base b
)
SELECT
    agency_code,
    ag_name,
    group_code,
    group_name,
    parent_channel,
    country,
    region,
    director,
    kam_manager,
    kam_lead,
    kam_name,
    gb_l30d,
    gb_ytd,
    gb_ytd_ly,
    gb_total_ly,
    last_sale,
    status,
    created
FROM base_ranked
WHERE 1=1
and rn = 1  --- esto hace que se traiga el agency_code por parent channel con la venta más reciente
and agency_code is not null
and agency_code <> ''
and agency_code <> 'agency_code'
and upper(ag_name) not like '%TEST%'
--and ag_name like '%Dida%'
--AND ag_name IS null
--and agency_code in ('Expedia', 'expedia')
--and agency_code = 'AG00008165'
-- 🔁 CHANGE: incluir agencias sin venta creadas en 2025
AND (last_sale IS NOT NULL OR created >= DATE '2025-01-01')
 AND NOT (
      COALESCE(gb_total_ly, 0) = 0
      AND LOWER(COALESCE(status, '')) = 'disabled'
  )
--and gb_l30d = 0 and gb_ytd = 0 and gb_ytd_ly = 0
--order by created asc
