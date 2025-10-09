WITH a AS (
  SELECT
    s.group_code,
    MIN(sm.first_sale) AS first_sale,
    MAX(sm.last_sale) AS last_sale,
    MAX((year(date_add('day', -1, current_date)) * 12 + month(date_add('day', -1, current_date))) - (year(sm.first_sale) * 12 + month(sm.first_sale))) AS month_since_f_sale,
    CASE
      WHEN COALESCE(SUM(CASE WHEN s.gestion_date > CAST(date_trunc('month', date_add('day', -1, current_date)) AS DATE) AND s.gestion_date <= date_add('day', -1, current_date) THEN s.gb ELSE 0 END), 0) > 0
       AND COALESCE(SUM(CASE WHEN s.gestion_date > CAST(date_trunc('month', date_add('month', -6, current_date)) AS DATE) AND s.gestion_date < CAST(date_trunc('month', date_add('day', -1, current_date)) AS DATE) THEN s.gb ELSE 0 END), 0) = 0
      THEN 1 ELSE 0
    END AS new_agency_flg,
    COALESCE(SUM(CASE WHEN s.gestion_date >= CAST(date_trunc('month', date_add('day', -1, current_date)) AS DATE) AND s.gestion_date <= date_add('day', -1, current_date) THEN s.gb ELSE 0 END), 0) AS gb_mtd,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -180, current_date) AND s.gestion_date <= date_add('day', -1, current_date) THEN s.gb ELSE 0 END), 0) AS gb_L180D,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -90, current_date) AND s.gestion_date <= date_add('day', -1, current_date) THEN s.gb ELSE 0 END), 0) AS gb_L90D,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -31, current_date) THEN s.gb ELSE 0 END), 0) AS gb_L30D,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -91, current_date) AND s.gestion_date < date_add('day', -31, current_date) THEN s.gb ELSE 0 END), 0) AS venta_entre_dia_90_y_31,
    COALESCE(SUM(CASE WHEN s.gestion_date >= CAST(date_trunc('month', date_add('month', -12, date_add('day', -1, current_date))) AS DATE) AND s.gestion_date <= date_add('month', -12, date_add('day', -1, current_date)) THEN s.gb ELSE 0 END), 0) AS gb_mtd_ly,
    COALESCE(SUM(CASE WHEN s.gestion_date >= CAST(date_trunc('year', current_date) AS DATE) THEN s.gb ELSE 0 END), 0) AS gb_ytd,
    COALESCE(SUM(CASE
      WHEN s.gestion_date >= CAST(date_trunc('year', date_add('year', -1, current_date)) AS DATE)
       AND s.gestion_date < CAST(date_trunc('year', current_date) AS DATE)
       AND s.gestion_date < date_add('day', date_diff('day', CAST(date_trunc('year', current_date) AS DATE), current_date), date_add('year', -1, current_date))
      THEN s.gb ELSE 0
    END), 0) AS gb_ytd_ly,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -90, current_date) AND s.gestion_date <= date_add('day', -1, current_date) AND s.productooriginal = 'Actividades' THEN s.gb ELSE 0 END), 0) AS gb_actividades_l90d,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -90, current_date) AND s.gestion_date <= date_add('day', -1, current_date) AND s.productooriginal = 'Hoteles' THEN s.gb ELSE 0 END), 0) AS gb_hoteles_l90d,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -90, current_date) AND s.gestion_date <= date_add('day', -1, current_date) AND s.productooriginal = 'Vuelos' THEN s.gb ELSE 0 END), 0) AS gb_vuelos_l90d,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -90, current_date) AND s.gestion_date <= date_add('day', -1, current_date) AND s.productooriginal = 'Carrito' THEN s.gb ELSE 0 END), 0) AS gb_paquetes_l90d,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -90, current_date) AND s.gestion_date <= date_add('day', -1, current_date) AND s.productooriginal = 'Otros' THEN s.gb ELSE 0 END), 0) AS gb_otros_l90d,
    COALESCE(SUM(CASE WHEN tr.fecha >= CAST(date_trunc('month', date_add('day', -1, current_date)) AS DATE) AND tr.fecha <= date_add('day', -1, current_date) THEN tr.Searchers ELSE 0 END), 0) AS searchs_mtd,
    COALESCE(COUNT(DISTINCT CASE WHEN s.gestion_date >= date_add('day', -90, current_date) AND s.gestion_date <= date_add('day', -1, current_date) THEN s.productooriginal END), 0) AS productos_l90d,
    COALESCE(SUM(tr.Searchers), 0) AS searchs_l3m,
    COALESCE(SUM(CASE WHEN s.gestion_date > CAST(date_trunc('month', date_add('day', -1, current_date)) AS DATE) AND s.gestion_date <= date_add('day', -1, current_date) THEN s.orders ELSE 0 END), 0) AS orders_mtd,
    COALESCE(SUM(CASE WHEN s.gestion_date >= date_add('day', -90, current_date) AND s.gestion_date <= date_add('day', -1, current_date) THEN s.orders ELSE 0 END), 0) AS orders_l90d,
    date_add('day', -1, current_date) AS partition_date
  FROM (
    SELECT
      gestion_date,
      fh.parent_channel,
      CASE WHEN fh.buy_type_code IN ('Carrito','Vuelos','Hoteles','Actividades') THEN fh.buy_type_code ELSE 'Otros' END AS productooriginal,
      group_code,
      ROUND(SUM(fh.gestion_gb * fh.confirmation_gradient), 2) AS gb,
      ROUND(SUM(CASE
        WHEN fh.country_code = 'BR' AND fh.product NOT IN ('Vuelos') THEN (pnl.net_revenues_usd - (bo.tpc_usd * CASE WHEN pnl.b2b_gradient_margin = '1' THEN 1 ELSE CAST(pnl.b2b_gradient_margin AS DOUBLE) END))
        WHEN fh.channel = 'expedia' THEN (pnl.net_revenues_usd - (bo.tpc_usd * CASE WHEN pnl.b2b_gradient_margin = '1' THEN 1 ELSE CAST(pnl.b2b_gradient_margin AS DOUBLE) END))
        ELSE pnl.net_revenues_usd
      END), 2) AS nr,
      COUNT(DISTINCT fh.product_id) AS orders
    FROM analytics.bi_sales_fact_sales_recognition fh
    LEFT JOIN analytics.bi_pnlop_fact_current_model pnl ON fh.product_id = pnl.product_id AND CAST(pnl.date_reservation_year_month AS VARCHAR) IS NOT NULL
    LEFT JOIN (
      SELECT 
        agency_code, 
        COALESCE(group_code, agency_code) AS group_code
      FROM raw.cartera_b2b_v1
      WHERE partition_date = date_add('day', -1, current_date)
    ) cr
      ON (
         (fh.channel = 'expedia' AND cr.agency_code = 'expedia')
      OR (fh.channel <> 'expedia' AND fh.partner_id = cr.agency_code)
    )
    LEFT JOIN (
      SELECT p.transaction_id AS product_id_original, MAX(p.net_commission_partner * p.conversion_rate) AS tpc_usd
      FROM lake.channels_bo_product p
      JOIN lake.channels_bo_sale s ON s.id = p.sale_id
      WHERE CAST(s.created AS DATE) >= DATE '2024-01-01' AND CAST(s.created AS DATE) < current_date
      GROUP BY p.transaction_id
    ) bo ON bo.product_id_original = fh.origin_product_id
    WHERE fh.gestion_date BETWEEN DATE '2024-01-01' AND date_add('day', -1, current_date)
      AND fh.partition_period >= '2024-01'
      AND fh.line_of_business_code = 'B2B'
      AND pnl.line_of_business = 'B2B'
    GROUP BY gestion_date, fh.parent_channel, fh.buy_type_code, group_code
  ) s
  LEFT JOIN (
    SELECT
      group_code,
      MIN(fh.gestion_date) AS first_sale,
      MAX(fh.gestion_date) AS last_sale
    FROM analytics.bi_sales_fact_sales_recognition fh
    LEFT JOIN analytics.bi_pnlop_fact_current_model pnl ON fh.product_id = pnl.product_id AND CAST(pnl.date_reservation_year_month AS VARCHAR) IS NOT NULL
    LEFT JOIN (
      SELECT 
        agency_code,
        COALESCE(group_code, agency_code) AS group_code
      FROM raw.cartera_b2b_v1
      WHERE partition_date = date_add('day', -1, current_date)
    ) cr
      ON lower(trim(cr.agency_code)) = lower(trim(
        CASE WHEN fh.channel = 'expedia' THEN 'expedia'
             ELSE fh.partner_id
        END
      ))
    WHERE CAST(fh.gestion_date AS DATE) <= date_add('day', -1, current_date)
      AND fh.partition_period IS NOT NULL
      AND pnl.date_reservation_year_month IS NOT NULL
      AND fh.line_of_business_code = 'B2B'
      AND pnl.line_of_business = 'B2B'
    GROUP BY group_code
  ) sm ON sm.group_code = s.group_code
  LEFT JOIN (
    SELECT
      group_code,
      fecha,
      SUM(visitantes) AS visitantes,
      SUM(hotel_dispo) AS hotel_dispo,
      SUM(hotel_request) AS hotel_request,
      SUM(Searchers) AS Searchers,
      SUM(bookings) AS bookings,
      SUM(bookings_errors) AS bookings_errors
    FROM (
      SELECT
        group_code,
        cvr.fecha AS fecha,
        0 AS visitantes,
        0 AS hotel_dispo,
        0 AS hotel_request,
        SUM(Searchers) AS Searchers,
        SUM(COALESCE(row_num, 0)) AS bookings,
        0 AS bookings_errors
      FROM (
        SELECT
          CAST(bwt.date AS DATE) AS fecha,
          group_code,
          'Afiliadas' AS parent_channel,
          CASE WHEN bwt.producto_fenix IN ('Carrito','Bundles','Escapadas') THEN 'Carrito' ELSE bwt.producto_fenix END AS original_product,
          bwt.producto_fenix AS product,
          bwt.transaction_code AS bookings_bi,
          CASE WHEN bwt.transaction_code IS NOT NULL THEN row_number() OVER (PARTITION BY bwt.transaction_code ORDER BY bwt.date) ELSE NULL END AS row_num,
          COUNT(DISTINCT CASE WHEN bwt.flow = 'SEARCH' THEN bwt.searchid END) AS Searchers,
          COUNT(DISTINCT CASE WHEN bwt.flow = 'DETAIL' THEN bwt.searchid END) AS search_detail,
          COUNT(DISTINCT CASE WHEN bwt.flow = 'CHECKOUT' THEN bwt.searchid END) AS search_checkouters
        FROM lake.bi_web_traffic bwt
        LEFT JOIN lake.ch_bo_partner_partner p ON bwt.partner_id = p.reference_id
        LEFT JOIN (
          SELECT agency_code, COALESCE(group_code, agency_code) AS group_code FROM raw.cartera_b2b_v1 WHERE partition_date = date_add('day', -1, current_date)
        ) cr ON bwt.partner_id = cr.agency_code
        WHERE CAST(bwt.date AS DATE) >= date_add('month', -3, current_date)
          AND bwt.ispageview = 1
          AND bwt.flg_detalle_cp = 0
          AND bwt.channel IN ('hoteldo-html-platinum','hoteldo-html-gold','hoteldo-html-silver','hoteldo-html-classic','travel-agency-bo','travel-agency-whitelabel')
        GROUP BY CAST(bwt.date AS DATE), group_code, bwt.transaction_code, bwt.producto_fenix, bwt.date
        HAVING COUNT(DISTINCT CASE WHEN bwt.flow = 'SEARCH' THEN bwt.searchid END) > 0
           OR COUNT(DISTINCT CASE WHEN bwt.flow = 'DETAIL' THEN bwt.searchid END) > 0
           OR COUNT(DISTINCT CASE WHEN bwt.flow = 'CHECKOUT' THEN bwt.searchid END) > 0
           OR bwt.transaction_code IS NOT NULL
      ) cvr
      WHERE row_num = 1 OR row_num IS NULL
      GROUP BY group_code, cvr.fecha

      UNION ALL

      SELECT
        group_code,
        l2b.hsm_date AS fecha,
        0 AS visitantes,
        SUM(l2b.hotel_ids_requested_with_availability) AS hotel_dispo,
        SUM(l2b.hotel_ids_requested) AS hotel_request,
        SUM(l2b.num_reqs_wrapper) AS Searchers,
        SUM(l2b.bookings_count) AS bookings,
        SUM(l2b.bookings_error) AS bookings_errors
      FROM analytics.b2b_fact_look_to_book l2b
      LEFT JOIN lake.ch_bo_partner_partner p ON p.partner_code = l2b.partner_id
      LEFT JOIN (
        SELECT agency_code, COALESCE(group_code, agency_code) AS group_code FROM raw.cartera_b2b_v1 WHERE partition_date = date_add('day', -1, current_date)
      ) cr ON l2b.partner_id = cr.agency_code
      WHERE CAST(l2b.hsm_date AS DATE) >= date_add('month', -3, current_date)
      GROUP BY group_code, l2b.hsm_date
    ) tr_union
    GROUP BY group_code, fecha
  ) tr ON tr.group_code = s.group_code AND s.gestion_date = tr.fecha
  GROUP BY s.group_code
),
b AS (
  SELECT
    group_code,
    ROUND(AVG(diff_date), 0) AS intermitencia_promedio_dias
  FROM (
    SELECT
      group_code,
      gestion_date,
      date_diff('day', lag(gestion_date) OVER (PARTITION BY group_code ORDER BY gestion_date), gestion_date) AS diff_date
    FROM (
      SELECT DISTINCT
        group_code,
        fh.gestion_date
      FROM analytics.bi_sales_fact_sales_recognition fh
      LEFT JOIN analytics.bi_pnlop_fact_current_model pnl ON fh.product_id = pnl.product_id AND CAST(pnl.date_reservation_year_month AS VARCHAR) IS NOT NULL
      LEFT JOIN (
        SELECT 
          agency_code,
          COALESCE(group_code, agency_code) AS group_code
        FROM raw.cartera_b2b_v1
        WHERE partition_date = date_add('day', -1, current_date)
      ) cr
        ON cr.agency_code = CASE 
                               WHEN fh.channel = 'expedia' THEN 'expedia'
                               ELSE fh.partner_id
                            END
      WHERE fh.gestion_date BETWEEN date_add('day', -365, current_date) AND date_add('day', -1, current_date)
        AND fh.partition_period >= '2024-01'
        AND fh.line_of_business_code = 'B2B'
        AND pnl.line_of_business = 'B2B'
        AND group_code IS NOT NULL
    ) t1
  ) t2
  WHERE diff_date IS NOT NULL
  GROUP BY group_code
)
SELECT
  a.group_code,
  a.gb_L30D AS dbg_gb_L30D,
  a.venta_entre_dia_90_y_31 AS dbg_gb_prev60,
  (a.venta_entre_dia_90_y_31 / 2.0) AS dbg_baseline_prev60_half,
  CASE WHEN (a.venta_entre_dia_90_y_31 / 2.0) > 0 THEN (a.gb_L30D / (a.venta_entre_dia_90_y_31 / 2.0)) END AS dbg_trend_ratio,
  CASE WHEN a.month_since_f_sale <= 3 THEN 1 ELSE 0 END AS flag_NEW,
  CASE WHEN a.searchs_l3m = 0 THEN 1 ELSE 0 END AS flag_INACTIVE,
  CASE WHEN a.searchs_l3m > 0 AND a.gb_L90D = 0 THEN 1 ELSE 0 END AS flag_ZOMBIE,
  CASE WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31 / 2.0, 0)) > 1.3 THEN 1 ELSE 0 END AS flag_RISING,
  CASE WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31 / 2.0, 0)) < 0.7 THEN 1 ELSE 0 END AS flag_FALLING,
  CASE WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31 / 2.0, 0)) BETWEEN 0.7 AND 1.3 THEN 1 ELSE 0 END AS flag_STABLE,
  CASE WHEN a.month_since_f_sale > 3 AND a.gb_L30D > 0 AND ABS(a.gb_L90D - a.gb_L30D) <= 0.001 THEN 1 ELSE 0 END AS flag_RETURNING,
  CASE
    WHEN a.month_since_f_sale <= 3 THEN 1
    WHEN a.month_since_f_sale > 3 AND a.gb_L30D > 0
         AND ABS(a.gb_L90D - a.gb_L30D) <= 0.001 THEN 2
    WHEN a.searchs_l3m > 0 AND a.gb_L90D = 0 THEN 3
    WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31/2.0,0)) < 0.7 THEN 4
    WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31/2.0,0)) BETWEEN 0.7 AND 1.3 THEN 5
    WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31/2.0,0)) > 1.3 THEN 6
    WHEN a.searchs_l3m = 0 THEN 7
    ELSE 98
  END AS active_segment_priority,
  CASE
    WHEN a.month_since_f_sale <= 3 THEN 'NEW'
    WHEN a.month_since_f_sale > 3 AND a.gb_L30D > 0 AND ABS(a.gb_L90D - a.gb_L30D) <= 0.001 THEN 'RETURNING'
    WHEN a.searchs_l3m > 0 AND a.gb_L90D = 0 THEN 'ZOMBIE'
    WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31/2.0,0)) < 0.7 THEN 'FALLING'
    WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31/2.0,0)) BETWEEN 0.7 AND 1.3 THEN 'STABLE'
    WHEN a.gb_L90D > 0 AND (a.gb_L30D / NULLIF(a.venta_entre_dia_90_y_31/2.0,0)) > 1.3 THEN 'RISING'
    WHEN a.searchs_l3m = 0 THEN 'INACTIVE'
    ELSE 'UNCLASSIFIED'
  END AS active_segment,
  a.first_sale,
  a.last_sale,
  a.month_since_f_sale,
  a.new_agency_flg,
  a.gb_mtd,
  a.gb_mtd_ly,
  a.gb_ytd,
  a.gb_ytd_ly,
  a.gb_L180D,
  a.gb_L90D,
  a.gb_L30D,
  a.venta_entre_dia_90_y_31,
  a.gb_vuelos_l90d,
  a.gb_hoteles_l90d,
  a.gb_actividades_l90d,
  a.gb_paquetes_l90d,
  a.gb_otros_l90d,
  COALESCE(a.gb_vuelos_l90d / NULLIF(a.gb_L90D, 0), 0) AS share_vuelos,
  COALESCE(a.gb_hoteles_l90d / NULLIF(a.gb_L90D, 0), 0) AS share_hoteles,
  COALESCE(a.gb_actividades_l90d / NULLIF(a.gb_L90D, 0), 0) AS share_actividades,
  COALESCE(a.gb_paquetes_l90d / NULLIF(a.gb_L90D, 0), 0) AS share_paquetes,
  COALESCE(a.gb_otros_l90d / NULLIF(a.gb_L90D, 0), 0) AS share_otros,
  CASE
    WHEN a.gb_L90D IS NULL OR a.gb_L90D = 0 THEN 'NO_GB'
    WHEN a.gb_hoteles_l90d = a.gb_L90D THEN 'SOLO_HOTELES'
    WHEN a.gb_actividades_l90d = a.gb_L90D THEN 'SOLO_ACTIVIDADES'
    WHEN a.gb_otros_l90d = a.gb_L90D THEN 'SOLO_OTROS'
    WHEN a.gb_vuelos_l90d = a.gb_L90D THEN 'SOLO_VUELOS'
    WHEN a.gb_paquetes_l90d = a.gb_L90D THEN 'SOLO_PAQUETES'
    WHEN a.productos_l90d > 3 THEN 'DIVERSIFICADA'
    ELSE 'SEMI_DIVERSIFICADA'
  END AS product_segment,
  a.productos_l90d,
  COALESCE(a.gb_L90D / NULLIF(a.orders_l90d, 0), 0) AS asp,
  a.orders_l90d AS asp_orders_den,
  CASE
    WHEN a.gb_L90D IS NULL OR a.orders_l90d IS NULL OR a.gb_L90D = 0 THEN 'NO_GB'
    WHEN a.gb_L90D / NULLIF(a.orders_l90d, 0) <= 224 THEN 'Muy Bajo'
    WHEN a.gb_L90D / NULLIF(a.orders_l90d, 0) > 224 AND a.gb_L90D / NULLIF(a.orders_l90d, 0) <= 352 THEN 'Bajo'
    WHEN a.gb_L90D / NULLIF(a.orders_l90d, 0) > 352 AND a.gb_L90D / NULLIF(a.orders_l90d, 0) <= 518 THEN 'Medio'
    WHEN a.gb_L90D / NULLIF(a.orders_l90d, 0) > 518 AND a.gb_L90D / NULLIF(a.orders_l90d, 0) <= 902 THEN 'Alto'
    ELSE 'Premium'
  END AS segmento_asp,
  CASE
    WHEN a.month_since_f_sale IS NULL THEN NULL
    WHEN a.month_since_f_sale < 3 THEN '0-3'
    WHEN a.month_since_f_sale < 6 THEN '3-6'
    WHEN a.month_since_f_sale < 12 THEN '6-12'
    WHEN a.month_since_f_sale < 24 THEN '12-24'
    WHEN a.month_since_f_sale < 48 THEN '24-48'
    ELSE '+48'
  END AS segmento_meses_antiguedad,
  b.intermitencia_promedio_dias,
  CASE
    WHEN b.intermitencia_promedio_dias IS NULL THEN 'no_data'
    WHEN b.intermitencia_promedio_dias < 8 THEN 'semanal'
    WHEN b.intermitencia_promedio_dias < 15 THEN 'quincenal'
    WHEN b.intermitencia_promedio_dias < 31 THEN 'mensual'
    WHEN b.intermitencia_promedio_dias < 61 THEN 'bimensual'
    WHEN b.intermitencia_promedio_dias < 91 THEN 'trimestral'
    ELSE 'trimestral+'
  END AS segmento_intermitencia,
  a.searchs_mtd,
  a.searchs_l3m,
  a.orders_mtd,
  a.partition_date
FROM a
LEFT JOIN b ON a.group_code = b.group_code
WHERE a.group_code IS NOT NULL;
