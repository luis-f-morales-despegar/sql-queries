# Explicación de la consulta Trino para segmentación B2B

## Resumen general
La consulta calcula métricas de ventas, tráfico y comportamiento para agencias/grupos B2B con el objetivo de clasificarlos en segmentos de actividad (NEW, RETURNING, ZOMBIE, etc.). El resultado final entrega un registro por `group_code` con KPIs recientes (GB, órdenes, búsquedas) y banderas de segmentación basadas en tendencias de los últimos meses.

## Fuentes de datos principales
- `analytics.bi_sales_fact_sales_recognition`: hechos de reconocimiento de ventas (`gestion_date`, `gestion_gb`, `parent_channel`, etc.).
- `analytics.bi_pnlop_fact_current_model`: información financiera (ingresos netos) filtrada para línea de negocio `B2B`.
- `raw.cartera_b2b_v1`: catálogo de agencias y su `group_code` actualizado al día anterior; se usa para mapear agencias individuales a grupos.
- `lake.channels_bo_product` y `lake.channels_bo_sale`: datos de comisión (`tpc_usd`) para ajustar ingresos de ciertas combinaciones de país/canal.
- `lake.bi_web_traffic` y `analytics.b2b_fact_look_to_book`: métricas de tráfico/búsquedas (`Searchers`, `bookings`) para los últimos 3 meses.

## CTE `a`: métricas por grupo
1. **Base de ventas (`s`)**
   - Agrupa por `gestion_date`, `parent_channel`, `productooriginal` (categoriza productos fuera de la lista en "Otros") y `group_code`.
   - Calcula GB (gross bookings) ponderado por `confirmation_gradient` y un ingreso neto (`nr`) ajustado por comisiones según canal/país.
   - Cuenta órdenes (`orders`).
   - Filtra fechas desde 2024-01-01 hasta el día anterior, solo para línea de negocio B2B.
   - Mapea agencias a grupos usando `cartera_b2b_v1` (con casos especiales para Expedia).

2. **Rango de primeras y últimas ventas (`sm`)**
   - Determina `first_sale` y `last_sale` por `group_code` considerando todas las ventas B2B disponibles hasta el día anterior.

3. **Tráfico (`tr`)**
   - Unifica métricas de tráfico web (`bi_web_traffic`) y look-to-book (`b2b_fact_look_to_book`).
   - Agrega búsquedas (`Searchers`), reservas (`bookings`) y otras métricas por fecha y `group_code` para los últimos 3 meses.

4. **Agregaciones finales en `a`**
   - Une `s`, `sm`, `tr` por `group_code` y fecha para calcular múltiples KPIs:
     - Ventas recientes en distintos horizontes: `gb_mtd`, `gb_L30D`, `gb_L90D`, `gb_L180D`.
     - Ventas por categoría (`gb_vuelos_l90d`, `gb_hoteles_l90d`, etc.).
     - Banderas como `new_agency_flg` (venta solo en mes actual tras 6 meses sin ventas).
     - Métricas de tráfico: `searchs_mtd`, `searchs_l3m` y órdenes (`orders_mtd`, `orders_l90d`).
     - Cómputo de `month_since_f_sale` (meses desde la primera venta hasta el mes actual).

## CTE `b`: intermitencia de ventas
- Extrae fechas distintas de ventas por `group_code` en los últimos 365 días.
- Calcula la diferencia de días entre ventas consecutivas (lag) y promedia (`intermitencia_promedio_dias`).
- Clasifica posteriormente esa intermitencia en categorías (semanal, mensual, etc.).

## Selección final
La cláusula `SELECT` final combina `a` y `b` para entregar, por cada `group_code`:

- **Diagnósticos de tendencia**:
  - `dbg_gb_L30D`: GB últimos 30 días.
  - `venta_entre_dia_90_y_31`: GB entre días -90 y -31 (baseline).
  - `dbg_trend_ratio`: relación entre el GB reciente y la mitad del baseline para medir tendencia.

- **Segmentación de actividad** (banderas y prioridad):
  - `flag_NEW`, `flag_RETURNING`, `flag_ZOMBIE`, `flag_RISING`, `flag_FALLING`, `flag_STABLE`, `flag_INACTIVE` basados en meses desde la primera venta, tráfico y ratio de tendencia.
  - `active_segment_priority` y `active_segment` asignan la categoría final.

- **Antigüedad y recurrencia**:
  - `month_since_f_sale` y `segmento_meses_antiguedad` agrupan por tramos de meses.
  - `intermitencia_promedio_dias` y `segmento_intermitencia` clasifican la frecuencia promedio entre ventas.

- **Mix de productos y ASP**:
  - Ventas por categoría y sus participaciones (`share_*`).
  - `product_segment` identifica si el grupo vende un solo tipo de producto o está diversificado.
  - `asp` y `segmento_asp` calculan el ticket promedio (`gb / órdenes`).

- **KPIs adicionales**: `gb_mtd`, `gb_ytd`, comparativos con el año anterior (`gb_mtd_ly`, `gb_ytd_ly`), búsquedas (`searchs_mtd`, `searchs_l3m`), órdenes (`orders_mtd`) y la fecha de partición (`partition_date`).

## Diagnóstico de sobreconteo (ejemplo `AG00008903`)

Para la agencia/agrupador `AG00008903` se observó que métricas como `gb_mtd` aparecen infladas respecto de los valores esperados. El problema proviene de la forma en que se unen las fuentes de ventas (`s`) con las de tráfico (`tr`).

1. **Grano de la CTE `s`**. La subconsulta agrega por `gestion_date`, `parent_channel`, `productooriginal` y `group_code`. Así, un mismo día puede producir múltiples filas para el mismo `group_code` (una por combinación de canal y producto).
2. **Grano de la CTE `tr`**. Aunque cada bloque interno agrega por `group_code` y fecha, el `UNION` mezcla dos fuentes distintas (tráfico web y look-to-book). En días donde existan datos en ambas fuentes, el `UNION` mantiene **dos** registros diferentes para la misma fecha y `group_code`.
3. **Join y agregación final**. Al unir `s` con `tr` por `group_code` y fecha, cada fila de `s` se duplica tantas veces como filas existan en `tr` para ese día. Como la agregación final vuelve a sumar `s.gb`, los valores de `gb_*` se multiplican por el número de filas provenientes de `tr`. Esto explica el sobreconteo observado en `gb_mtd` y el resto de métricas basadas en ventas para `AG00008903` (y cualquier otro grupo con tráfico en ambas fuentes).

### Próximos pasos sugeridos

- Cambiar el `UNION` por `UNION ALL` y agregar una segunda agregación externa que consolide la suma de `Searchers`/`bookings` por `group_code` y fecha, evitando duplicar las filas de ventas.
- Alternativamente, mover el join con `tr` a otra CTE y agregar primero todas las métricas de ventas (`s`) antes de unirlas con el tráfico, asegurando que el grano esté alineado.

## Uso potencial
Esta vista permite al equipo comercial monitorear el estado de cada agencia/grupo, detectar comportamientos (nuevos, en caída, en recuperación) y priorizar acciones comerciales basadas en actividad reciente, mezcla de productos y frecuencia de compras.

## Query corregida

La consulta actualizada que consolida las métricas de tráfico antes de unirlas con las ventas para evitar el sobreconteo se encuentra en `analysis/trino_b2b_segmentacion_fixed.sql`.

Principales ajustes realizados:
- El bloque de tráfico (`tr`) ahora usa `UNION ALL` seguido de una agregación externa (`GROUP BY group_code, fecha`) para garantizar un único registro por fecha y `group_code`, evitando duplicar las filas de ventas al hacer el join.
- Se mantiene el resto de la lógica de ventas y segmentación, por lo que los KPIs y banderas conservan su definición original.

Puedes ejecutar ese SQL directamente en Trino para validar que métricas como `gb_mtd` ya no se multipliquen al existir tráfico tanto en `bi_web_traffic` como en `b2b_fact_look_to_book`.

