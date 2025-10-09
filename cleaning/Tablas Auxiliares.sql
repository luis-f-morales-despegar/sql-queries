
---------------------------------------------------------------------------------------------------------------------------------------------
----- VARIOS ---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

---- Paises -----------------------------------------------------------------------


-----------------------------------------------------
------- Pais_Corregido ----------------------


WITH bt_detail AS (
    SELECT 
        CASE 
            WHEN fh.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 'AP12149', 'AP12148', 'AG00015606', 'AP13029', 'AP13030') THEN 'PY'
            WHEN fh.partner_id IN ('AG00017056', 'AP13049', 'AG00017054', 'AP13050') THEN 'UY'
            WHEN fh.partner_id IN ('AP12212', 'AP11666') THEN 'CR_CTA'
            WHEN fh.partner_id = 'AP12147' THEN 'SV_CTA'
            WHEN fh.partner_id = 'AP12854' THEN 'SV_CTA'
            WHEN fh.partner_id IN ('AP12509', 'AP11813') THEN 'GT_CTA'
            WHEN fh.partner_id = 'AP12158' THEN 'PA_CTA'
            WHEN fh.partner_id IN ('AP12213', 'AP11843') THEN 'HN_CTA'
            WHEN fh.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'DO_CTA'
            ELSE fh.country_code 
        END AS pais_corregido
    FROM analytics.bi_sales_fact_sales_recognition fh 
    WHERE fh.gestion_date >= DATE('2024-01-01')
      AND fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
      AND fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
      AND partition_period > '2021-01-01'
    GROUP BY 1
),
country_codes AS (
    SELECT DISTINCT 
         pais_corregido,
         CASE
            WHEN pais_corregido = 'MX' THEN 'MX'
            WHEN pais_corregido = 'BR' THEN 'BR'
            WHEN pais_corregido = 'CO' THEN 'CO'
            WHEN pais_corregido = 'AR' THEN 'AR'
            WHEN pais_corregido = 'EC' THEN 'EC'
            WHEN pais_corregido = 'PE' THEN 'PE'
            WHEN pais_corregido = 'CL' THEN 'CL'
            WHEN pais_corregido = 'PY' THEN 'OT'
            WHEN pais_corregido IN ('CR_CTA', 'SV_CTA', 'GT_CTA', 'PA_CTA', 'HN_CTA', 'DO_CTA') THEN 'OT'
            WHEN pais_corregido IN ('US', 'PA', 'ES', 'CR') THEN 'GL'
            WHEN pais_corregido IN ('UY', 'BO') THEN 'OT'
            ELSE 'OT'
         END AS country_code_corregido
    FROM bt_detail
)
SELECT 
    pais_corregido,
    country_code_corregido,
    CASE
       WHEN country_code_corregido = 'BR' THEN 'Brasil'
       WHEN country_code_corregido = 'OT' THEN 'Others'
       WHEN country_code_corregido = 'AR' THEN 'Argentina'
       WHEN country_code_corregido = 'MX' THEN 'Mexico'
       WHEN country_code_corregido = 'CO' THEN 'Colombia'
       WHEN country_code_corregido = 'CL' THEN 'Chile'
       WHEN country_code_corregido = 'GL' THEN 'USA/ROW'
       WHEN country_code_corregido = 'PE' THEN 'Peru'
       WHEN country_code_corregido = 'EC' THEN 'Ecuador'
       ELSE 'Others'
    END AS country_metas
FROM country_codes;




-----------------------------------------------------
------- Country_Code_Corregido ---------------------- * Usa country_code como dimTable, no pais_coregido

WITH bt_detail AS (
    SELECT 
        /* Corrección de país */
        CASE 
            WHEN fh.partner_id IN ('AP12142', 'AP12961', 'AP12767', 'AP12539', 'AP12792', 
                                     'AP12149', 'AP12148', 'AG00015606', 'AP13029', 'AP13030') THEN 'PY'
            WHEN fh.partner_id IN ('AG00017056', 'AP13049', 'AG00017054', 'AP13050') THEN 'UY'
            WHEN fh.partner_id IN ('AP12212', 'AP11666') THEN 'CR_CTA'
            WHEN fh.partner_id = 'AP12147' THEN 'SV_CTA'
            WHEN fh.partner_id = 'AP12854' THEN 'SV_CTA'
            WHEN fh.partner_id IN ('AP12509', 'AP11813') THEN 'GT_CTA'
            WHEN fh.partner_id = 'AP12158' THEN 'PA_CTA'
            WHEN fh.partner_id IN ('AP12213', 'AP11843') THEN 'HN_CTA'
            WHEN fh.partner_id IN ('AP12439', 'AP12438', 'AP12449', 'AP12805', 
                                   'AP12820', 'AP12900', 'AP12906', 'AP12896') THEN 'DO_CTA'
            ELSE fh.country_code 
        END AS pais_corregido
    FROM analytics.bi_sales_fact_sales_recognition fh 
    WHERE fh.gestion_date >= DATE('2024-01-01')
      AND fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
      AND fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
      AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
      AND partition_period > '2021-01-01'
    GROUP BY 1
),
table_country AS (
    SELECT DISTINCT
        pais_corregido,
        CASE
            WHEN pais_corregido = 'MX' THEN 'MX'
            WHEN pais_corregido = 'BR' THEN 'BR'
            WHEN pais_corregido = 'CO' THEN 'CO'
            WHEN pais_corregido = 'AR' THEN 'AR'
            WHEN pais_corregido = 'EC' THEN 'EC'
            WHEN pais_corregido = 'PE' THEN 'PE'
            WHEN pais_corregido = 'CL' THEN 'CL'
            WHEN pais_corregido = 'PY' THEN 'OT'
            WHEN pais_corregido IN ('CR_CTA', 'SV_CTA', 'GT_CTA', 'PA_CTA', 'HN_CTA', 'DO_CTA') THEN 'OT'
            WHEN pais_corregido IN ('US', 'PA', 'ES', 'CR') THEN 'GL'
            WHEN pais_corregido IN ('UY', 'BO') THEN 'OT'
            ELSE 'OT'
        END AS country_code_corregido
    FROM bt_detail
)
SELECT 
  --  pais_corregido,
   distinct country_code_corregido,
    CASE
        WHEN country_code_corregido = 'BR' THEN 'Brasil'
        WHEN country_code_corregido = 'OT' THEN 'Others'
        WHEN country_code_corregido = 'AR' THEN 'Argentina'
        WHEN country_code_corregido = 'MX' THEN 'Mexico'
        WHEN country_code_corregido = 'CO' THEN 'Colombia'
        WHEN country_code_corregido = 'CL' THEN 'Chile'
        WHEN country_code_corregido = 'GL' THEN 'USA/ROW'
        WHEN country_code_corregido = 'PE' THEN 'Peru'
        WHEN country_code_corregido = 'EC' THEN 'Ecuador'
        ELSE 'Others'
    END AS country_metas
FROM table_country;



------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------
----dimCountry_Code_Corregido *Se utiliza UY en vez de UY-BO

let
    // 1) Define your list of countries, now including country_metas
    CountryList = {
        [ country_code_corregido = "CL",    country_name = "Chile",         country_metas = "Chile" ],
        [ country_code_corregido = "CO",    country_name = "Colombia",      country_metas = "Colombia" ],
        [ country_code_corregido = "GL",    country_name = "Globales",      country_metas = "USA/ROW" ],
        [ country_code_corregido = "BR",    country_name = "Brasil",        country_metas = "Brasil" ],
        [ country_code_corregido = "AR",    country_name = "Argentina",     country_metas = "Argentina" ],
        [ country_code_corregido = "PE",    country_name = "Peru",          country_metas = "Peru" ],
        [ country_code_corregido = "MX",    country_name = "Mexico",        country_metas = "Mexico" ],
        [ country_code_corregido = "EC",    country_name = "Ecuador",       country_metas = "Ecuador" ],
        [ country_code_corregido = "PY",    country_name = "Paraguay",      country_metas = "Others" ],
        [ country_code_corregido = "UY", country_name = "Uruguay",       country_metas = "Others" ],
        [ country_code_corregido = "CTA",   country_name = "Centroamerica", country_metas = "Others" ]
    },

    // 2) Turn that list into a table
    Source = Table.FromRecords(CountryList),

    // 3) Ensure all three columns are text
    ChangedType = Table.TransformColumnTypes(
        Source,
        {
            {"country_code_corregido", type text},
            {"country_name",           type text},
            {"country_metas",          type text}
        }
    )
in
    ChangedType





----

select distinct pais 
from data.lake.dm_pnl_commercial_intelligence_model


---- Poductos -----------------------------------------------------------------------


-----------------------------------------------------
------- Producto Original ----------------------

SELECT 
    fh.buy_type_code AS productooriginal,
    CASE 
        WHEN fh.buy_type_code IN ('Hoteles', 'Vuelos', 'Carrito', 'Traslados')
            THEN fh.buy_type_code
        ELSE 'DS'
    END AS productos_DS,
    CASE 
        WHEN fh.buy_type_code = 'Vuelos' THEN 'Vuelos'
        WHEN fh.buy_type_code = 'Hoteles' THEN 'Hoteles'
        WHEN fh.buy_type_code = 'Carrito' THEN 'Carrito'
        ELSE 'ONA'
    END AS productos_ONA
FROM analytics.bi_sales_fact_sales_recognition fh 
WHERE fh.gestion_date >= DATE('2024-01-01')
  AND fh.gestion_date < CURRENT_DATE -- Excluye el día de hoy
  AND fh.lob_gestion IN ('stg__sales_b2bnohoteldo','stg_sales__b2bhoteldo')
  AND fh.channel NOT IN ('bestday-wl-mobile', 'affiliate-sicoob')
  AND partition_period > '2021-01-01'
GROUP BY 
    fh.buy_type_code;


    
-----------------------------------------------------
------- dimParent_Channel (2) ----------------------

let
    /* ---------- tabla original ---------- */
    Source =
        Table.FromRows(
            {
                {"API",  "API",                "API"},
                {"HTML", "Agencias afiliadas", "MIN"},
                {"API",  "API",                "MAY"}
            },
            {"API_HTML", "parent_channel", "channel_metas"}
        ),

    /* tipado explícito (opcional) */
    #"Changed Type" =
        Table.TransformColumnTypes(
            Source,
            { {"API_HTML",      type text},
              {"parent_channel", type text},
              {"channel_metas",  type text} }
        ),

    /* ---------- fila extra con AFF ---------- */
    #"Added AFF row" =
        Table.InsertRows(
            #"Changed Type",
            Table.RowCount(#"Changed Type"),
            {
                [ API_HTML = "HTML",
                  parent_channel = "Agencias afiliadas",
                  channel_metas  = "AFF" ]
            }
        ),

    /* ---------- listas con los nuevos valores ---------- */
    brandList          = { "Despegar", "Best Day", "Best Day", "Despegar" },
    channelReporteList = { "API D!", "HTML HDO", "API HDO", "AFF by HDO" },

    /* índice para alinear cada fila con su valor de lista */
    #"Added Index" =
        Table.AddIndexColumn(#"Added AFF row", "idx", 0, 1, Int64.Type),

    /* columna brand */
    #"Added brand" =
        Table.AddColumn(
            #"Added Index",
            "brand",
            each brandList{[idx]},
            type text
        ),

    /* columna channel_reporte */
    #"Added channel_reporte" =
        Table.AddColumn(
            #"Added brand",
            "channel_reporte",
            each channelReporteList{[idx]},
            type text
        ),

    /* quitar la columna de índice auxiliar */
    #"Removed idx" =
        Table.RemoveColumns(#"Added channel_reporte", {"idx"})
in
    #"Removed idx"
   


---------------------------------------------------------------------------------------------------------------------------------------------
----- CALENDARIOS ---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

---- Calendario_Venta -----------------------------------------------------------------------
---- dimBooking_Calendar ----------------*Por alguna razon trae una columna llamada "ISO YEAR" innecesaria que luego borra -------------------------------------------------

let
    // Definir el rango de fechas
    StartDate        = #date(2024, 1, 1),
    EndDate          = #date(2025, 12, 31),
    DaysList         = List.Dates(StartDate, Number.From(EndDate - StartDate) + 1, #duration(1,0,0,0)),
    CalendarTable    = Table.FromList(DaysList, Splitter.SplitByNothing(), {"Date"}),
    ChangedType      = Table.TransformColumnTypes(CalendarTable, {{"Date", type date}}),

    // Agregar columna de día de la semana donde lunes = 1
    WeekDayColumn    = Table.AddColumn(ChangedType, "Week Day", each Date.DayOfWeek([Date], Day.Monday) + 1, Int64.Type),

    // Extraer Año
    YearColumn       = Table.AddColumn(WeekDayColumn, "Year", each Date.Year([Date]), Int64.Type),

    // Formato Año-Mes YYYY-MM
    YearMonthColumn  = Table.AddColumn(YearColumn, "Year-Month", each
                          Text.PadStart(Text.From([Year]), 4, "0") & "-" &
                          Text.PadStart(Text.From(Date.Month([Date])), 2, "0"),
                          type text),

    // Formato Trimestre YYYY-QX (texto)
    QuarterColumn    = Table.AddColumn(YearMonthColumn, "Quarter", each
                          Text.PadStart(Text.From(Date.Year([Date])), 4, "0") & "-Q" &
                          Text.From(Date.QuarterOfYear([Date])),
                          type text),

    // Calcular Año ISO correctamente
    ISOYearColumn    = Table.AddColumn(QuarterColumn, "ISO Year", each
                          let
                              CurrentYear   = Date.Year([Date]),
                              WeekNumberRaw = Date.WeekOfYear([Date], Day.Monday),
                              ISOYear =
                                  if WeekNumberRaw = 1 and Date.Month([Date]) = 12 then CurrentYear + 1
                                  else if WeekNumberRaw >= 52 and Date.Month([Date]) = 1 then CurrentYear - 1
                                  else CurrentYear
                          in
                              ISOYear,
                          Int64.Type),

    // Calcular ISO Week texto "YYYY-WW"
    ISOWeekColumn    = Table.AddColumn(ISOYearColumn, "ISO Week", each
                          let
                              FirstThursday = Date.AddDays(#date([ISO Year], 1, 4), -Date.DayOfWeek(#date([ISO Year], 1, 4), Day.Monday)),
                              IWraw         = Number.RoundDown((Number.From([Date] - FirstThursday) / 7) + 1, 0),
                              IW            = if IWraw > 52 then 1 else IWraw
                          in
                              Text.PadStart(Text.From([ISO Year]), 4, "0") & "-" & Text.PadStart(Text.From(IW), 2, "0"),
                          type text),

    // Renombrar columnas base
    RenamedBase      = Table.RenameColumns(ISOWeekColumn, {
                          {"Date",       "Booking_Date"},
                          {"Week Day",   "Booking_WeekDay"},
                          {"Year",       "Booking_Year"},
                          {"Year-Month", "Booking_YearMonth"},
                          {"Quarter",    "Booking_YearQuarter"},
                          {"ISO Week",   "Booking_YearWeek"}
                      }),

    // Asegurar tipos base
    ChangedBaseTypes = Table.TransformColumnTypes(RenamedBase, {
                          {"Booking_Year",     Int64.Type},
                          {"Booking_WeekDay",  Int64.Type}
                      }),

    // 1) Booking_Month: número de mes de Booking_Date
    AddedMonth        = Table.AddColumn(ChangedBaseTypes, "Booking_Month", each Date.Month([Booking_Date]), Int64.Type),

    // 2) Booking_Quarter: número de trimestre (1–4) de Booking_Date
    AddedQuarter      = Table.AddColumn(AddedMonth, "Booking_Quarter", each Date.QuarterOfYear([Booking_Date]), Int64.Type),

    // 3) Booking_Week: extrae el número de semana de Booking_YearWeek (texto "YYYY-WW")
    AddedWeek         = Table.AddColumn(AddedQuarter, "Booking_Week", each Number.From(Text.End([Booking_YearWeek], 2)), Int64.Type),
    #"Removed Columns" = Table.RemoveColumns(AddedWeek,{"ISO Year"}),
    #"Reordered Columns" = Table.ReorderColumns(#"Removed Columns",{"Booking_Date", "Booking_WeekDay", "Booking_Week", "Booking_Month", "Booking_Quarter", "Booking_Year", "Booking_YearWeek", "Booking_YearQuarter", "Booking_YearMonth"})
in
    #"Reordered Columns"
    
    
    
    
  --------- Calendario_RI 2025-03-31 -------------------------------------------------------------------------------------
  ------- dimRecognition_Calendar  
    
    
let
    // 1. Definir el rango de fechas
    StartDate        = #date(2024, 1, 1),
    EndDate          = #date(2027, 12, 31),
    DaysList         = List.Dates(
                          StartDate,
                          Number.From(EndDate - StartDate) + 1,
                          #duration(1,0,0,0)),
    CalendarTable    = Table.FromList(DaysList, Splitter.SplitByNothing(), {"Date"}),
    ChangedType      = Table.TransformColumnTypes(CalendarTable, {{"Date", type date}}),

    // 2. Día de la semana (lunes = 1)
    WeekDayColumn    = Table.AddColumn(ChangedType, "Week Day",
                          each Date.DayOfWeek([Date], Day.Monday) + 1, Int64.Type),

    // 3. Año
    YearColumn       = Table.AddColumn(WeekDayColumn, "Year",
                          each Date.Year([Date]), Int64.Type),

    // 4. Año-Mes (YYYY-MM)
    YearMonthColumn  = Table.AddColumn(YearColumn, "Year-Month",
                          each Text.PadStart(Text.From([Year]),4,"0") & "-"
                              & Text.PadStart(Text.From(Date.Month([Date])),2,"0"),
                          type text),

    // 5. Trimestre (YYYY-QX)
    QuarterColumn    = Table.AddColumn(YearMonthColumn, "Quarter",
                          each Text.PadStart(Text.From(Date.Year([Date])),4,"0") & "-Q"
                              & Text.From(Date.QuarterOfYear([Date])),
                          type text),

    // 6. Recognition_Quarter (1-4)
    AddedRecQuarter  = Table.AddColumn(QuarterColumn, "Recognition_Quarter",
                          each Date.QuarterOfYear([Date]), Int64.Type),

    // 7. Recognition_Month (1-12)  ← NUEVO
    AddedRecMonth    = Table.AddColumn(AddedRecQuarter, "Recognition_Month",
                          each Date.Month([Date]), Int64.Type),

    // 8. Año ISO
    ISOYearColumn    = Table.AddColumn(AddedRecMonth, "ISO Year", each
                          let
                              CY   = Date.Year([Date]),
                              WF   = Date.WeekOfYear([Date], Day.Monday),
                              ISOY = if WF = 1  and Date.Month([Date]) = 12 then CY + 1
                                     else if WF >= 52 and Date.Month([Date]) = 1 then CY - 1
                                     else CY
                          in  ISOY, Int64.Type),

    // 9. Recognition_YearWeek (texto YYYY-WW)
    ISOWeekColumn    = Table.AddColumn(ISOYearColumn, "ISO Week", each
                          let
                              FirstThu = Date.AddDays(#date([ISO Year],1,4),
                                                       -Date.DayOfWeek(#date([ISO Year],1,4), Day.Monday)),
                              IW = Number.RoundDown((Number.From([Date] - FirstThu)/7) + 1, 0)
                          in  if IW > 52
                              then Text.PadStart(Text.From([ISO Year] + 1),4,"0") & "-01"
                              else Text.PadStart(Text.From([ISO Year]),4,"0") & "-"
                                   & Text.PadStart(Text.From(IW),2,"0"),
                          type text),

    // 10. Recognition_Week (1-52)
    AddedRecWeek     = Table.AddColumn(ISOWeekColumn, "Recognition_Week",
                          each Number.From(Text.End([ISO Week],2)), Int64.Type),

    // 11. Renombrar columnas a su nombre final
    RenamedCols      = Table.RenameColumns(
                          AddedRecWeek,
                          {
                            {"Date",       "Recognition_Date"},
                            {"Week Day",   "Recognition_WeekDay"},
                            {"Year",       "Recognition_Year"},
                            {"Year-Month", "Recognition_YearMonth"},
                            {"Quarter",    "Recognition_YearQuarter"},
                            {"ISO Week",   "Recognition_YearWeek"}
                          }),

    // 12. Reordenar
    ReorderedCols    = Table.ReorderColumns(
                          RenamedCols,
                          {
                            "Recognition_Date", "Recognition_WeekDay", "Recognition_Year",
                            "Recognition_Month",         // ← nueva
                            "Recognition_YearMonth", "Recognition_YearQuarter",
                            "Recognition_YearWeek", "Recognition_Quarter",
                            "Recognition_Week"
                          }),
    #"Removed Columns" = Table.RemoveColumns(ReorderedCols,{"ISO Year"}),
    #"Reordered Columns" = Table.ReorderColumns(#"Removed Columns",{"Recognition_Date", "Recognition_WeekDay", "Recognition_Week", "Recognition_Month", "Recognition_Quarter", "Recognition_Year", "Recognition_YearWeek", "Recognition_YearMonth", "Recognition_YearQuarter"})
in
    #"Reordered Columns"


    
    
    
    
    
    
    
    ------------------------
    
    --Calendario de RI pero YearMonth menor jerarquia
    ----- "dimRecognition_YearMonth"
    
    
    let
    // 1. Definir el rango de fechas
    StartDate        = #date(2024, 1, 1),
    EndDate          = #date(2027, 12, 31),
    DaysList         = List.Dates(StartDate, Duration.Days(EndDate - StartDate) + 1, #duration(1,0,0,0)),
    CalendarTable    = Table.FromList(DaysList, Splitter.SplitByNothing(), {"Date"}),
    ChangedType      = Table.TransformColumnTypes(CalendarTable, {{"Date", type date}}),

    // 2. Extraer Año y Mes como números
    YearColumn       = Table.AddColumn(ChangedType, "Year",  each Date.Year([Date]),  Int64.Type),
    MonthColumn      = Table.AddColumn(YearColumn,  "Month", each Date.Month([Date]), Int64.Type),

    // 3. Crear YearMonth como texto y YearQuarter (YYYY-Q#) como texto
    YearMonthColumn  = Table.AddColumn(MonthColumn, "YearMonth", each
                          Text.PadStart(Text.From([Year]), 4, "0") & "-" &
                          Text.PadStart(Text.From([Month]), 2, "0"),
                          type text),
    YearQuarterColumn= Table.AddColumn(YearMonthColumn, "YearQuarter", each
                          Text.PadStart(Text.From([Year]), 4, "0") & "-Q" &
                          Text.From(Date.QuarterOfYear([Date])),
                          type text),

    // 4. Agregar columna numérica Recognition_Quarter (1–4)
    QuarterNumber    = Table.AddColumn(YearQuarterColumn, "Quarter", each Date.QuarterOfYear([Date]), Int64.Type),

    // 5. Dejar sólo un registro por YYYY-MM
    UniqueYearMonth  = Table.Distinct(QuarterNumber, {"YearMonth"}),

    // 6. Ajustar tipos y renombrar
    ChangedTypes     = Table.TransformColumnTypes(
                          UniqueYearMonth,
                          {
                            {"Year",    Int64.Type},
                            {"Month",   Int64.Type},
                            {"Quarter", Int64.Type}
                          }
                       ),
    RenamedColumns   = Table.RenameColumns(
                          ChangedTypes,
                          {
                            {"Year",         "Recognition_Year"},
                            {"Month",        "Recognition_Month"},
                            {"YearMonth",    "Recognition_YearMonth"},
                            {"YearQuarter",  "Recognition_YearQuarter"},
                            {"Quarter",      "Recognition_Quarter"}
                          }
                       ),

    // 7. Eliminar columna Date y reordenar
    RemovedDate      = Table.RemoveColumns(RenamedColumns, {"Date"}),
    #"Reordered Columns" = Table.ReorderColumns(
                            RemovedDate,
                            {
                              "Recognition_YearMonth",
                              "Recognition_YearQuarter",
                              "Recognition_Year",
                              "Recognition_Month",
                              "Recognition_Quarter"
                            }
                         )
in
    #"Reordered Columns"