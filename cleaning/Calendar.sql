---- Calendario_Venta / 

let
    // Definir el rango de fechas
    StartDate = #date(2024, 1, 1),
    EndDate = #date(2025, 12, 31),
    DaysList = List.Dates(StartDate, Number.From(EndDate - StartDate) + 1, #duration(1,0,0,0)),
    CalendarTable = Table.FromList(DaysList, Splitter.SplitByNothing(), {"Date"}),
    ChangedType = Table.TransformColumnTypes(CalendarTable, {{"Date", type date}}),

    // Agregar columna de día de la semana donde lunes = 1
    WeekDayColumn = Table.AddColumn(ChangedType, "Week Day", each Date.DayOfWeek([Date], Day.Monday) + 1, type number),

    // Extraer Año
    YearColumn = Table.AddColumn(WeekDayColumn, "Year", each Date.Year([Date]), type number),

    // Formato Año-Mes YYYY-MM
    YearMonthColumn = Table.AddColumn(YearColumn, "Year-Month", each 
        Text.PadStart(Text.From(Date.Year([Date])), 4, "0") & "-" & 
        Text.PadStart(Text.From(Date.Month([Date])), 2, "0"), type text),

    // Formato Trimestre YYYY-QX
    QuarterColumn = Table.AddColumn(YearMonthColumn, "Quarter", each 
        Text.PadStart(Text.From(Date.Year([Date])), 4, "0") & "-Q" & 
        Text.From(Date.QuarterOfYear([Date])), type text),

    // Calcular Año ISO correctamente
    ISOYearColumn = Table.AddColumn(QuarterColumn, "ISO Year", each 
        let
            CurrentYear = Date.Year([Date]),
            FirstThursday = Date.AddDays(#date(CurrentYear, 1, 4), -Date.DayOfWeek(#date(CurrentYear, 1, 4), Day.Monday)), // Encuentra el primer jueves del año ISO
            WeekNumber = Date.WeekOfYear([Date], 2), // Semana ISO con lunes como inicio
            ISOYear = if WeekNumber = 1 and Date.Month([Date]) = 12 then CurrentYear + 1
                      else if WeekNumber >= 52 and Date.Month([Date]) = 1 then CurrentYear - 1 
                      else CurrentYear
        in
            ISOYear, type number),

    // Calcular ISO Week correctamente (y eliminar la semana 53 si no existe)
    ISOWeekColumn = Table.AddColumn(ISOYearColumn, "ISO Week", each 
        let
            FirstThursday = Date.AddDays(#date([ISO Year], 1, 4), -Date.DayOfWeek(#date([ISO Year], 1, 4), Day.Monday)), // Encuentra el primer jueves del año ISO
            ISOWeek = Number.RoundDown((Number.From([Date] - FirstThursday) / 7) + 1, 0)
        in
            if ISOWeek > 52 then Text.PadStart(Text.From([ISO Year] + 1), 4, "0") & "-01" // Corregir si hay una semana 53 inexistente
            else Text.PadStart(Text.From([ISO Year]), 4, "0") & "-" & Text.PadStart(Text.From(ISOWeek), 2, "0"), type text),

    #"Changed Type" = Table.TransformColumnTypes(ISOWeekColumn, {{"ISO Year", type text}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type", {"ISO Year"}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Removed Columns", {{"Year", type text}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type1",{{"Date", "Booking_Date"}, {"Week Day", "Booking_WeekDay"}, {"Year", "Booking_Year"}, {"Year-Month", "Booking_YearMonth"}, {"Quarter", "Booking_Quarter"}, {"ISO Week", "Booking_ISOWeek"}})
in
    #"Renamed Columns"
    
    
    
    
  --------- Calendario_RI 2025-03-31
    
  
    let
    // Definir el rango de fechas
    StartDate = #date(2024, 1, 1),
    EndDate = #date(2027, 12, 31),
    DaysList = List.Dates(StartDate, Number.From(EndDate - StartDate) + 1, #duration(1,0,0,0)),
    CalendarTable = Table.FromList(DaysList, Splitter.SplitByNothing(), {"Date"}),
    ChangedType = Table.TransformColumnTypes(CalendarTable, {{"Date", type date}}),

    // Agregar columna de día de la semana donde lunes = 1
    WeekDayColumn = Table.AddColumn(ChangedType, "Week Day", each Date.DayOfWeek([Date], Day.Monday) + 1, type number),

    // Extraer Año
    YearColumn = Table.AddColumn(WeekDayColumn, "Year", each Date.Year([Date]), type number),

    // Formato Año-Mes YYYY-MM
    YearMonthColumn = Table.AddColumn(YearColumn, "Year-Month", each 
        Text.PadStart(Text.From(Date.Year([Date])), 4, "0") & "-" & 
        Text.PadStart(Text.From(Date.Month([Date])), 2, "0"), type text),

    // Formato Trimestre YYYY-QX
    QuarterColumn = Table.AddColumn(YearMonthColumn, "Quarter", each 
        Text.PadStart(Text.From(Date.Year([Date])), 4, "0") & "-Q" & 
        Text.From(Date.QuarterOfYear([Date])), type text),

    // Calcular Año ISO correctamente
    ISOYearColumn = Table.AddColumn(QuarterColumn, "ISO Year", each 
        let
            CurrentYear = Date.Year([Date]),
            FirstThursday = Date.AddDays(#date(CurrentYear, 1, 4), -Date.DayOfWeek(#date(CurrentYear, 1, 4), Day.Monday)), // Encuentra el primer jueves del año ISO
            WeekNumber = Date.WeekOfYear([Date], 2), // Semana ISO con lunes como inicio
            ISOYear = if WeekNumber = 1 and Date.Month([Date]) = 12 then CurrentYear + 1
                      else if WeekNumber >= 52 and Date.Month([Date]) = 1 then CurrentYear - 1 
                      else CurrentYear
        in
            ISOYear, type number),

    // Calcular ISO Week correctamente (y eliminar la semana 53 si no existe)
    ISOWeekColumn = Table.AddColumn(ISOYearColumn, "ISO Week", each 
        let
            FirstThursday = Date.AddDays(#date([ISO Year], 1, 4), -Date.DayOfWeek(#date([ISO Year], 1, 4), Day.Monday)), // Encuentra el primer jueves del año ISO
            ISOWeek = Number.RoundDown((Number.From([Date] - FirstThursday) / 7) + 1, 0)
        in
            if ISOWeek > 52 then Text.PadStart(Text.From([ISO Year] + 1), 4, "0") & "-01" // Corregir si hay una semana 53 inexistente
            else Text.PadStart(Text.From([ISO Year]), 4, "0") & "-" & Text.PadStart(Text.From(ISOWeek), 2, "0"), type text),

    #"Changed Type" = Table.TransformColumnTypes(ISOWeekColumn, {{"ISO Year", type text}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type", {"ISO Year"}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Removed Columns", {{"Year", type text}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type1",{{"Date", "Recognition_Date"}, {"Week Day", "Recognition_WeekDay"}, {"Year", "Recognition_Year"}, {"Year-Month", "Recognition_YearMonth"}, {"Quarter", "Recognition_Quarter"}, {"ISO Week", "Recognition_ISOWeek"}})
in
    #"Renamed Columns"
    
    