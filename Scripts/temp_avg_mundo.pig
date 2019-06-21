-- (stat_code, date, typeOfData, value, a, b, c, d), las letras son irrelevantes para este análisis
-- typeOfData: TAVG, PRCP: Precipitation (tenths of mm), SNOW: Snowfall (mm)
-- TAVG = Average temperature (tenths of degrees C)
-- PRCP = Precipitation (tenths of mm)
anos = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/1910-filterjoin/part-r-00000'USING PigStorage(',') AS (station_code:chararray, lat:float, longitud:float, elevation:float, city_name:chararray, county_code:chararray, country_name:chararray, date:chararray, typeOfData:chararray, value:int);
filtered = FILTER anos BY (typeOfData == 'TAVG' AND (value is not null));
group_data = GROUP filtered BY typeOfData;
average_temps = FOREACH group_data GENERATE AVG(filtered.value);
DUMP average_temps;

