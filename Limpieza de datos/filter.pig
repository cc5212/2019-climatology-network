-- (station_code, lat, longitud, elevation, city_name, county_code, country_name);
paises_estaciones = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/estaciones-paises.csv' USING PigStorage(',') AS (station_code:chararray, lat:float, longitud:float, elevation:float, city_name:chararray, county_code:chararray, country_name:chararray);

-- AÑO 2010
-- (stat_code, date, typeOfData, value, a, b, c, d), las letras son irrelevantes para este análisis
-- typeOfData: TAVG, PRCP: Precipitation (tenths of mm), SNOW: Snowfall (mm)
-- TAVG = Average temperature (tenths of degrees C)
-- PRCP = Precipitation (tenths of mm)

anio2010 = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/1990.csv' USING PigStorage(',') AS (stat_code:chararray, date:chararray, typeOfData:chararray, value:int, a, b, c, d);
tempFilter = FILTER anio2010 BY (typeOfData == 'TAVG' OR typeOfData == 'TMAX' OR typeOfData == 'TMIN');

joincito = JOIN paises_estaciones BY station_code, tempFilter BY stat_code;
final = FOREACH joincito GENERATE $0, $1, $2, $3, $4, $5, $6, $8, $9, $10;

--DUMP final;
STORE final INTO '/uhadoop2019/grupo8/1990-filterjoin' using PigStorage(',');

