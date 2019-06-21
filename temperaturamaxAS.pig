 -- (station_code, lat, longitud, elevation, city_name, county_code, country_name);
paises_estaciones = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/estaciones-paises.csv' USING PigStorage(',') AS (stat_code:chararray, lat:chararray, longitud:chararray, elevation, city_name, county_code:chararray, country_name:chararray);

-- AÑO 2010
-- (stat_code, date, typeOfData, value, a, b, c, d), las letras son irrelevantes para este análisis
-- typeOfData: TAVG, PRCP: Precipitation (tenths of mm), SNOW: Snowfall (mm)
-- TAVG = Average temperature (tenths of degrees C)
-- PRCP = Precipitation (tenths of mm)
anio2010 = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/2010.csv.gz' USING PigStorage(',') AS (stat_code:chararray, date:chararray, typeOfData:chararray, value:float, a:chararray, b:chararray, c:chararray, d:int);
tempFilter = FILTER anio2010 BY typeOfData == 'TMAX' ;
estacion_as= Filter paises_estaciones By country_name == 'Australia';
-- joincito = JOIN tempFilter BY  stat_code, estacion_as BY stat_code;

-- bien_hecho = FOREACH joincito GENERATE FLATTEN($2), $0, $1;
-- group_ano = GROUP bien_hecho BY bien_hecho.country_name;
-- maxima = FOREACH group_ano BY bien_hecho.country_name GENERATE MAX(group_ano.value);

STORE estacion_as INTO '/uhadoop2019/vgonzalez/resultados_1/';
