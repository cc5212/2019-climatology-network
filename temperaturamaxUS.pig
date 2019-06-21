paises_estaciones = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/estaciones-paises.csv' USING PigStorage(',') AS (station_code:chararray, lat:chararray, longitud:chararray, elevation, city_name, county_code:chararray, country_name:chararray);
anio2010 = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/1980.csv' USING PigStorage(',') AS (stat_code:chararray, date:chararray, typeOfData:chararray, value:float, a:chararray, b:chararray, c:chararray, d:int);
-- anio2010 = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/2000-filtered/part-m-00000 USING PigStorage(',') AS (stat_code:chararray, date:chararray, typeOfData:chararray, value:float);
tempFilter = FILTER anio2010 BY ((typeOfData == 'TMAX' ) AND (value IS NOT NULL ) );
estacion_as= FILTER paises_estaciones By (county_code == 'AS');
joincito = JOIN tempFilter BY stat_code, estacion_as BY station_code;
group_ano = GROUP joincito BY $2;
maxima = FOREACH group_ano GENERATE MAX(joincito.value);
dump maxima;