-- FORMATO
-- AGE00135039,357.297,0.65,50.0,ORAN-HOPITAL MILITAIRE,AG,Algeria ,19001125,TMAX,200
M1900 = LOAD 'hdfs://cm:9000/uhadoop2019/grupo8/1980-filterjoin/part-r-00000'USING PigStorage(',') AS (station_code:chararray, lat:float, longitud:float, elevation:float, city_name:chararray, county_code:chararray, country_name:chararray, date:chararray, typeOfData:chararray, value:int);

filtered = FILTER M1900 BY (typeOfData == 'TMAX' AND (value is not null));

pais = GROUP filtered BY country_name;
--DUMP pais;
average_temps = FOREACH pais GENERATE group, AVG(filtered.value);

ordered = ORDER average_temps BY group ASC;

DUMP ordered;
--STORE average_temps INTO '/uhadoop2019/grupo8/1900-maxgpais' using PigStorage(',');


