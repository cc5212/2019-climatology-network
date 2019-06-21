-- Input: (code, name)
paises = LOAD 'hdfs://cm:9000/uhadoop2019/mvargas/countries.csv' USING PigStorage(',') AS (code:chararray, country_name);
estaciones = LOAD 'hdfs://cm:9000/uhadoop2019/mvargas/stations.csv' USING PigStorage(',') AS (station_code:chararray, lat, longitud, elevation, city_name);

-- (station_code, lat, elevation, city_name) , CI
estaciones2 = FOREACH estaciones GENERATE (station_code, lat, longitud, elevation, city_name), SUBSTRING(station_code, 0, 2);


joincito = JOIN paises BY code, estaciones2 BY $1;

bien_hecho = FOREACH joincito GENERATE FLATTEN($2), $0, $1;

-- TODO: REPLACE ahogan WITH YOUR FOLDER
STORE bien_hecho INTO '/uhadoop2019/mvargas/pigcito' using PigStorage(',');

