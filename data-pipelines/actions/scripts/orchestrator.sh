#!/usr/bin/env bash
CONNECTION_URL="jdbc:postgresql://localhost:5432/tpch-benchmark"
USERNAME="postgres"
DATA_SET_PATH="../tpch-benchmark-datasets/s-factor-1/"

sqoop import --connect ${CONNECTION_URL}\
 --username ${USERNAME} \
 --password ${PASSWORD} \
 --m 1  \
 --table nation \
 --target-dir  ${DATA_SET_PATH}/nation/ \
 --optionally-enclosed-by '\"' \
 -- --schema raw_store;

sqoop import --connect ${CONNECTION_URL}\
 --username ${USERNAME} \
 --password ${PASSWORD} \
 --m 1  \
 --table region \
 --target-dir  ${DATA_SET_PATH}/region/ \
 --optionally-enclosed-by '\"' \
 -- --schema raw_store;

 sqoop import --connect ${CONNECTION_URL}\
 --username ${USERNAME} \
 --password ${PASSWORD} \
 --m 1  \
 --table supplier \
 --target-dir  ${DATA_SET_PATH}/supplier/ \
 --optionally-enclosed-by '\"' \
 -- --schema raw_store;

sqoop import --connect ${CONNECTION_URL}\
 --username ${USERNAME} \
 --password ${PASSWORD} \
 --m 1  \
 --table customer \
 --target-dir  ${DATA_SET_PATH}/customer/ \
 --optionally-enclosed-by '\"' \
 -- --schema raw_store;

 sqoop import --connect ${CONNECTION_URL}\
 --username ${USERNAME} \
 --password ${PASSWORD} \
 --m 1  \
 --table part \
 --target-dir  ${DATA_SET_PATH}/part/ \
 --optionally-enclosed-by '\"' \
 -- --schema raw_store;

spark-submit --jars ../jars/prep-buddy-0.5.0-jar-with-dependencies.jar \
  --class com.thoughtworks.pipeline.DimensionTransformation ../build/libs/data-lake-1.0-SNAPSHOT.jar ${DATA_SET_PATH};

sqoop export --connect ${CONNECTION_URL} \
 --username ${USERNAME} \
 --password ${PASSWORD} \
 --table nation_dim \
 --export-dir ${DATA_SET_PATH}/dimNation \
 --optionally-enclosed-by '\"' -m 1 \
 -- --schema tpch_star_schema;

rm *.java