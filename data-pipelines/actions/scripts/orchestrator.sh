#!/usr/bin/env bash

sqoop import --connect jdbc:postgresql://localhost:5432/tpch-benchmark\
 --username postgres \
 --password ${PASSWORD} \
 --m 1  \
 --table nation \
 --target-dir  ../tpch-benchmark-datasets/s-factor-1/nation/ \
 --optionally-enclosed-by '\"' \
 -- --schema raw_store;

sqoop import --connect jdbc:postgresql://localhost:5432/tpch-benchmark\
 --username postgres \
 --password ${PASSWORD} \
 --m 1  \
 --table region \
 --target-dir  ../tpch-benchmark-datasets/s-factor-1/region/ \
 --optionally-enclosed-by '\"' \
 -- --schema raw_store;

rm nation.java region.java

spark-submit --jars prep-buddy.jar --class NationDimension data-lake.jar "";

sqoop export --connect --connect jdbc:postgresql://localhost:5432/tpch-benchmark \
 --username postgres --password ${PASSWORD} --table nation_dim \
 --export-dir deNormalizedNationTable \
 --optionally-enclosed-by '\"' -m 1 -- --schema tpch_star_schema;