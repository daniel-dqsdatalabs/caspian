#!/usr/bin/env bash

sqoop import --connect jdbc:postgresql://localhost:5432/tpch-benchmark\
 --username postgres \
 --password ${PASSWORD} \
 --m 1  \
 --table nation \
 --target-dir  ../tpch-benchmark-datasets/s-factor-1/nation/ \
 -- --schema raw_store

sqoop import --connect jdbc:postgresql://localhost:5432/tpch-benchmark\
 --username postgres \
 --password ${PASSWORD} \
 --m 1  \
 --table region \
 --target-dir  ../tpch-benchmark-datasets/s-factor-1/region/ \
 -- --schema raw_store

rm nation.java region.java