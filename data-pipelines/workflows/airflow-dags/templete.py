from credentials import *

sqoop_cmd_template = "sqoop {action} " \
                     "--connect " + rds_store_uri + " " + \
                     "--username {username} --password {password} " \
                     "--optionally-enclosed-by '\"' ".format(username=username, password=password)

import_cmd_template = sqoop_cmd_template.format(action="import") + \
                      "--table {table} " \
                      "--warehouse-dir " + s3_bucket + " " + \
                      "-- --schema raw_store ;"

export_cmd_template = sqoop_cmd_template.format(action="export") + \
                      "--table {table} " \
                      "--export-dir " + s3_bucket + "/{inputPath} " + \
                      "-- --schema tpch_star_schema;"

spark_job_template = "spark-submit --jars prep-buddy.jar " + \
                     "--class {mainClass} data-lake.jar {args}"

import_all_cmd_template = sqoop_cmd_template.format(action="import-all-tables") + \
                          "--warehouse-dir " + s3_bucket + " " + \
                          "--exclude-tables {exclude_tables} " + " " + \
                          "-- --schema raw_store ;"
