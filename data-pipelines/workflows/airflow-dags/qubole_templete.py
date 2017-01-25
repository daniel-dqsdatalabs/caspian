from credentials import *

sqoop_cmd_template = "/usr/lib/sqoop/bin/sqoop {action} " \
                     "--connect " + rds_store_uri + " " + \
                     "--username {username} --password {password} " \
                     "--optionally-enclosed-by '\"' ".format(username=username, password=password)

import_cmd_template = sqoop_cmd_template.format(action="import") + \
                      "--table {table} " \
                      "--warehouse-dir " + s3_bucket_key + "imports/ " + \
                      "--schema raw_store ;"

import_all_cmd_template = sqoop_cmd_template.format(action="import-all-tables") + \
                          "--warehouse-dir " + s3_bucket_key + "imports/ " + \
                          "--exclude-tables {exclude_tables} " + " " + \
                          "--schema raw_store ;"

export_cmd_template = sqoop_cmd_template.format(action="export") + \
                      "--table {table} " \
                      "--export-dir " + s3_bucket_key + "/{inputPath} " + \
                      "--schema tpch_star_schema;"

spark_job_template = "/usr/lib/spark/bin/spark-submit --jars " + s3_bucket + "/jars/prep-buddy.jar " + \
                     "--class {mainClass} " + s3_bucket + "/jars/data-lake.jar {args}"
