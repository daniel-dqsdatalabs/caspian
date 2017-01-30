package com.thoughtworks.datapipeline

import com.thoughtworks.SparkTestCase
import com.thoughtworks.pipeline.Table
import org.apache.spark.rdd.RDD

class TableTest extends SparkTestCase {
    test("should be able to merge two tables according to the new schema") {
        val nationRecord: RDD[String] = sc.parallelize(List("0,ALGERIA,0,haggle."))
        val regionRecord: RDD[String] = sc.parallelize(List("0,AFRICA,regular deposits."))
        val nationTable: Table = new Table(nationRecord, primaryKeyIndex = 0)
        val regionTable: Table = new Table(regionRecord, primaryKeyIndex = 2)
        val normalizedTable: Table = nationTable.merge(regionTable, foreignKeyIndex = 2)


        assert(normalizedTable.toRDD.collect().head == "0,ALGERIA,haggle.,AFRICA,regular deposits.")
    }

    test("should be able to merge two tables according new schema with commas in middle") {
        val nationRecord: RDD[String] = sc.parallelize(List("0,ALGERIA,0,\"haggle.,is here\""))
        val regionRecord: RDD[String] = sc.parallelize(List("0,AFRICA,\"regular ,deposits.\""))
        val nationTable: Table = new Table(nationRecord, 0)
        val regionTable: Table = new Table(regionRecord, 2)
        val normalizedTable: Table = nationTable.merge(regionTable, foreignKeyIndex = 2)

        assert(normalizedTable.toRDD.collect().head == "0,ALGERIA,\"haggle.,is here\",AFRICA,\"regular ,deposits.\"")
    }

    test("should be able to merge two tables without deleting the existing key") {
        val nationRecord: RDD[String] = sc.parallelize(List("0,ALGERIA,0,haggle."))
        val regionRecord: RDD[String] = sc.parallelize(List("0,AFRICA,regular deposits."))
        val nationTable: Table = new Table(nationRecord, 0)
        val regionTable: Table = new Table(regionRecord, 2)
        val normalizedTable: Table = nationTable.merge(regionTable, foreignKeyIndex = 2, preserveNaturalKey = true)


        assert(normalizedTable.toRDD.collect().head == "0,ALGERIA,0,haggle.,AFRICA,regular deposits.")
    }
}
