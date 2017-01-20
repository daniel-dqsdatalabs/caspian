package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FactTransformation {
    var sc: SparkContext = _
    val surrogateKeyGenerator = new SurrogateKeyGenerator()
    var path = ""

    def setSparkContext(): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(getClass.getName)
        sc = new SparkContext(conf)
    }

    def salesFact(lineItemTable: RDD[String], ordersTable: RDD[String]): TransformableRDD = {
        val merger: TableMerger = new TableMerger(lineItemTable, ordersTable)
        val joinedTable = merger.merge(0, 0, preserveKey = true)
        val factTable = new SalesFactTable(joinedTable)
        val salesTable = factTable
            .replaceWithSurrogateKey(sc.textFile(path + "dimPart"), 1)
            .replaceWithSurrogateKey(sc.textFile(path + "dimSupplier"), 2)
            .replaceWithSurrogateKey(sc.textFile(path + "dimCustomer"), 16)
            .toRDD
        surrogateKeyGenerator.addSurrogateKey(salesTable)
    }

    def main(args: Array[String]): Unit = {
        path = args(0)
        setSparkContext()
        val lineItemTable = new TransformableRDD(sc.textFile(path + "lineitem"))
        val ordersTable = new TransformableRDD(sc.textFile(path + "orders"))
        val salesJoinedTable = salesFact(lineItemTable, ordersTable)
        salesJoinedTable.saveAsTextFile("testing")
    }
}
