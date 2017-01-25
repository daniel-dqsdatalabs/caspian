package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FactTransformation {
    var sc: SparkContext = _
    val surrogateKeyGenerator = new SurrogateKeyGenerator()
    var path = ""

    def setSparkContext(): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(getClass.getName)
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)
        sc = new SparkContext(conf)
    }

    def salesFact(lineItemTable: RDD[String], ordersTable: RDD[String]): TransformableRDD = {
        val dimensionPath = "dimensions/"
        val merger: TableMerger = new TableMerger(lineItemTable, ordersTable)
        val joinedTable = merger.merge(0, 0, preserveKey = true)
        val factTable = new SalesFactTable(joinedTable)
        val dateDimension = sc.textFile(path + dimensionPath + "dimDate")
        val salesTable = factTable
            .replaceWithSurrogateKey(sc.textFile(path + dimensionPath + "dimPart"), 1)
            .replaceWithSurrogateKey(sc.textFile(path + dimensionPath + "dimSupplier"), 2)
            .replaceWithSurrogateKey(sc.textFile(path + dimensionPath + "dimCustomer"), 16)
            .replaceWithSurrogateKey(dateDimension, 10)
            .replaceWithSurrogateKey(dateDimension, 11)
            .replaceWithSurrogateKey(dateDimension, 12)
            .replaceWithSurrogateKey(dateDimension, 19)
            .toRDD
        surrogateKeyGenerator.addSurrogateKey(salesTable)
    }

    def main(args: Array[String]): Unit = {
        path = args(0)
        setSparkContext()
        val importsPath = "imports/"
        val lineItemTable = new TransformableRDD(sc.textFile(path + importsPath + "lineitem"))
        val ordersTable = new TransformableRDD(sc.textFile(path + importsPath + "orders"))
        val salesJoinedTable = salesFact(lineItemTable, ordersTable)
        salesJoinedTable.saveAsTextFile(path + "facts/factSales")
        sc.stop()
    }
}
