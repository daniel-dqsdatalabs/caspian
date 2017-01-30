package com.thoughtworks.pipeline

import org.apache.spark.{SparkConf, SparkContext}

object FactTransformation {
    var sc: SparkContext = _
    var path = ""

    def setSparkContext(): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(getClass.getName)
        sc = new SparkContext(conf)
    }

    def salesFact(lineItemTable: Table, ordersTable: Table): FactTable = {
        val dimensionPath = "dimensions/"
        val joinedTable = lineItemTable.merge(ordersTable, 0, preserveNaturalKey = true)
        val factTable = new FactTable(joinedTable)
        val dateDimension = new Table(sc.textFile(path + dimensionPath + "dimDate"))
        val salesTable = factTable
            .replaceWithSurrogateKey(new Table(sc.textFile(path + dimensionPath + "dimPart")), 1)
            .replaceWithSurrogateKey(new Table(sc.textFile(path + dimensionPath + "dimSupplier")), 2)
            .replaceWithSurrogateKey(new Table(sc.textFile(path + dimensionPath + "dimCustomer")), 16)
            .replaceWithSurrogateKey(dateDimension, 10)
            .replaceWithSurrogateKey(dateDimension, 11)
            .replaceWithSurrogateKey(dateDimension, 12)
            .replaceWithSurrogateKey(dateDimension, 19)
        salesTable.addSurrogateKey()
    }

    def main(args: Array[String]): Unit = {
        path = args(0)
        setSparkContext()
        val importsPath = "imports/"
        val lineItemTable = new Table(sc.textFile(path + importsPath + "lineitem"))
        val ordersTable = new Table(sc.textFile(path + importsPath + "orders"))
        val salesJoinedTable = salesFact(lineItemTable, ordersTable)
        salesJoinedTable.saveAsTextFile(path + "facts/factSales")
        sc.stop()
    }
}
