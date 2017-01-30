package com.thoughtworks.pipeline

import org.apache.spark.rdd.RDD

class DimensionTable(table: Table) {
    def saveAsTextFile(path: String): Unit = table.saveAsTextFile(path)

    def addSurrogateKey(): DimensionTable = {
        val withSurrogateKey = table.toRDD.zipWithIndex().map {
            case (record, key) => CSVParser.makeCSV(key.toString +: CSVParser.parse(record))
        }
        new DimensionTable(new Table(withSurrogateKey))
    }

    def toRDD: RDD[String] = table.toRDD

}
