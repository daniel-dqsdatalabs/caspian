package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.types.CSV
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.rdd.RDD

class DimensionTable(table: Table) {
    def saveAsTextFile(path: String): Unit = table.saveAsTextFile(path)

    def addSurrogateKey(): DimensionTable = {
        val withSurrogateKey = table.toRDD.zipWithIndex().map((tuple) => {
            val key = new RowRecord(Array(tuple._2.toString))
            val record = CSV.parse(tuple._1).map(x => x).toArray
            CSV.join(key.appendColumns(record))
        })
        new DimensionTable(new Table(withSurrogateKey))
    }

    def toRDD: RDD[String] = table.toRDD

}
