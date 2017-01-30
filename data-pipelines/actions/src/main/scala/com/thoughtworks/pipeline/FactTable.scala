package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.types.CSV
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map

class FactTable(joinedTable: Table) {

    def saveAsTextFile(path: String): Unit = joinedTable.saveAsTextFile(path)

    def addSurrogateKey(): FactTable = {
        val withSurrogateKey = joinedTable.toRDD.zipWithIndex().map {
            case (record, key) => CSVParser.makeCSV(key.toString +: CSVParser.parse(record))
        }
        new FactTable(new Table(withSurrogateKey))
    }

    def replaceWithSurrogateKey(dimensionTable: Table, indexInJoinedTable: Int): FactTable = {
        val broadcast = tableToBroadcast(dimensionTable)
        val joined = toRDD.map((x) => {
            val parse = CSV.parse(x)
            val select = parse.select(indexInJoinedTable)
            val value = broadcast.value(select)
            CSV.join(parse.replace(indexInJoinedTable, value))
        })
        new FactTable(new Table(joined))
    }

    private def tableToBroadcast(dimensionTable: Table): Broadcast[Map[String, String]] = {
        val sc = dimensionTable.toRDD.sparkContext
        val keyValue = dimensionTable.toRDD.map((x) => {
            val parse = CSVParser.parse(x)
            (parse(1), parse.head)
        })
        sc.broadcast(keyValue.collectAsMap())
    }

    def toRDD: RDD[String] = joinedTable.toRDD

}
