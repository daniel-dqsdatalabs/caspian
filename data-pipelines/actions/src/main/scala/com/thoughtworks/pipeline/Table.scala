package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.types.CSV
import org.apache.spark.rdd.RDD

class Table(tableRecords: RDD[String], primaryKeyIndex: Int = 0) {
    def saveAsTextFile(path: String): Unit = toRDD.saveAsTextFile(path)

    def toRDD: RDD[String] = tableRecords

    def merge(child: Table, foreignKeyIndex: Int, preserveNaturalKey: Boolean = false): Table = {
        val other = tableByIndex(child.toRDD, primaryKeyIndex)
        val primary = tableByIndex(tableRecords, foreignKeyIndex, preserveNaturalKey)
        val transformedTable = primary.join(other)
        val joinedTable = transformedTable.map {
            case (_, (pTable, oTable)) => CSV.appendDelimiter(pTable) + oTable
        }
        new Table(joinedTable, primaryKeyIndex)
    }

    private def tableByIndex(table: RDD[String], columnIndex: Int, preserve: Boolean = false): RDD[(String, String)] = {
        table.map((record) => {
            val parse = CSVParser.parse(record)
            if (preserve) {
                (parse(columnIndex), record)
            } else {
                val unzip = parse.zipWithIndex.collect { case (value, key) if key != columnIndex => value }
                (parse(columnIndex), CSVParser.makeCSV(unzip))
            }
        })
    }
}
