package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import com.thoughtworks.datacommons.prepbuddy.types.CSV
import org.apache.spark.rdd.RDD

class TableMerger(primaryTable: RDD[String], otherTable: RDD[String]) {
    def merge(primaryKeyIndex: Int, foreignKeyIndex: Int, preserveKey: Boolean = false): TransformableRDD = {
        val other = tableByIndex(otherTable, primaryKeyIndex)
        val primary = tableByIndex(primaryTable, foreignKeyIndex, preserveKey)
        val transformedTable = primary.join(other)
        val joinedTable = transformedTable.map {
            case (_, (pTable, oTable)) => CSV.appendDelimiter(pTable) + oTable
        }
        new TransformableRDD(joinedTable)
    }

    private def tableByIndex(table: RDD[String], columnIndex: Int, preserve: Boolean = false): RDD[(String, String)] = {
        table.map((record) => {
            val parse = CSV.parse(record)
            val range = 0 until parse.length
            if (preserve) {
                (parse.select(columnIndex), record)
            } else {
                val selection = range.toBuffer.filter(x => x != columnIndex).toList
                val filtered = parse.select(selection)
                (parse.select(columnIndex), CSV.join(filtered))
            }
        })
    }
}
