package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.types.CSV
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord
import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class SalesFactTable(joinedTable: RDD[String]) {
    def replaceWithSurrogateKey(dimensionTable: RDD[String], indexInJoinedTable: Int): SalesFactTable = {
        val context = dimensionTable.sparkContext
        val keyValue = dimensionTable.map((x) => {
            val parse = CSV.parse(x)
            (parse.select(1), parse.select(0))
        })
        val broadcast = context.broadcast(keyValue.collectAsMap())
        val joined = joinedTable.map((x) => {
            val parse = CSV.parse(x)
            val select = parse.select(indexInJoinedTable)
            val value = broadcast.value(select)
            CSV.join(parse.replace(indexInJoinedTable, value))
        })
        new SalesFactTable(joined)
    }

    def toRDD: TransformableRDD = new TransformableRDD(joinedTable)

}
