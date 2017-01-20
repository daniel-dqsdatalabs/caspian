package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import com.thoughtworks.datacommons.prepbuddy.types.CSV
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.rdd.RDD

class SurrogateKeyGenerator {
    def addSurrogateKey(primaryTable: RDD[String]): TransformableRDD = {
        val withSurrogateKey = primaryTable.zipWithIndex().map((tuple) => {
            val key = new RowRecord(Array(tuple._2.toString))
            val record = CSV.parse(tuple._1).map(x => x).toArray
            CSV.join(key.appendColumns(record))
        })
        new TransformableRDD(withSurrogateKey)
    }
}
