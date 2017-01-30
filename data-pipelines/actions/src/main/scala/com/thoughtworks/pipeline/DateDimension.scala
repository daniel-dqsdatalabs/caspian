package com.thoughtworks.pipeline

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class DateDimension(from: String, until: String, sc: SparkContext, dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")) {
    def toRDD: RDD[String] = dimension.toRDD

    private val dimension = new DimensionTable(calculateRange)

    def saveAsTextFile(path: String): Unit = dimension.saveAsTextFile(path)

    def addSurrogateKey(): DimensionTable = dimension.addSurrogateKey()

    private def calculateRange = {
        val fromDate = dateFormat.parse(from)
        val untilDate = dateFormat.parse(until)
        val allDates = datesBetween(fromDate, untilDate).map(dateFormat.format)
        new Table(sc.parallelize(allDates))
    }

    private def datesBetween(from: Date, until: Date): List[Date] = {
        var list: ListBuffer[Date] = ListBuffer(from)
        var currentDate = from
        while (until.compareTo(currentDate) > 0) {
            currentDate = DateUtils.addDays(currentDate, 1)
            list = list :+ currentDate
        }
        list.toList
    }

}
