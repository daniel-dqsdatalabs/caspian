package com.thoughtworks.pipeline

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang.time.DateUtils

import scala.collection.mutable.ListBuffer

class DateDimension {
    def calculateDates(from: String, until: String): List[String] = {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val fromDate = dateFormat.parse(from)
        val untilDate = dateFormat.parse(until)
        calculateDates(fromDate, untilDate).map(dateFormat.format)
    }

    def calculateDates(from: Date, until: Date): List[Date] = {
        var list: ListBuffer[Date] = ListBuffer(from)
        var currentDate = from
        while (until.compareTo(currentDate) > 0) {
            currentDate = DateUtils.addDays(currentDate, 1)
            list = list :+ currentDate
        }
        list.toList
    }
}
