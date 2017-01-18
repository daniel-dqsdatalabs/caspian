package com.thoughtworks.pipeline

import java.util.Date

import org.apache.commons.lang.time.DateUtils

import scala.collection.mutable.ListBuffer

class DateDimension {
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
