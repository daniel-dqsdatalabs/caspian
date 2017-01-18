package com.thoughtworks.datapipeline

import java.text.SimpleDateFormat

import com.thoughtworks.pipeline.DateDimension
import org.scalatest.FunSuite

class DateDimensionTest extends FunSuite {
    test("should give the dates between specified date") {
        val format = new SimpleDateFormat("yyyy MM dd")
        val from = format.parse("2014 01 01")
        val until = format.parse("2014 12 31")
        val dimension = new DateDimension()
        val dateRange = dimension.calculateDates(from, until)
        assert(dateRange.length == 365)
    }

}
