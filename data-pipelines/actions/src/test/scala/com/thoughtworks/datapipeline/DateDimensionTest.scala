package com.thoughtworks.datapipeline

import java.text.SimpleDateFormat

import com.thoughtworks.SparkTestCase
import com.thoughtworks.pipeline.DateDimension

class DateDimensionTest extends SparkTestCase {
    test("should give the dates between specified date with default Format") {
        val dimension = new DateDimension("2014-01-01", "2014-12-31", sc)
        val dateRange = dimension.toRDD
        assert(dateRange.collect().contains("2014-01-01"))
        assert(dateRange.count == 365)
    }

    test("should give the dates between specified date in dd MM yyyy format") {
        val dimension = new DateDimension("01-01-2014", "31-12-2014", sc, new SimpleDateFormat("dd-MM-yyyy"))
        val dateRange = dimension.toRDD
        assert(dateRange.collect().contains("01-02-2014"))
        assert(dateRange.count == 365)
    }
}
