package com.thoughtworks.pipeline

import com.thoughtworks.datacommons.prepbuddy.types.CSV
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord


object CSVParser {

    def parse(string: String): Seq[String] = CSV.parse(string).map((x) => x)

    def makeCSV(record: Seq[String]): String = {
        val value = new RowRecord(record.toArray)
        CSV.join(value)
    }

}
