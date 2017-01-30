package com.thoughtworks.datapipeline

import com.thoughtworks.pipeline.CSVParser
import org.scalatest.FunSuite

class CSVParserTest extends FunSuite {
    test("should be able to parse simple CSV ") {
        val string = "First,Second,Third"
        val parse = CSVParser.parse(string)
        assert(parse sameElements Array("First", "Second", "Third"))
    }
    test("should be able to parse CSV with escape quotes") {
        val string = "First,\"Second,Middle\",Third"
        val parse = CSVParser.parse(string)
        assert(parse sameElements Array("First", "Second,Middle", "Third"))
    }
    test("should be able to join CSV with escape quotes") {
        val parsedRecord = Array("First", "Second,Middle", "Third")
        val parse = CSVParser.makeCSV(parsedRecord)
        assert(parse == "First,\"Second,Middle\",Third")
    }
}
