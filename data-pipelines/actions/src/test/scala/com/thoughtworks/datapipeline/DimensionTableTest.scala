package com.thoughtworks.datapipeline

import com.thoughtworks.SparkTestCase
import com.thoughtworks.pipeline.{DimensionTable, Table}

class DimensionTableTest extends SparkTestCase {
    test("should be able to add surrogate key on the first column of the table") {
        val nationRecord = sc.parallelize(List("0,ALGERIA,\"haggle.,is here\",AFRICA,\"regular ,deposits.\""))
        val nationTable = new Table(nationRecord)
        val dimTable = new DimensionTable(nationTable)
        val tableWithSurrogateKey = dimTable.addSurrogateKey()

        assert(tableWithSurrogateKey.toRDD.collect().head == "0,0,ALGERIA,\"haggle.,is here\",AFRICA,\"regular ,deposits.\"")
    }

}
