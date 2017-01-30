package com.thoughtworks.datapipeline

import com.thoughtworks.SparkTestCase
import com.thoughtworks.pipeline.{FactTable, Table}

class FactTableTest extends SparkTestCase {
    test("should replace the surrogate key of dimension as a foreign key in the primary table") {
        val salesJoinedRecord = sc.parallelize(Array("Key,Key,NaturalKey,Key"))
        val table = new Table(salesJoinedRecord)
        val factTable = new FactTable(table)
        val supplier = new Table(sc.parallelize(Array(
            "SurrogateKey,NaturalKey,Supplier#000000001",
            "55,123,Supplier#000000002",
            "56,223,Supplier#000000003")
        ))
        val actual = factTable.replaceWithSurrogateKey(supplier, 2)
        val expected = sc.parallelize(Array("Key,Key,SurrogateKey,Key"))
        assertResult(expected.collect())(actual.toRDD.collect())
    }
    test("should be able to add surrogate key on the first column of the table") {
        val nationRecord = sc.parallelize(List("0,ALGERIA,\"haggle.,is here\",AFRICA,\"regular ,deposits.\""))
        val nationTable = new Table(nationRecord)
        val dimTable = new FactTable(nationTable)
        val tableWithSurrogateKey = dimTable.addSurrogateKey()


        assert(tableWithSurrogateKey.toRDD.collect().head == "0,0,ALGERIA,\"haggle.,is here\",AFRICA,\"regular ,deposits.\"")
    }
}
