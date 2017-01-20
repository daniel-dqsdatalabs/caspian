package com.thoughtworks.datapipeline

import com.thoughtworks.SparkTestCase
import com.thoughtworks.pipeline.SalesFactTable

class SalesFactTableTest extends SparkTestCase {
    test("should add the surrogate key of dimension as a foreign key in the primary table") {
        val salesJoinedRecord = sc.parallelize(Array("Key,Key,NaturalKey,Key"))
        val mapper = new SalesFactTable(salesJoinedRecord)
        val supplier =sc.parallelize(Array(
            "SurrogateKey,NaturalKey,Supplier#000000001",
            "55,123,Supplier#000000002",
            "56,223,Supplier#000000003")
        )
        val actual = mapper.replaceWithSurrogateKey(supplier,2)
        val expected = sc.parallelize(Array("Key,Key,SurrogateKey,Key"))
        assertResult(expected.collect())(actual.toRDD.collect())
    }

}
