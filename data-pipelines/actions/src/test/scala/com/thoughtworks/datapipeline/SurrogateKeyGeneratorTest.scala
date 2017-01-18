package com.thoughtworks.datapipeline

import com.thoughtworks.SparkTestCase
import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import com.thoughtworks.pipeline.SurrogateKeyGenerator
import org.apache.spark.rdd.RDD

class SurrogateKeyGeneratorTest extends SparkTestCase {
    test("should be able to add surrogate key on the first column of the table") {
        val nationTable: RDD[String] = sc.parallelize(List("0,ALGERIA,\"haggle.,is here\",AFRICA,\"regular ,deposits.\""))
        val transformableRDD = new TransformableRDD(nationTable)
        val surrogateKey: SurrogateKeyGenerator = new SurrogateKeyGenerator(transformableRDD)
        val tableWithSurrogateKey: RDD[String] = surrogateKey.addSurrogateKey()


        assert(tableWithSurrogateKey.collect().head == "0,0,ALGERIA,\"haggle.,is here\",AFRICA,\"regular ,deposits.\"")
    }

}
