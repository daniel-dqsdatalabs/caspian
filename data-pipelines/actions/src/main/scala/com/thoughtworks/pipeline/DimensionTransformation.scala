package com.thoughtworks.pipeline


import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DimensionTransformation {
    var sc: SparkContext = _
    val surrogateKeyGenerator = new SurrogateKeyGenerator()

    def setSparkContext(): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(getClass.getName)
        sc = new SparkContext(conf)
    }

    def nationDim(nationTable: RDD[String], regionTable: RDD[String]): TransformableRDD = {
        val merger: TableMerger = new TableMerger(nationTable, regionTable)
        val transformedNationTable = merger.merge(0, 2)
        surrogateKeyGenerator.addSurrogateKey(transformedNationTable)
    }

    def customerDim(customerTable: TransformableRDD): TransformableRDD = {
        surrogateKeyGenerator.addSurrogateKey(customerTable)
    }

    def supplierDim(supplierTable: TransformableRDD): TransformableRDD = {
        surrogateKeyGenerator.addSurrogateKey(supplierTable)
    }

    def partDim(partTable: TransformableRDD): TransformableRDD = {
        surrogateKeyGenerator.addSurrogateKey(partTable)
    }

    def dateDim: TransformableRDD = {
        val dimension = new DateDimension()
        val range = dimension.calculateDates("2005 01 01", "2020 12 31")
        val dates = sc.parallelize(range).map(_.toString)
        val dateRange = new TransformableRDD(dates)
        surrogateKeyGenerator.addSurrogateKey(dateRange)
    }

    def main(args: Array[String]) {
        setSparkContext()
        val path = args(0)
        val nationTable = new TransformableRDD(sc.textFile(path + "nation"))
        val regionTable = new TransformableRDD(sc.textFile(path + "region"))
        nationDim(nationTable, regionTable).saveAsTextFile(path + "dimNation")

        val customerTable = new TransformableRDD(sc.textFile(path + "customer"))
        customerDim(customerTable).saveAsTextFile(path + "dimCustomer")

        val supplierTable = new TransformableRDD(sc.textFile(path + "supplier"))
        supplierDim(supplierTable).saveAsTextFile(path + "dimSupplier")

        val partTable = new TransformableRDD(sc.textFile(path + "part"))
        partDim(partTable).saveAsTextFile(path + "dimPart")

        dateDim.saveAsTextFile(path + "dimDate")
        sc.stop()
    }


}