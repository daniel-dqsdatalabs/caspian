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
        val range = dimension.calculateDates("2010-01-01", "2020-12-31")
        val dateRange = sc.parallelize(range)
        surrogateKeyGenerator.addSurrogateKey(dateRange)
    }

    def main(args: Array[String]) {
        setSparkContext()
        val path = args(0)
        val dimensionPath = "dimensions/"
        val importsPath = "imports/"
        val nationTable = new TransformableRDD(sc.textFile(path + importsPath + "nation"))
        val regionTable = new TransformableRDD(sc.textFile(path + importsPath + "region"))
        nationDim(nationTable, regionTable).saveAsTextFile(path + dimensionPath + "dimNation")

        val customerTable = new TransformableRDD(sc.textFile(path + importsPath + "customer"))
        customerDim(customerTable).saveAsTextFile(path + dimensionPath + "dimCustomer")

        val supplierTable = new TransformableRDD(sc.textFile(path + importsPath + "supplier"))
        supplierDim(supplierTable).saveAsTextFile(path + dimensionPath + "dimSupplier")

        val partTable = new TransformableRDD(sc.textFile(path + importsPath + "part"))
        partDim(partTable).saveAsTextFile(path + dimensionPath + "dimPart")

        dateDim.saveAsTextFile(path + dimensionPath + "dimDate")
        sc.stop()
    }


}