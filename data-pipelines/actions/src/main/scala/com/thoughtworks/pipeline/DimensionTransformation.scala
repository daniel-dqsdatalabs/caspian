package com.thoughtworks.pipeline


import org.apache.spark.{SparkConf, SparkContext}

object DimensionTransformation {
    var sc: SparkContext = _

    def setSparkContext(): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(getClass.getName)
        sc = new SparkContext(conf)
    }

    def nationDim(nationTable: Table, regionTable: Table): DimensionTable = {
        val transformedNationTable = nationTable.merge(regionTable, 2)
        val nationDimensionTable = new DimensionTable(transformedNationTable)
        nationDimensionTable.addSurrogateKey()
    }

    def customerDim(customerTable: Table): DimensionTable = {
        val customerDimension = new DimensionTable(customerTable)
        customerDimension.addSurrogateKey()
    }

    def supplierDim(supplierTable: Table): DimensionTable = {
        val supplierDimension = new DimensionTable(supplierTable)
        supplierDimension.addSurrogateKey()
    }

    def partDim(partTable: Table): DimensionTable = {
        val partDimension = new DimensionTable(partTable)
        partDimension.addSurrogateKey()
    }

    def dateDim: DimensionTable = {
        val dimension = new DateDimension("2010-01-01", "2020-12-31", sc)
        dimension.addSurrogateKey()
    }

    def main(args: Array[String]) {
        setSparkContext()
        val path = args(0)
        val dimensionsPath = path + "dimensions/"
        val importsPath = path + "imports/"
        val nationTable = new Table(sc.textFile(importsPath + "nation"))
        val regionTable = new Table(sc.textFile(importsPath + "region"))
        nationDim(nationTable, regionTable).saveAsTextFile(dimensionsPath + "dimNation")

        val customerTable = new Table(sc.textFile(importsPath + "customer"))
        customerDim(customerTable).saveAsTextFile(dimensionsPath + "dimCustomer")

        val supplierTable = new Table(sc.textFile(importsPath + "supplier"))
        supplierDim(supplierTable).saveAsTextFile(dimensionsPath + "dimSupplier")

        val partTable = new Table(sc.textFile(importsPath + "part"))
        partDim(partTable).saveAsTextFile(dimensionsPath + "dimPart")

        dateDim.saveAsTextFile(dimensionsPath + "dimDate")
        sc.stop()
    }

}