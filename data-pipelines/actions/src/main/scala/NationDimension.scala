import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object NationDimension {
    def main(args: Array[String]) {
        val path = args(0)
        val conf: SparkConf = new SparkConf().setAppName(getClass.getName)
        val sc: SparkContext = new SparkContext(conf)
        val nationTable = new TransformableRDD(sc.textFile(path+"nation"))
        val regionTable  = new TransformableRDD(sc.textFile(path+"region"))
        val merger: TableMerger = new TableMerger(nationTable,regionTable)
        val transformedNationTable: RDD[String] = merger.merge(0,2)
        transformedNationTable.saveAsTextFile(path+"deNormalizedNationTable")
    }
}