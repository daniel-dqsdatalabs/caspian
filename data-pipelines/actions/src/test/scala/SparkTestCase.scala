import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SparkTestCase extends FunSuite with BeforeAndAfterEach {
    var sc: SparkContext = null

    override def beforeEach() {
        val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        sc = new SparkContext(sparkConf)
    }

    override def afterEach() {
        sc.stop()
    }
}

