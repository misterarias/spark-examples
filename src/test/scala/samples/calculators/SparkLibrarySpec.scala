package samples.calculators

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class SparkLibrarySpec extends FlatSpec with Matchers with BeforeAndAfter {

  var sc: SparkContext = _

  before {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .setAppName("Spark Testing")

    sc = SparkContext
      .getOrCreate(conf)

    sc.setLogLevel("ERROR")
  }

  "A PiCalculator" should "not aproxximate PI after 1 iteration" in {
    val calc = new PiCalculator(1, sc)
    calc.montecarlo should not be 3.14
  }
}