package samples.calculators

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._


class SparkLibrarySpec extends FlatSpec with Matchers with BeforeAndAfter {

  var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .setAppName("Spark Testing")

    sc = SparkContext
      .getOrCreate(conf)
  }

  "A PiCalculator" should "barely aproxximate PI after 1 iteration" in {
    val calc = new PiCalculator(1, sc)
    calc.montecarlo should be(4)
  }
}