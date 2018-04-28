package samples

import org.apache.spark.{SparkConf, SparkContext}
import samples.calculators.PiCalculator

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .setAppName("Spark Testing")

    val sc = SparkContext
      .getOrCreate(conf)

    val samples = 1000
    val pi = new PiCalculator(samples, sc)
    print(pi.montecarlo)

    sc.stop()
  }
}
