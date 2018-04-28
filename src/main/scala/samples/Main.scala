package samples

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import samples.calculators.PiCalculator

object Main {

  def initContext: SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .setAppName("Spark Testing")

    val sc = SparkContext
      .getOrCreate(conf)

    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)

    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = initContext

    val samples = 1000
    val pi = new PiCalculator(samples, sc)
    print(pi.montecarlo)

    sc.stop()
  }
}
