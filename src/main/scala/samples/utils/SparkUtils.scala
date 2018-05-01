package samples.utils

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
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
}
