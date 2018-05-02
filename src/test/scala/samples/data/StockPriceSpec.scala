package samples.data

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import samples.utils.SparkUtils

class StockPriceSpec extends FlatSpec with Matchers with BeforeAndAfter {
  var stockPrices: StockPrices = _
  var session: SparkSession = _

  before {
    val sc = SparkUtils.initContext
    session = SparkSession.builder()
      .config(sc.getConf)
      .getOrCreate()
    stockPrices = new StockPrices(session)
  }

  // Example of how to use the API
  "SNA closing price" should "be 125.839996 on 2016-01-20" in {
    val dataFrame = stockPrices.dataFrame
    dataFrame.createOrReplaceTempView("stock_prices")
    val closingPrice = session.sql("SELECT close FROM stock_prices")
      .select("close")
      .first()
      .getAs[Double]("close")

    closingPrice should be(125.839996)
  }

  "SNA average opening price" should "be 150 in January" in {
    stockPrices.example1 should be(150)
  }

  "The three highest prices for March the 1st" should "be those of SNA, WDP and ISP" in {
    stockPrices.example2 should be("SNA" :: "WDP" :: "ISP" :: Nil)
  }
}
