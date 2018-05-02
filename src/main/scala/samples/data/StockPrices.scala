package samples.data

import org.apache.spark.sql.{DataFrame, SparkSession}
import samples.utils.SparkUtils

/*
 * Copyright 2018 .
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class StockPrices(session: SparkSession) {

  /**
    * Head of prices.csv:
    *
    * date,symbol,open,close,low,high,volume
    * 2016-01-05 00:00:00,WLTW,123.43,125.839996,122.309998,126.25,2163600.0
    * 2016-01-06 00:00:00,WLTW,125.239998,119.980003,119.940002,125.540001,2386400.0
    * 2016-01-07 00:00:00,WLTW,116.379997,114.949997,114.93,119.739998,2489500.0
    */
  val dataFrame: DataFrame = session.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .load(s"src/main/resources/prices.csv")


  def example1(): Double = {
    0.0
  }

  def example2(): List[String] = {
    "Company1" :: "Company2" :: "Company3" :: Nil
  }

}

object Main {
  def main(args: Array[String]): Unit = {

    val sc = SparkUtils.initContext
    val session: SparkSession = SparkSession.builder()
      .config(sc.getConf)
      .getOrCreate()

    val csvReader = new StockPrices(session)
    val pricesDataFrame = csvReader.dataFrame

    pricesDataFrame.createOrReplaceTempView("stock_prices")
    session.sql("SELECT min(low), max(low), symbol FROM stock_prices GROUP BY symbol").show()
  }
}
