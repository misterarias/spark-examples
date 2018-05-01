package samples.data

import org.apache.spark.SparkContext
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
class CsvRead(sc: SparkContext) {
  def run() {
    val session: SparkSession = SparkSession.builder()
      .config(sc.getConf)
      .getOrCreate()

    val dataFrame: DataFrame = session.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("src/main/resources/data.csv")

    dataFrame.createOrReplaceTempView("people")

    session.sql("SELECT name, age FROM people WHERE age < 43").show()
    session.sql("SELECT avg(age) FROM people").show()
  }
}

object Main {
  def main(args: Array[String]): Unit = {

    val sc = SparkUtils.initContext
    new CsvRead(sc).run()
  }
}
