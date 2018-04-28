package main.scala

/*
 * Copyright 2018 - Schibsted.
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
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .setAppName("Spark Testing")

    val sc = SparkContext
      .getOrCreate(conf)

    val samples = 1000
    val numbers = 1.to(samples).map(x => x - math.random*x)
    print(sc.parallelize(numbers).sum() / samples)

    sc.stop()
  }
}