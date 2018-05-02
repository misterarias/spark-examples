# Spark Examples

Meant to help learning the use of Spark.
It also provides a nice [pom.xml](./pom.xml) that sets up a maven Spark project with:
* Spark 2.3 core and SQL support
* Scala 2.12
* [Scalatest](http://www.scalatest.org/) testing support

## What does this *not* do (yet)?
* Configure uber-jar creation for cluster deployment
* Spark Structured Streaming testing

## Requirements

You need to copy the _prices.csv_ dataset for Stocks to `src/main/resources`.
