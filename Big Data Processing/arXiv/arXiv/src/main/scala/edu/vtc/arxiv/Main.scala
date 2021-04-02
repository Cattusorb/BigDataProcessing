
package edu.vtc.arxiv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import play.api.libs.json._


object Main {

  def getAuthorsWithTitle(document: JsValue) = {
    val authorsArray = (document \ "authors_parsed").get.as[JsArray].value
    val title = (document \ "title").get.as[JsString].value

    for (author <- authorsArray ) yield {
      val lastName = (author.as[JsArray] \ 0).get.as[JsString].value
      val firstName = (author.as[JsArray] \ 1).get.as[JsString].value
      (firstName + " " + lastName, title)
    }
  }

  def main(args: Array[String]): Unit = {

    // Create a Spark Context.
    val conf = new SparkConf().setAppName("arXiv")
    val sc = new SparkContext(conf)

    // Load input data.
    val input = sc.textFile("/user/hadoop/arxiv-metadata-small.json")

    val authorsWithTitles = input map { Json.parse } flatMap { getAuthorsWithTitle }

    val parsedInput = authorsWithTitles.reduceByKey(_ + _)

    parsedInput.saveAsTextFile("output")
  }
}
