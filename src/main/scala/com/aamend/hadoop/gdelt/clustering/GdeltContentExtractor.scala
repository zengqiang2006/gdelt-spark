package com.aamend.hadoop.gdelt.clustering

import java.io.File

import com.aamend.hadoop.gdelt.io.{ArticleWritable, EventInputFormat}
import com.gravity.goose.{Configuration, Goose}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

object GdeltContentExtractor extends App {

  val numArgs = 3
  if (args.length < numArgs) {
    System.err.println("Bad number of arguments. Expected " + numArgs + ", actual " + args.length)
    System.err.println("usage: GdeltContentExtractor <inputDir> <outputDir> <parallel>")
    System.exit(1)
  }

  val inputDir: String = args(0)
  val outputDir: String = args(1)
  val parallel: Int = args(2).toInt

  val jsonDir: String = outputDir + File.separator + "json"
  val contentDir: String = outputDir + File.separator + "content"

  val sc = new SparkContext(new SparkConf().setAppName("GdeltContentExtractor"))
  val hdpConf = sc.hadoopConfiguration

  // Convert GDELT to JSON
  val input = sc.newAPIHadoopFile(inputDir, classOf[EventInputFormat], classOf[Text], classOf[Text], hdpConf).map { case (k, v) => v.toString}.repartition(parallel)

  // Parse JSON and get CAMEO and URL
  val cameoUrlJson = input.map { line =>
    val json = parse(line)
    implicit val formats = DefaultFormats.withDouble
    (CameoUrl((json \ "cameo").extract[Int], (json \ "url").extract[String]), line)
  }

  // Extract webContent based on URL (need to use more partitions for browsing)
  val articles = cameoUrlJson
    .map { case (k, v) => (k.cameo, extractContentFromUrl(k.url))}
    .filter(_._2.content != null)
    .map { case (k, v) => (k, WebContent(v.title, v.content))}

  // Save CAMEO + content to HDFS
  articles
    .map { case (k, v) => (new IntWritable(k), new ArticleWritable(v.title, v.content))}
    .saveAsNewAPIHadoopFile(contentDir, classOf[IntWritable], classOf[ArticleWritable], classOf[SequenceFileOutputFormat[IntWritable, ArticleWritable]], hdpConf)

  // Output CAMEO + JSON to HDFS
  cameoUrlJson
    .map { case (k, v) => (new IntWritable(k.cameo), new Text(v))}
    .saveAsNewAPIHadoopFile(jsonDir, classOf[IntWritable], classOf[Text], classOf[SequenceFileOutputFormat[IntWritable, Text]], hdpConf)


  def extractContentFromUrl(url: String): WebContent = {

    try {
      val config = new Configuration
      config.setEnableImageFetching(false)
      config.setBrowserUserAgent("Mozilla")
      val article = new Goose(config).extractContent(url)
      WebContent(article.title, article.cleanedArticleText)
    } catch {
      case e: Exception => WebContent(null, null)
    }
  }

  case class WebContent(title: String, content: String)
  case class CameoUrl(cameo: Int, url: String)

}
