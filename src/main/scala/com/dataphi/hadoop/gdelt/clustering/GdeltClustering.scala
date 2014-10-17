package com.dataphi.hadoop.gdelt.clustering

import java.io.{File, StringReader}

import com.dataphi.hadoop.gdelt.io.ArticleWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}

object GdeltClustering extends App {

  val numArgs = 1
  if (args.length < numArgs) {
    System.err.println("Bad number of arguments. Expected " + numArgs + ", actual " + args.length)
    System.err.println("usage: GdeltClustering <dir>")
    System.exit(1)
  }

  val dir: String = args(0)

  val contentDir: String = dir + File.separator + "content"
  val dataDir: String = dir + File.separator + "points"

  val sc = new SparkContext(new SparkConf().setAppName("GdeltClustering"))
  val hdpConf = sc.hadoopConfiguration

  // Read extracted Content
  val input = sc.newAPIHadoopFile(contentDir, classOf[SequenceFileInputFormat[IntWritable, ArticleWritable]], classOf[IntWritable], classOf[ArticleWritable], hdpConf)

  // Parse webContent
  val articles = input.map{ case (k,v) => (k.get(), tokenize(v.getContent))}

  // Create TermFrequency RDD[Vectors]
  val tf = articles.map { case (k, v) => new HashingTF().transform(v)}

  // Create InverseDocumentFrequency - TermFrequency RDD[Vectors]
  tf.cache()
  val tfidf = new IDF().fit(tf).transform(tf)

  // Train KMeans
  tfidf.cache()
  val count = tfidf.count()
  val model = KMeans.train(tfidf, Math.sqrt(count / 2).toInt, 40)
  println("****************************************")
  println("COST = " + model.computeCost(tfidf))
  println("****************************************")

  // Cluster data
  println("****************************************")
  println("CLUSTERING " + count + " VECTORS")
  println("****************************************")

  // Save CLUSTER_ID + CAMEO to HDFS
  articles
    .map { case (k, v) => (new IntWritable(model.predict(new HashingTF().transform(v))), new IntWritable(k)) }
    .saveAsNewAPIHadoopFile(dataDir, classOf[IntWritable], classOf[IntWritable], classOf[TextOutputFormat[IntWritable, IntWritable]], hdpConf)


  def tokenize(content: String): Seq[String] = {

    val tReader = new StringReader(content)
    val analyzer = new EnglishAnalyzer()
    val tStream = analyzer.tokenStream("contents", tReader)
    val term = tStream.addAttribute(classOf[CharTermAttribute])
    tStream.reset()

    val result = scala.collection.mutable.ArrayBuffer.empty[String]
    while (tStream.incrementToken()) {
      val termValue = term.toString
      if (!(termValue matches ".*[\\d\\.].*")) {
        result += term.toString
      }
    }
    result
  }

}
