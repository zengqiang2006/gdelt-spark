package com.aamend.hadoop.gdelt.analytics

import java.io.File

import com.aamend.hadoop.gdelt.io.GkgInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.json4s.jackson.JsonMethods._
import org.json4s._

object GkgTopN extends App {

  if (args.length < 2) {
    System.err.println("Usage: GkgTopNPerson <gdelt.dir> <output.dir>")
    System.exit(1)
  }

  val inputDir = args(0)
  val outputDir = args(1)
  val themesDir = outputDir + File.separator + "themes"
  val personsDir = outputDir + File.separator + "persons"
  val orgsDir = outputDir + File.separator + "organizations"

  val sc = new SparkContext(new SparkConf().setAppName("GkgTopN"))

  // Read Hadoop Json file
  // Use Json value only
  val jsonRDD = sc.newAPIHadoopFile(inputDir, classOf[GkgInputFormat], classOf[Text], classOf[Text], new Configuration)
    .map { case (k, v) => v.toString}

  // Extract themes, persons, organizations tuples
  val extract = jsonRDD.flatMap { str =>
    implicit lazy val formats = org.json4s.DefaultFormats
    val json = parse(str)
    val v1 = (json \ "persons").extract[Seq[String]].map(l => (("p", l), 1))
    val v2 = (json \ "organizations").extract[Seq[String]].map(l => (("o", l), 1))
    val v3 = (json \ "themes").extract[Seq[String]].map(l => (("t", l), 1))
    v1.union(v2).union(v3)
  }

  // Sum up all tuples
  // Sort all tuples by value
  // Add to cache (3 iterations will be needed)
  val sort = extract.reduceByKey(_ + _)
    .map { case ((k1, k2), v) => (v, (k1, k2))}
    .sortByKey(false)
    .map { case (v, (k1, k2)) => ((k1, k2), v)}
    .cache()

  // Output themes
  sort
    .filter(_._1._1 == "t")
    .map { case ((k1, k2), v) => k2 + "\t" + v}
    .saveAsTextFile(themesDir)

  // Output persons
  sort
    .filter(_._1._1 == "p")
    .map { case ((k1, k2), v) => k2 + "\t" + v}
    .saveAsTextFile(personsDir)

  // Output organizations
  sort
    .filter(_._1._1 == "o")
    .map { case ((k1, k2), v) => k2 + "\t" + v}
    .saveAsTextFile(orgsDir)


}
