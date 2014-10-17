package com.aamend.hadoop.gdelt.analytics

import com.aamend.hadoop.gdelt.io.EventInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.Text


object GoldsteinSqlSpark extends App {

  if(args.length < 2){
    System.err.println("Usage : Gdelt <input> <output>")
    System.exit(1)
  }

  // Access spark context and configuration
  val sc = new SparkContext(new SparkConf().setAppName("GoldsteinSqlSpark"))

  // Access spark SQL context
  val sqlContext = new SQLContext(sc)

  // Read file from HDFS - Use GdeltInputFormat
  val input = sc.newAPIHadoopFile(args(0),classOf[EventInputFormat],classOf[Text],classOf[Text])

  // Get values from Hadoop Key / Values pair
  val values = input.map{ case (k,v) => v}

  // Need to deserialize Text
  val json = values.map(t => t.toString)

  // Load JSON (String) as a SQL JsonRDD
  val jsonRDD = sqlContext.jsonRDD(json)

  // Make sure schema is correct
  //jsonRDD.printSchema()

  // Register our GDELT dataset as a Table
  jsonRDD.registerTempTable("gdelt")

  // Execute my SQL
  val relations = sqlContext.sql(
    "SELECT day, AVG(goldsteinScale) " +
      "FROM gdelt WHERE " +
      "actor1Geo.countryCode = 'FR' AND actor2Geo.countryCode = 'UK' " +
      "GROUP BY day")

  // Output result
  relations.map(row => row(0) + "\t" + row(1)).saveAsTextFile(args(1))

}
