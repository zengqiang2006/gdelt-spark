package com.aamend.hadoop.gdelt.analytics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SparkMovingAverage extends App {

  if (args.length < 3) {
    System.err.println("Usage: SparkMovingAverage <input[yyyyMMdd,goldstein]> <window> <output[yyyyMMdd,goldstein]>")
    System.exit(1)
  }

  // Access spark context and configuration
  val sc = new SparkContext(new SparkConf().setAppName("SparkMovingAverage"))

  // Create our window RDD
  val inputFile = args(0)
  val window = args(1).toInt
  val outputDir = args(2)

  val input = sc.textFile(inputFile)

  // Input data should be a CSV : date(yyyyMMdd),goldsteinScale(double)
  // I assume data is already a 1 day average
  val goldsteinAvg1Rdd = input.map( line => line.split(",") ).map( tok => (tok(0), java.lang.Double.parseDouble(tok(1))))

  // Count initial number of records
  val avg1Count = goldsteinAvg1Rdd.count()

  // Prepare our time window RDD
  val day = 3600 * 1000 * 24
  val windowRdd = sc.parallelize((0 to window - 1).toList, 1)

  // Unleash the spark power !
  val goldsteinAvgWRdd = goldsteinAvg1Rdd

    // Cartesian product of Goldstein with window RDD
    .cartesian(windowRdd)

    // Parse each date, and for each, add the window time (0 -> window - 1)
    // Also increment a window counter (will be used for the average)
    // Also create a pivot (negative for the first pivot, null and greater for others)
    .map{ case ( ( gd , gs ) , w ) => ( new java.text.SimpleDateFormat("yyyyMMdd").parse(gd).getTime() + ( w * day ) , ( gs , 1 , -1 + w ) ) }

    // Sum up all goldstein for a same window date
    // Increment window counter
    // Always keep the lowest pivot (-1 for best case)
    .reduceByKey{ case ( ( gs1 , wc1 , wp1 ) , ( gs2 , wc2 , wp2 ) ) => ( gs1 + gs2 , wc1 + wc2 , Math.min(wp1, wp2) )}

    // Window is valid if and only if the minimum pivot is -1
    .filter{case ( gd , ( gs , wc , wp ) ) => wp == - 1 }

    // Parse the time back into date
    // Compute window average
    .map{ case ( gd , ( gs , wc , wp ) ) => ( new java.text.SimpleDateFormat("yyyyMMdd").format(new java.util.Date(gd)) , gs / wc ) }

    // Sort average records by date
    .sortByKey(true)

    // Output as a string
    .map{ case ( gd , gs ) => gd + "," + gs }

  // Count the new records
  val avgWCount = goldsteinAvgWRdd.count()

  // Make sure we still have all our records
  if ( avgWCount != avg1Count ) {
    System.err.println("Some data are missing from Window average")
    System.exit(1)
  }

  // Write down to HDFS
  goldsteinAvgWRdd.saveAsTextFile(outputDir)


}
