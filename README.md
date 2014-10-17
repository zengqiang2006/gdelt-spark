GDELT public data set
======================

Spark / Scala analytics jobs against GDELT dataset (http://www.gdeltproject.org/)

## Content Clustering

### Exercise: 

* Download actual Content from GDELT event based on provided URL
* Cluster article based on content (IDF-TF clustering)
* Track daily clusters over the time

#### Extract Content - Spark Job:

* Read TSV values as Json using Hadoop RecordReader
* For each URL, download and parse actual content using goose library (https://github.com/GravityLabs/goose)
* Try to parallelize tasks at most (web crawling might be quite a heavy process)
* Extract word from webcontent using Lucene english analyzer

#### Cluster daily data - Spark Job:

* Parse each webContent into Lucene english analyzer
* Create sparse vectors using Spark MLLib
* Train MLLib KMeans on 1 day worth of data
* Cluster the 1 day worth of data using created clusters

### Execution:

`./spark-submit --class com.dataphi.hadoop.gdelt.GdeltContentExtractor --master yarn-cluster /path/to/my.jar input/gdelt/20140909.export.CSV /output/gdelt 20`

* (Required) Input daily GDELT file
* (Required) Output Directory
* (Optional) The degree of parallelism for web browsing (suggested value is 20-100) for 1 day worth of data

## Build

`mvn clean package`

This will create a shaded JAR including all dependencies required for the project execution

## Misc Analytics

* GkgTopN : Extract the topN themes, persons and organization from GKG dataset
* GoldsteinSqlSpark: Use SQLSpark to query GKG dataset
* SparkMovingAverage: Create a moving window average for supplied data points

## Misc result

* Using some above spark jobs, I've been able to create the following graph showing the average goldstein scale between russia and ukraine over the past 30 years

![alt tag](https://raw.github.com/aamend/gdelt-spark/master/src/main/resources/MovingAverageGoldstein.png)


## Authors

Antoine Amend <antoine.amend@gmail.com>



