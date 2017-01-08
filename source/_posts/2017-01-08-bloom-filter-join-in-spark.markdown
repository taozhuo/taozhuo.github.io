---
layout: post
title: "Bloom-filter join in Spark"
date: 2017-01-08 14:33:32 -0800
comments: true
categories: 
---

Bloom filter is a probabilistic data structure:

{% codeblock lang:scala %}
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("bloom-filter-join")
    val sc = new SparkContext(conf)

    //val rootLogger = Logger.getRootLogger()
    //rootLogger.setLevel(Level.DEBUG)

    //big file schema: date, uuid1, uuid2 .....
    val bigFile = sc.newAPIHadoopFile(args(0), classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],classOf[org.apache.hadoop.io.Text])
    val bigRDD: RDD[(String, String)] = bigFile.map(_._2.toString).map(x=>(x.split("\t")(1), x.split("\t")(2)))

    //small file schema: id, pfc
    val smallFile = sc.newAPIHadoopFile(args(1), classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],classOf[org.apache.hadoop.io.Text])
    val smallRDD = smallFile.map(_._2.toString).map(x=>(x.split("\t")(0), 1))
    val cnt:Long = smallRDD.count()

    // 1a. create bloom filters for smaller data locally on each partition
    // 1b. merge them in driver
    val bf = smallRDD.mapPartitions { iter =>
      val b = BloomFilter.create(cnt, 0.1)  //false positive probability=0.1
      iter.foreach(i => b.putString(i._1))
      Iterator(b)
    }.reduce((x,y) => x.mergeInPlace(y))

    // 2. driver broadcasts bloom-filter
    sc.broadcast(bf)

    // 3. use bloom-filter to filter big data set
    val filtered= bigRDD.filter(x => bf.mightContain(x._1) && bf.mightContain(x._2))

    // 4. join big data set and small data set
    filtered.join(smallRDD).saveAsTextFile(args(2))
  }
{% endcodeblock %}

... which is shown in the screenshot below:
{% img left /images/screenshot.png 350 350 'image' 'images' %}


