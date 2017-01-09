---
layout: post
title: "Bloom-filter join in Spark"
date: 2017-01-08 14:33:32 -0800
comments: true
categories: 
---

When it comes to join optimization techniques, map-side(broadcast) join is an obvious candidate, however it doesn't work when the data set is not small enough to fit into memory. But we can extend this approach if we find a representation of the data set that is small, e.g. Bloom filter.

A Bloom filter is a space-efficient probabilistic data structure used to test whether a member is an element of a set(<10 bits per element are required for a 1% false positive rate). Recently I ported a job from Apache Pig to Spark which gained significant speedup by using Bloom filter. This job joins 60 days of mobile-device pairs to cookies from a partner. The optimized join job goes like this: first build partial Bloom filters in each partition of smaller data, which can be done using mapPartitions in Spark. They are collected into driver node and merged into a full Bloom filter, which is then distributed to all executors and used to filter out large portions of the data that will not find a match when joined.

Implementation of Bloom filter in MapReduce is cumbersome in that you have to explicitly use DistributedCache, and and write a lot of boilerplate code that handles file system I/O when writing it to HDFS and read it back in second stage. Mapper with only one cpu core is inefficient when the filter is large. While MultithreadedMapper is possible, it's difficult to write code that is thread-safe. Spark's executor is a thread-pool by design, you can easily assign many cpu cores to an executor. And Spark has a nice feature called Broadcast Variables that saves you a lot of effort, allowing you to distribute large data using efficient broadcast algorithms to reduce communication cost.


The good news is we don't need to write our own Bloom filter from scratch, instead we can use  `org.apache.spark.util.sketch.BloomFilter` which is largely based on Google's Guava. Under the hood it's a **long[]** representing a bit array, it has the advantage over other implementation in that the number of inserted bits can be larger than 4bn.

{% codeblock lang:scala %}
import org.apache.spark.util.sketch.BloomFilter

//reading files
val bigRDD = ...
val smallRDD = ...
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
{% endcodeblock %}


In order for this to run on the Hadoop cluster, we need to set sufficiently large memory on both driver and executors to hold the underlying bit array, depending on the value of false positive probability we've set above.  In addition we need to increase the maximum allowable size of Kryo serialization buffer, otherwise we'll see exceptions from Kryo:

{% codeblock lang:scala %}

spark.kryoserializer.buffer.max  512m
 --driver-memory 100g \
 --driver-cores 24 \
 --num-executors 130 \
 --executor-memory 50g \
 --executor-cores 8 \
{% endcodeblock %}

