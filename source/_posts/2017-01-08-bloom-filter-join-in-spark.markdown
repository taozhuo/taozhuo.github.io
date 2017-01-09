---
layout: post
title: "Bloom-filter join in Spark"
date: 2017-01-08 14:33:32 -0800
comments: true
categories: 
---

When it comes to join optimization techniques, map-side(broadcast) join is an obvious candidate, however it doesn't work when the data set is not small enough to fit into memory. But we can extend this approach if we find a representation of the data set that is small, e.g. Bloom filter.

A Bloom filter is a space-efficient probabilistic data structure used to test whether a member is an element of a set.

Implementation of Bloom filter in MapReduce is cumbersome in that you have to explicitly use DistributedCache, and and write a lot of boilerplate code that handles file system I/O when writing it to HDFS and read it back in second stage. Spark has a nice feature called Broadcast Variables that saves you a lot of effort, and allows you to distribute the data using efficient broadcast algorithms to reduce communication cost.

We don't need to write our own Bloom filter from scratch, instead we can use **org.apache.spark.util.sketch.BloomFilter** which is largely based on Google's Guava. Under the hood it's a **long[]** representing a bit array, it has the advantage over other implementation that the number of inserted bits can be larger than 4bn.

{% codeblock lang:scala %}

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


