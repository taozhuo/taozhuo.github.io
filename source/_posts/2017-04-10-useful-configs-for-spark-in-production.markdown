---
layout: post
title: "Useful Configs for Spark in Production"
date: 2017-04-10 11:07:21 -0700
comments: true
categories: 
---

`spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` Sometimes your Spark jobs finished all stages, but Web UI hangs there forever. The reason is if your job generates a lot of files to commit, and `commitTask` method will rename the temporary directories before `commitJob` merges every task output file into the final output folder, this process can be slow. At first I thought I can use `DirectParquetOutputCommitter` instead of default committer. However it is removed in 2.0 because speculation might cause loss of data:  [SPARK-10063](https://issues.apache.org/jira/browse/SPARK-10063). Then I found the root cause is that committer algo version is set to 1 by default in Hadoop, which sets the commit to single-threaded and waits until all tasks have completed. Change the version number to 2 so that this algorithm will reduce the output commit time for large jobs by having the tasks commit directly to the final output directory as they were completing.

`spark.yarn.executor.memoryOverhead=15360` This is the amount of OS overhead memory to be allocated to each executor. By default it's set to executorMemory * 0.10, but it's way too little when you want to shuffle large amount of data. Each task is fetching shuffle files over NIO channel. The buffers required are to be allocated from OS overheads. Give it a large number to make shuffling more stable.

`spark.executor.extraJavaOptions -XX:MaxDirectMemorySize=30g` Direct buffer memory is located within off-heap region, usually the limit of its size is small compare to heap size. When I run jobs that write parquet files with wide and complicated schema, I often see the following error. This is due to parquet snappy codec allocates large off-heap buffers for decompression:  [SPARK-4073](https://issues.apache.org/jira/browse/SPARK-4073). There are two ways to fix it, one is use Lzo compression codec, the other is set a much higher off heap memory limit.  

{% codeblock lang:scala %}
org.apache.spark.SparkException: Task failed while writing rows
	at org.apache.spark.sql.execution.datasources.DefaultWriterContainer.writeRows(WriterContainer.scala:261)
	at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand$$anonfun$run$1$$anonfun$apply$mcV$sp$1.apply(InsertIntoHadoopFsRelationCommand.scala:143)
	at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand$$anonfun$run$1$$anonfun$apply$mcV$sp$1.apply(InsertIntoHadoopFsRelationCommand.scala:143)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:70)
	at org.apache.spark.scheduler.Task.run(Task.scala:86)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:274)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:745)
Caused by: java.lang.OutOfMemoryError: Direct buffer memory
	at java.nio.Bits.reserveMemory(Bits.java:658)
	at java.nio.DirectByteBuffer.<init>(DirectByteBuffer.java:123)
	at java.nio.ByteBuffer.allocateDirect(ByteBuffer.java:306)
	at org.apache.parquet.hadoop.codec.SnappyCompressor.setInput(SnappyCompressor.java:97)
{% endcodeblock %}

`dfs.datanode.socket.write.timeout=3000000`, `dfs.socket.timeout=3000000`. When connection to datanode is bad, saving parquet files into HDFS sometimes gives you errors like the following. You can fix it by setting a higher datanode socket write timeout in Hadoop settings.

{% codeblock lang:scala %}
 Suppressed: java.io.IOException: The file being written is in an invalid state. Probably caused by an error thrown previously. Current state: COLUMN
 at org.apache.parquet.hadoop.ParquetFileWriter$STATE.error(ParquetFileWriter.java:146)
 at org.apache.parquet.hadoop.ParquetFileWriter$STATE.startBlock(ParquetFileWriter.java:138)
 at org.apache.parquet.hadoop.ParquetFileWriter.startBlock(ParquetFileWriter.java:195)
 at org.apache.parquet.hadoop.InternalParquetRecordWriter.flushRowGroupToStore(InternalParquetRecordWriter.java:153)
 at org.apache.parquet.hadoop.InternalParquetRecordWriter.close(InternalParquetRecordWriter.java:113)
 at org.apache.parquet.hadoop.ParquetRecordWriter.close(ParquetRecordWriter.java:112)
{% endcodeblock %}

There are a lot of other timeouts in Spark configs, often times we trade off some performance for higher reliability. e.g. set `spark.executor.heartbeatInterval=120s` and `spark.network.timeout=600s` if you are running different workloads such as Pig/MapReduce or ad-hoc data science jobs on a busy Yarn cluster, as they have unpredictable impact on the networking condition. Just be careful some of the timeouts follow some rules, e.g. `heartbeatInterval` should be significantly less than `spark.network.timeout`, otherwise you will see strange errors.

 
