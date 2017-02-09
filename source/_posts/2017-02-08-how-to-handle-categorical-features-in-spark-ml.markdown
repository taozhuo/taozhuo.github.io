---
layout: post
title: "How to Handle Categorical Features in Spark ML(Random Forest)"
date: 2017-02-08 15:07:49 -0800
comments: true
categories: 
---

Random Forest(RF) is an ensemble training algorithm that is very popular in data science and machine learning communities. It uses bagging and feature subsets to train on 100s of decision trees to reduce overfitting. It's also fast because it's an Embarrassingly parallel task. In production runs using scikit-learn's implementation, it takes less than one hour to train 5 million samples on 30 cpu cores, and several hours if doing a grid search on two grid points with cross validation. Compared to other ensemble algorithms like Gradient Boosted Trees, it's much faster.

RF doesn't require feature normalization, and can handle categorical fetures. However this is up to specific implementations. In Spark ML, it's a little tricky. Before Spark 2.0, MLlib's RDD-based API lets you pass in `categoricalFeaturesInfo`, which is a mapping from feature index to number of categories, into method `RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)`. However since 2.0 the primary API is the DataFrame-based API in the spark.ml package, which doesn't reach feature parity until 2.2.

There are still several ways to handle categorical fetures. The easiest one is treat all features as continuous variable, e.g. you can do some string splitting on the text file you read in, wrap them in `LabeledPoint` objects and convert RDD into DataFrame, which is then passed to `RandomForestClassifier`:

{% codeblock lang:scala %}
    val testDF = testRDD.map { line =>
      val list = line.split(',')
      val label = list(0).toDouble
      val features = list.slice(1,100).map(_.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }.toDF("label", "features")
{% endcodeblock %}

However, you do it the easy way at the expense of the model quality. In my testing, it only reaches AUC-PR score of 0.45 compared to 0.49 in scikit-learn. 

Another way is build a pipeline that combines feature transformers and model estimators. We can use `VectorIndexer` to automatically identify categorical features based on threshold for the number of values a categorical feature can take. If a feature is found to have > maxCategories distinct values then it is declared continuous, otherwise it's declared categorical feature. By default it's 4, but you can try different values:

{% codeblock lang:scala %}
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(5)
      .fit(train)
{% endcodeblock %}

Everything looks fine up to this point. But when you run model prediction, you immediately see the following error:

{% codeblock lang:scala %}
 java.util.NoSuchElementException: key not found: 3.0
 	at scala.collection.MapLike$class.default(MapLike.scala:228)
	at scala.collection.AbstractMap.default(Map.scala:58)
	at scala.collection.MapLike$class.apply(MapLike.scala:141)
	at scala.collection.AbstractMap.apply(Map.scala:58)
	at org.apache.spark.ml.feature.VectorIndexerModel$$anonfun$10$$anonfun$apply$4.apply(VectorIndexer.scala:316)
	at org.apache.spark.ml.feature.VectorIndexerModel$$anonfun$10$$anonfun$apply$4.apply(VectorIndexer.scala:315)
	at scala.collection.immutable.HashMap$HashMap1.foreach(HashMap.scala:224)
	at scala.collection.immutable.HashMap$HashTrieMap.foreach(HashMap.scala:403)
	at scala.collection.immutable.HashMap$HashTrieMap.foreach(HashMap.scala:403)
	at org.apache.spark.ml.feature.VectorIndexerModel$$anonfun$10.apply(VectorIndexer.scala:315)
	at org.apache.spark.ml.feature.VectorIndexerModel$$anonfun$10.apply(VectorIndexer.scala:309)
	at org.apache.spark.ml.feature.VectorIndexerModel$$anonfun$11.apply(VectorIndexer.scala:351)
	at org.apache.spark.ml.feature.VectorIndexerModel$$anonfun$11.apply(VectorIndexer.scala:351)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.apply(Unknown Source)
	...
{% endcodeblock %}

This is caused by unknown categorical features that are not in VectorIndexer's map(SPARK-12375). The issue is not resolved yet, but you can try installing the patch.

You might wonder, is there a way to pass in category info into the DataFrame-based model? The answer is, sort of. Since Spark 1.2, there is metadata field(SPARK-3569) in the schema that can be used by machine learning applications to store information like categorical/continuous, number categories, category-to-index map. It's keyed by "ml_attr". This field is empty by default, it'll generate something for you if you run VectorIndexer:

{% codeblock lang:scala %}
scala> train.schema(1).metadata
res6: org.apache.spark.sql.types.Metadata = {}
scala> field.metadata.getMetadata("ml_attr")
java.util.NoSuchElementException: key not found: ml_attr
...
{% endcodeblock %}

`RandomForestClassifier.scala` then checks the metadata field to get the list of categorical information:

{% codeblock lang:scala %}
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
{% endcodeblock %}

If we dig a little deeper to see how the mapping is generated, there is an object called `NominalAttribute`:

{% codeblock lang:scala %}
          attr match {
            case _: NumericAttribute | UnresolvedAttribute => Iterator()
            case binAttr: BinaryAttribute => Iterator(idx -> 2)
            case nomAttr: NominalAttribute =>
              nomAttr.getNumValues match {
                case Some(numValues: Int) => Iterator(idx -> numValues)
{% endcodeblock %}

The bad news is there is no public API yet(SPARK-8515) to generate an `NominalAttribute` object. The only factory method looks like this:

{% codeblock lang:scala %}
  object NominalAttribute extends AttributeFactory {
	   /** The default nominal attribute. */
	   final val defaultAttr: NominalAttribute = new NominalAttribute
	   private[attribute] override def fromMetadata(metadata: Metadata): NominalAttribute = {
	...
{% endcodeblock %}
 
Of course, you can always hack the core files and build your Spark. Have fun!




