---
layout: post
title: "How to Tune Gradient-Boosted Trees in Spark"
date: 2017-02-27 14:54:30 -0800
comments: true
categories: 
---
Gradient-Boosted trees(GBT) is one of the tree ensemble models that is popular among data science community. The most popular implementation - XGBoost, is used by half of winning teams competing in Kaggle challenges. However, it requires many native libraries that do not play well with JVM-based production system such as Hadoop and Spark, adding a lot of operational complexities. Sometimes it's not the efficient way. E.g. in the prediction phase of the end-to-end ML pipeline, we need to apply the trained model on a large amount of data(tens of billions of records). This is done in Python via Hadoop streaming, whcih consumes a lot of memory when loading batches of data fore prediction.

GBT in Spark ML/Mllib. there are a lot of variants in tree ensembles in old RDD-based API and new DataFrame-based API, but both point to the same Random forest implementation in [RandomForest.scala](https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/tree/impl/RandomForest.scala). Each iteration within the main loop of [GradientBoostedTrees.scala](https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/tree/impl/GradientBoostedTrees.scala) calls `RandomForest` and will stop early if some validation condition is satisfied. [GBTClassifier.scala](https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/classification/GBTClassifier.scala) is a wrapper class if you want to plug it into the Spark ML pipeline APIs, but it doesn't provide a method to output predicted probabilities. I still use the old RDD-based wrapper [GradientBoostedTrees](https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/tree/GradientBoostedTrees.scala) so I can calculate `AUPRC` and `AUROC` scores. Below is a code snippet on how to train the model and `AUPRC` & `AUROC` calculations:

{% codeblock lang:scala %}
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
	//boosting parameters
    boostingStrategy.numIterations = 100
    boostingStrategy.learningRate = 0.15
    boostingStrategy.setValidationTol(0.01)
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]( 57->3, 58->4,59->3,60->2,61->2, 62->3, 63->2, 64->2,
      65->3, 66->2, 67->2, 68->3, 69->2, 70->2, 72->3, 73->3, 74->3, 93->5, 94->3, 96->3, 99->3, 102->3, 105->3,
      108->3, 111->3, 113->10, 114->10)
	//tree parameters
    boostingStrategy.treeStrategy.useNodeIdCache = true
    boostingStrategy.treeStrategy.subsamplingRate = 0.8
    boostingStrategy.treeStrategy.maxBins = 10
    boostingStrategy.treeStrategy.maxDepth = 12
    boostingStrategy.treeStrategy.minInstancesPerNode = 32
    Logger.getLogger(GradientBoostedTrees.getClass).setLevel(Level.DEBUG)
    val model = GradientBoostedTrees.train(train, boostingStrategy)
    val gbt = new GradientBoostedTrees(boostingStrategy)
{% endcodeblock %}

{% codeblock lang:scala %}
    // Get class probability
    val treePredictions = test.map { point => model.trees.map(_.predict(point.features)) }
    val treePredictionsVector = treePredictions.map(array => Vectors.dense(array))
    val treePredictionsMatrix = new RowMatrix(treePredictionsVector)
    val learningRate = model.treeWeights
    val learningRateMatrix = Matrices.dense(learningRate.size, 1, learningRate)
    val weightedTreePredictions = treePredictionsMatrix.multiply(learningRateMatrix)
    val classProb = weightedTreePredictions.rows.flatMap(_.toArray).map(x => 1 / (1 + Math.exp(-1 * x)))
    val labels = test.map(_.label)
    val predictionAndLabels = (classProb zip labels).persist(MEMORY_ONLY)
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)
    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
{% endcodeblock %}

_Model Tuning_  
GBT in Spark is based on J.H. Friedman's paper in 1999 - "Stochastic Gradient Boosting." It trains a series of weakk classifiers, each tree is built on the pseudo residuals of the previous model, and finally the best model that minimizes the loss function will be selected.

**categorical features**  
Before tuning the model, I set categorical features in `boostingStrategy.treeStrategy.categoricalFeaturesInfo`, there is some lift immediately:  

cat. info        |   AUPRC            |
  -------------- |--------------------| 
     yes         |  0.5085462235      | 
     no	         |  0.5094705132      |  


**default configs:**  
There are different ways to configure gradient boosting algorithm, this [post](http://machinelearningmastery.com/configure-gradient-boosting-algorithm/), for instance, gives ideas based on different implementations.
Basically there are two types of parameter to be tuned here – tree based and boosting parameters. Default configs are usually linked to internal implementation. In spark, `org.apache.spark.mllib.tree.configuration.BoostingStrategy` has the following default boosting parameters:

{% codeblock lang:scala %}
 numIterations: Int = 100,
 learningRate: Double = 0.1,
 validationTol: Double = 0.001
 {% endcodeblock %}   
 
**learning rate**  
Boosting parameters are mostly regularization techniques that reduce overfitting and penalize the complexities of the models. One strategy is giving a large `numIterations`, and then tune down the shrinkage parameter(or `learningRate`) to scale the contribution of each weak learner and achieve better result. It comes at the price of increasing training time. Lower learning rate requires more iterations, as rule of thumb for iterations >500, a smaller value of learning rate(<0.1) gives much better generalizetion errors. However in my tests, Spark GBT runs much slower when iterations go up to 200(testing on 400 cpu cores). Below is my test result, clearly an appropriately higher learning rate for small iterations can also lift the scores:  

  Learning Rate  |   AUPRC            | AUROC	
  -------------  |--------------------| -------------
  0.05           |   0.5030750757     | 0.87685504
  0.1            |   0.5094705132     | 0.8802381335
  0.15	         |   0.5095999476	  | 0.8811939415
 
 
**subsampling rate**  
Bagging is often used in Random Forest to reduce the variance, it can also be used in GBT(combined with learning rate). By using fraction of samples to fit the individual base learners, it is said to perform better than deterministic  boosting, that's also the idea of "Stochastic Gradient Boosting". In the original paper(stochastic boosting), it's suggested subsampling rate around 0.4(or less than 1). However I didn't see this improvement in my test, the reason is probably because the number of iterations is not big enough(subsampling rate often interacts with num iterations):

  Subsampling Rate  |   AUPRC            | AUROC	
  ----------------  |--------------------| -------------
  0.4	            |   0.6965167027     | 0.908289203
  0.8               |   0.6972948047     | 0.9086104967
  1.0               |   0.7056956335     | 0.9113023003


**max depth of tree**:  
GBTs generally train shallower trees compared to random forest. They have a smaller variance, and run many iterations to reduce the bias. Similar to RF, I chose the max depth based on number of samples, the more samples the deeper the tree. I used [15,20] for RF, and lowered range for GBT. Below is the testing result based on 10m training samples, clearly deeper trees helped when training large number of data:    
                                    
Max Depth |	 Iterations	  | AUC-PR         |	AUC-ROC
--------- |---------------| -------------  | -------------
10	      |	      100     | 0.7056956335   |  0.9113023003
12	      |	      100     |0.7055160758    |  0.9112645385
12	      |	      200     |0.7124389723    |  0.9139163337
13	      |	      100     |0.7082843826    |  0.9122832517

                                 
**number of iterations**:  
Increasing number of iterations reduces the error on training set, but setting it too high may lead to overfitting. An optimal value of iterations is often selected by monitoring prediction error on an independent validation data set. In GBT, it not only affects the model quality, but also the training time. I hit the the runtime bottleneck whening increasing number of iterations to 200.

Spark has succesfully eliminates a lot of bottlencks, but introduces other new bottlenecks compared to many single-machine implementations including XGBoost. I captured the timelines from Web UI. At the beginning, schedule delay dominates the run time, clearly this is due to network latency and coordination between executors and drivers.   
{% img left /images/timeline1.png %}  

RandomForest:   init: 46.988458552  
  total: 259.506305596  
  findSplits: 22.73275623  
  findBestSplits: 212.461930748  
  chooseSplits: 211.908053401  

After a while, task deserialization time is getting longer. The reason is some big data objects are wrapped in the function closure when broadcasting to other executors. One way to improve this is use more cpu cores(e.g. 10 or 20) in each executor, because communication cost between threads is  much smaller than between executors. 

{% img right /images/timeline2.png %}  

RandomForest:   init: 9.169380495  
  total: 100.28455719  
  findSplits: 2.750761187  
  findBestSplits: 91.051591324  
  chooseSplits: 90.761085741  



