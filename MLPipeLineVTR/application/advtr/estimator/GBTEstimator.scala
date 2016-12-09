package application.advtr.estimator

import commons.framework.estimate.SparkEstimate
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2016/12/9.
  */
class GBTEstimator (val estimator: String = "GBTEstimator", val isPredict: Boolean = false) extends SparkEstimate{

  override type SOURCE = LabeledPoint
  override var originRDD: RDD[SOURCE] = _
  var splitRatio = 0.7
  var bestTestResultRDD: RDD[(Double, Double)] = _

  override def doEstimate(): Unit = {
    if(originRDD != null){
      val splits = originRDD.randomSplit(Array(splitRatio, 1 - splitRatio), seed = 11L)
      val trainingData = splits(0).coalesce(840, true).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val testingData = splits(1).coalesce(504, true).persist(StorageLevel.MEMORY_ONLY_SER)

      val boostingStrategy = BoostingStrategy.defaultParams("Classification")
      boostingStrategy.setNumIterations(10)
      boostingStrategy.treeStrategy.setMaxDepth(5)
      boostingStrategy.setLearningRate(0.01)
      boostingStrategy.treeStrategy.setMinInfoGain(0.05)
      boostingStrategy.treeStrategy.setNumClasses(2)
      NaiveBayes.train(trainingData, lambda = 1.0)

      val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

      //Evaluate model on test instance and compute test error
      bestTestResultRDD = testingData.map({point =>
      val prediction = model.predict(point.features)
        (point.label, prediction)
      }).sortBy(_._2)

      val tp = bestTestResultRDD.filter(r => r._2 == 1.0 && r._1 == r._2).count()
      val fp = bestTestResultRDD.filter(r => r._2 == 1.0 && r._1 != r._2).count()
      val total = testingData.count()
      val testErr = bestTestResultRDD.filter(r => r._1 != r._2).count.toDouble / total
      println("total = " + total + ", Test Error = " + testErr + ", tp = " + tp + ",fp = " + fp)

    }
  }

}
