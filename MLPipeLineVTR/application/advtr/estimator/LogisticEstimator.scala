package application.advtr.estimator

import commons.framework.estimate.SparkEstimate
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2016/12/9.
  */
class LogisticEstimator(val estimatorName: String = "LogisticEstimator",  val isPredict: Boolean = false) extends SparkEstimate{

  type SOURCE = LabeledPoint
  var originRDD: RDD[SOURCE] = _
  var splitRatio = 0.7
  val regParams = Array(0.0) //,0.01,0.02,0.04,0.08,0.16,0.32,0.64,1.28,2.56,5.12,10.24
  val thresholds = Array(0.001,0.006,0.011,0.016,0.021,0.026,0.031,0.036,0.041,0.046,0.051,0.056,0.061,0.1)
  //(reg, model, auc)
  val resultData = new Array[(Double, LogisticRegressionModel, Double)](regParams.size)
  var bestModel: LogisticRegressionModel = _
  var bestTestResultRDD: RDD[(Double,Double)] = _

  override def doEstimate(): Unit = {
    if(originRDD != null){
      val splits = originRDD.randomSplit(Array(splitRatio, 1 - splitRatio), seed = 11L)
      val trainingData = splits(0).coalesce(1680,true).persist(StorageLevel.MEMORY_ONLY_SER)
      val testingData = splits(1).coalesce(504,true).persist(StorageLevel.MEMORY_ONLY_SER)
      //val l_sgd = new LogisticRegressionWithSGD()
      //val l_lbfs = new LogisticRegressionWithLBFGS().setNumClasses(2) //new LogisticRegressionWithLBFGS() l_bfgs not support L1 regularization
      var index = 0
      var maxROC = Double.MinValue
      for(reg <- regParams){
        //L2 regularization
        val l_lbfs = new LogisticRegressionWithLBFGS().setNumClasses(2) //new LogisticRegressionWithLBFGS() l_bfgs not support L1 regularization
        l_lbfs.optimizer.setRegParam(reg).setUpdater(new SquaredL2Updater).setNumIterations(30).setNumCorrections(5)
        val model_L2 = l_lbfs.run(trainingData)

        model_L2.clearThreshold() //当清除threshold后，预测输出的结果就不是01分类，而是概率值
        val scoreAndLabels_L2 = testingData.map(point => {
          val score = model_L2.predict(point.features)
          (score,point.label)
        })
        val metrics_L2 = new BinaryClassificationMetrics(scoreAndLabels_L2)
        val roc_L2 = metrics_L2.areaUnderROC()

        if(roc_L2 > maxROC){
          bestModel = model_L2
          maxROC = roc_L2
          bestTestResultRDD = scoreAndLabels_L2.sortBy(_._1)
        }

        resultData(index) = (reg, model_L2, roc_L2)
        index += 1
      }
      trainingData.unpersist()
      testingData.unpersist()
    }
  }

  override def analyzeResult(){
    var maxroc = Double.MinValue
    println(s"*******************bestTestResultRDD analysis*******************")
    if(bestTestResultRDD != null) {
      bestTestResultRDD.persist(StorageLevel.MEMORY_ONLY_SER)
      for (threshold <- thresholds) {
        val threshResult = bestTestResultRDD.map(result =>{
          if(result._1 >= threshold)(1.0, result._2)else(0.0,  result._2)
        })
        val tp = threshResult.filter(r => r._1 == 1.0 && r._1 == r._2).count()
        val fp = threshResult.filter(r => r._1 == 1.0 && r._1 != r._2).count()
        val tn = threshResult.filter(r => r._1 == 0.0 && r._1 == r._2).count()
        val fn = threshResult.filter(r => r._1 == 0.0 && r._1 != r._2).count()
        val tp_fp = (tp*1.0)/(tp+fp)
        val tn_fn = (tn*1.0)/(tn+fn)
        println(s"threshold=$threshold,tp=$tp ,fp=$fp ,tn=$tn ,fn=$fn, tp/(tp+fp)=$tp_fp, tn/(tn+fn)=$tn_fn")
      }
    }
    println(s"*******************$estimatorName*******************")
    for((reg, model_L2, roc_L2) <- resultData){
      println(s"$estimatorName,regularization param:$reg ,[L2 roc:$roc_L2]")
    }
  }
}
