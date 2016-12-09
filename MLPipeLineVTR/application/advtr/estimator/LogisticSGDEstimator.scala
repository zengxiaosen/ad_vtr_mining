package application.advtr.estimator

import java.io.{File, PrintWriter}

import application.advtr.data.MyLabeledPoint
import commons.framework.estimate.SparkEstimate
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.LogisticGradient
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2016/12/9.
  */
class LogisticSGDEstimator (val estimatorName: String = "LogisticSGDEstimator", val isPredict: Boolean = false)
  extends SparkEstimate{

  type SOURCE = MyLabeledPoint
  var originRDD: RDD[SOURCE] = _
  var trainingData:RDD[SOURCE] = _
  var testingData:RDD[SOURCE] = _
  var splitRatio = 0.8
  val regParams = Array(0.0) //,0.01,0.02,0.04,0.08,0.16,0.32,0.64,1.28,2.56,5.12,10.24
  //(reg, model, auc)
  var preMetrics:(Double, Double,Double,Double) = _
  var bestModel: LogisticRegressionModel = _
  var tmpModel: LogisticRegressionModel = _
  var bestTestResultRDD: RDD[String] = _   //RDD[(String,Double,Double)] = _
  val recordCurve = false

  def effect(Tdata:RDD[SOURCE],model:LogisticRegressionModel): Unit ={
    if(Tdata!= null && LogisticRegressionModel!= null){
      val scoreAndLabels = Tdata.map(point => {
        val score = model.predict(point.labeledPoint.features)
        (score, point.labeledPoint.label)
      })
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      val auc = metrics.areaUnderROC()
      val lrgradient = new LogisticGradient()
      // test loss
      val loss = Tdata.map(point =>
        lrgradient.compute(point.labeledPoint.features,point.labeledPoint.label,tmpModel.weights)._2
      ).sum()
      println(s"AUC: ${auc},Loss ${loss}")
    }
  }
  override def doEstimate(): Unit = {
    val iterNum = 800
    val step = 2
    val batchFraction = 0.5
    val reg = 0.001
    val isTest = !true

    if(originRDD != null){
      val splits = originRDD.randomSplit(Array(splitRatio, 1 - splitRatio), seed = 11L)
      //val trainingData = splits(0).map(_.labeledPoint).coalesce(840, true).persist(StorageLevel.MEMORY_ONLY_SER)
      //val testingData = splits(1).coalesce(504, true).persist(StorageLevel.MEMORY_ONLY_SER)
      trainingData = splits(0).persist(StorageLevel.MEMORY_ONLY_SER)
      testingData = splits(1).persist(StorageLevel.MEMORY_ONLY_SER)
      //val l_sgd = new LogisticRegressionWithSGD()
      //val l_lbfs = new LogisticRegressionWithLBFGS().setNumClasses(2) //new LogisticRegressionWithLBFGS() l_bfgs not support L1 regularization
      //trainer
      var auc = 0.0
      var best_metrics_L2:BinaryClassificationMetrics = null
      var best_scoreAndLabels:RDD[(String, String, Double, Double)] = null
      val trainer = new LogisticRegressionWithSGD()

      if(isTest){
        val iterNum = 200
        val step = 2
        val batchFraction = 0.5
        val reg = 0.001
        trainer.optimizer.setNumIterations(iterNum).setStepSize(step).setMiniBatchFraction(batchFraction)
        trainer.optimizer.setRegParam(reg)
        var model = trainer.run(trainingData.map(_.labeledPoint))
        model.clearThreshold() //当清除threshold后，预测输出的结果就不是01分类，而是概率值
        println(s"#############watch effect each ${iterNum} iteration")
        effect(testingData,model)
        for(i <- 1 to 5){
          model = trainer.run(trainingData.map(_.labeledPoint),model.weights)
          model.clearThreshold()
          effect(testingData,model)
        }
        bestModel = model
        println(s"#############watch effect over#####################")
      }
      else{
        println("Final Estimate!")
        trainer.optimizer.setNumIterations(iterNum).setStepSize(step).setMiniBatchFraction(batchFraction)
        trainer.optimizer.setRegParam(reg)
        val model = trainer.run(trainingData.map(_.labeledPoint))

        model.clearThreshold() //当清除threshold后，预测输出的结果就不是01分类，而是概率值


        bestModel = model
      }

      doPredict()
      trainingData.unpersist()
      testingData.unpersist()
    }

  }

  override def doPredict(){
    if(isPredict) testingData = originRDD
    if(testingData != null && bestModel != null){
      bestModel.clearThreshold()

      val scoreAndLabels_L2 = testingData.map(point => {
        val score = bestModel.predict(point.labeledPoint.features)
        (point.posId, point.clientType, score, point.labeledPoint.label)
      })

      val metrics_L2 = new BinaryClassificationMetrics(scoreAndLabels_L2.map(u => (u._3,u._4)))
      val auc = metrics_L2.areaUnderROC()
      //      var fMeasureCurve = originRDD.sparkContext.parallelize(List((0.0,0.0)))
      //      var precisionCurve = originRDD.sparkContext.parallelize(List((0.0,0.0)))
      //      var recallCurve = originRDD.sparkContext.parallelize(List((0.0,0.0)))
      //      var prCurve = originRDD.sparkContext.parallelize(List((0.0,0.0)))
      //      if(recordCurve) {
      //        fMeasureCurve = metrics_L2.fMeasureByThreshold()
      //        precisionCurve = metrics_L2.precisionByThreshold()
      //        recallCurve = metrics_L2.recallByThreshold()
      //        prCurve = metrics_L2.pr()
      //      }

      val mae = analyzeMAE(scoreAndLabels_L2.map(u => (u._3,u._4)))

      val vtr_list = scoreAndLabels_L2.map(u =>u._3).collect
      val label_list = scoreAndLabels_L2.map(u =>u._4).collect
      val avg_vtr = vtr_list.sum/vtr_list.size
      val avg_label = label_list.sum/label_list.size

      bestTestResultRDD =scoreAndLabels_L2.sortBy(_._3).map(u => u._1 + "\t" + u._2 + "\t" + u._3 + "\t" + u._4)
      preMetrics = (auc, mae,avg_vtr,avg_label)
    }
  }

  def createModel(weights: String, intercept: Double){
    val weightsVec = Vectors.dense(weights.split(",").map(_.toDouble))
    bestModel = new LogisticRegressionModel(weightsVec,intercept)
    println(s"ssy:create LogisticRegressionModel succeed!,weights size:${weightsVec.size}")
  }

  def analyzeResult(path:String){

    println("ssy:analyzeResult::::")

    //roc_L2, mae, fMeasureCurve,precisionCurve,recallCurve,prCurve) <- preMetrics
    //write _metrics
    val auc = preMetrics._1
    val mae = preMetrics._2
    val avg_vtr = preMetrics._3
    val avg_label = preMetrics._4
    //    val fMeasureCurve = preMetrics._5
    //    val precisionCurve = preMetrics._6
    //    val recallCurve = preMetrics._7
    //    val prCurve = preMetrics._8
    var writer = new PrintWriter(new File(path+"/metrics.txt"))
    writer.println(s"auc:$auc")
    writer.println(s"mae:$mae")
    writer.println(s"avg_vtr:$avg_vtr")
    writer.println(s"avg_label:$avg_label")
    writer.close()
    //*******write curves***************//
    //write fMeasureCurve
    //    writer = new PrintWriter(new File(path+"/fMeasureCurve.txt"))
    //    fMeasureCurve.collect().foreach(x=>writer.println(x))
    //    writer.close()
    //    //write precisionCurve
    //    writer = new PrintWriter(new File(path+"/precisionCurve.txt"))
    //    precisionCurve.collect().foreach(x=>writer.println(x))
    //    writer.close()
    //    //write recallCurve
    //    writer = new PrintWriter(new File(path+"/recallCurve.txt"))
    //    recallCurve.collect().foreach(x=>writer.println(x))
    //    writer.close()
    //    //write prCurve
    //    writer = new PrintWriter(new File(path+"/prCurve.txt"))
    //    prCurve.collect().foreach(x=>writer.println(x))
    //    writer.close()
  }

  def analyzeMAE(scoreAndLabels: RDD[(Double, Double)]): Double ={
    val sum_e = scoreAndLabels.map(x=>x._1 - x._2 abs).reduce(_+_)
    val mae = sum_e/scoreAndLabels.count()
    mae
  }

  override def analyzeResult(): Unit = ???
}