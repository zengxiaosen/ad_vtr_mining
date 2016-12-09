package application.advtr.pipeline

import commons.framework.estimate.SparkEstimate
import commons.framework.transform.SparkTransform
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2016/12/9.
  */
class PipelineStageExecutor (val transformers: Array[SparkTransform], val estimators: Array[SparkEstimate] = null){
  var index = 0
  def execTransformers(){
    // exec transformer, serial exec
    if(transformers != null){
      index = 0
      while(index < transformers.size){
        val t = transformers(index)
        System.out.println(s"${t.transformName} is starting transform!")
        if(t.inputRDD == null && index != 0){
          t.inputRDD = transformers(index - 1).outputRDD.asInstanceOf[RDD[t.IN]]
        }
        print(s"${t.transformName} transform begin !")
        println("ssy: Transformer:", index)
        println("ssy: inputRDD.name: ", t.inputRDD.name)
        println("ssy: inputRDD count: ", t.inputRDD.count())
        t.rddTransform()
        println("ssy: outRDD.name: ", t.outputRDD.name)
        println("ssy: outRDD.count: ", t.outputRDD.count())
        println(s"${t.transformName} transform end!\n")
        index += 1
      }
    }
  }

  def execEstimators(){
    //exec estimator
    if(estimators != null){
      if(transformers == null || index == 0){
        if(estimators != null){
          for(estimator <- estimators){
            if(estimator.isPredict){
              estimator.doPredict()
            }else{
              estimator.doEstimate()
            }
          }
        }
      }else if(index == transformers.size && estimators != null){
        val lastTransformer = transformers(index - 1)
        for(estimator <- estimators){
          estimator.originRDD = lastTransformer.outputRDD.asInstanceOf[RDD[estimator.SOURCE]]
          if(estimator.isPredict){
            estimator.doPredict()
          }else{
            estimator.doEstimate()
          }
        }
      }
    }
  }

  //one step from start to end
  def execStages(){
    //exec transformer, serial exec
    execTransformers()
    //exec estimator
    execEstimators()
    //unpersist rdd
    unCacheRDD()
  }

  def unCacheRDD(){
    //unpersist rdd
    if(transformers != null) {
      for (transformer <- transformers) {
        transformer.unCache()
      }
    }
  }
}
