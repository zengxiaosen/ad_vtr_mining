package application.advtr.transformer

import application.advtr.sample.VTRSampleMaker
import application.advtr.sample.handler.ChannelHandler
import commons.framework.transform.SparkTransform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/12/9.
  */
class TransformControler {

}

//test object
object TransformControler{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("ClosureExper")
    val sc = new SparkContext(conf)
    val training = sc.parallelize(Array("b\t0","a\t1.0","d\t1.0", "v\t1.0", "o\t0"))

    var featureIndex = 0
    val sampleMaker = VTRSampleMaker.instance
    featureIndex = sampleMaker.registerFeatureHandler(new ChannelHandler, featureIndex)

    val dataInstanceTransformer = new PlainText2DataInstanceTransformer()
    dataInstanceTransformer.inputRDD = training
    val sampleTransformer = new DataInstance2SampleTransformer(sampleMaker)

    val transformers = Array[SparkTransform](dataInstanceTransformer, sampleTransformer)
    var index = 0
    while(index < transformers.size){
      val t = transformers(index)
      if(t.inputRDD == null && index != 0){
        t.inputRDD = transformers(index - 1).outputRDD.asInstanceOf[RDD[t.IN]]
      }
      t.rddTransform()
      index += 1
    }
    transformers(index - 1).outputRDD.collect().foreach(println)
  }
}
