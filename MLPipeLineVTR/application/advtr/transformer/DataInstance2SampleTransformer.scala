package application.advtr.transformer

import application.advtr.data.VTRDataInstance
import commons.framework.data.DataInstance
import commons.framework.sample.{Sample, SampleMaker}
import commons.framework.transform.SparkTransform
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by Administrator on 2016/12/9.
  */
class DataInstance2SampleTransformer(val transformer: String = "DataInstance2SampleTransformer") extends SparkTransform {

  def this(sampleMaker: SampleMaker) {
    this()
    this.transformParams = sampleMaker
  }

  type IN = DataInstance
  type OUT = Sample

  override implicit def tag: ClassManifest[Sample] = DataInstance2SampleTransformer.tagSample

  override var inputRDD: RDD[DataInstance] = _
  override var outputRDD: RDD[Sample] = _
  override protected var transformParams: Any = _ //sampleMaker
  override def createTransformFunc(params: Any): (DataInstance) => Sample = {
    def transform(dataInstance: DataInstance): Sample = {
      val sample = new Sample(1, Double.MinValue, "-1", "-1")//20150907 zengxiaosen修改
      if(params != null && params.isInstanceOf[SampleMaker] && dataInstance.isInstanceOf[VTRDataInstance]){
        val sampleMaker = params.asInstanceOf[SampleMaker]
        val VTRDataInstance = dataInstance.asInstanceOf[VTRDataInstance]
        sample.targetValue = VTRDataInstance.targetValue
        sample.posId = VTRDataInstance.adxPId//ssy 20151114
        sample.clientType = VTRDataInstance.clientType//ssy 20151117
        sampleMaker.fillSample(sample, dataInstance)
      }
      sample
    }
    transform
  }

  override def dataFilter(inputObj: Sample): Boolean = {
    inputObj.targetValue != Double.MinValue
  }

}

object DataInstance2SampleTransformer{
  val tagSample = reflect.classTag[Sample]
}