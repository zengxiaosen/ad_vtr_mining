package application.advtr.transformer

import application.advtr.data.MyLabeledPoint
import commons.framework.sample.Sample
import commons.framework.transform.SparkTransform
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by Administrator on 2016/12/9.
  */
class Sample2LabeledPointTransformer (val transformName: String = "Sample2LabeledPointTransformer") extends SparkTransform{
   def this(featureSize: Int){
     this()
     transformParams = featureSize
   }
  type IN = Sample
  type OUT = MyLabeledPoint
  override implicit def tag: ClassTag[OUT] = Sample2LabeledPointTransformer.tagLabeledPoint
  override var inputRDD: RDD[IN] = _
  override var outputRDD: RDD[OUT] = _
  override protected var transformParams: Any = _
  override def createTransformFunc(params: Any): (Sample) => MyLabeledPoint = {
    def transform(sample: Sample): MyLabeledPoint = {
      if(params.isInstanceOf[Int]){
        val featureSize = params.asInstanceOf[Int]
        MyLabeledPoint(sample.posId, sample.clientType, LabeledPoint(sample.targetValue, Vectors.sparse(featureSize,sample.getFeaturesTupleSeq())))
      }else{
        null
      }
    }
    transform
  }

  override def dataFilter(inputObj: MyLabeledPoint): Boolean = {
    inputObj match {
      case lp: MyLabeledPoint => true
      case _=> false
    }
  }
}

object Sample2LabeledPointTransformer{
  val tagLabeledPoint = reflect.classTag[MyLabeledPoint]
}