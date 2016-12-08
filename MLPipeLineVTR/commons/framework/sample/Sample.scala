package commons.framework.sample

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/12/8.
  */
class Sample (val sampleId: Long, var targetValue: Double, var posId: String, var clientType: String, var predictValue : Double = Double.MinValue){
  var featureArrayList : ArrayBuffer[Feature] = new ArrayBuffer[Feature](1024)
  def addFeatures(feature: ArrayBuffer[Feature]){
    featureArrayList.++=(feature)
  }

  def getFeaturesTupleSeq(): Seq[(Int, Double)] = {
    //featureIndex需要-1，因为sparse的数组是从0开始
    val featuresArray = featureArrayList.map(feature => (feature.featureIndex - 1, feature.featureValue))
    featuresArray.toSeq
  }

  override def toString():String = {
    val sortedFeatures = featureArrayList.map(feature => (feature.featureIndex, feature)).sortBy(_._1).map(_._2)
    val result = targetValue + " " + sortedFeatures.mkString(" ")
    result
  }

}
