package commons.framework.sample

import commons.framework.data.DataInstance

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/12/8.
  */
trait FeatureHandler extends Serializable{
  val featureHandlerName: String
  def initFeatureHandler(initFeatureIndex: Int): Int
  def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature]
}
