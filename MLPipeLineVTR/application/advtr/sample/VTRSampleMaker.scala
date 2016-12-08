package application.advtr.sample

import commons.framework.sample.{FeatureHandler, SampleMaker}

/**
  * Created by Administrator on 2016/12/8.
  */
class VTRSampleMaker private extends SampleMaker{
  def registerFeatureHandler(featureHandler: FeatureHandler, featureIndex: Int): Int = {
    val rtnFeatureIndex = featureHandler.initFeatureHandler(featureIndex)
    super.registerFeatureHandler(featureHandler)
    rtnFeatureIndex
  }

}

object VTRSampleMaker{
  val VTRSampleMaker = new VTRSampleMaker
  def instance = VTRSampleMaker
}
