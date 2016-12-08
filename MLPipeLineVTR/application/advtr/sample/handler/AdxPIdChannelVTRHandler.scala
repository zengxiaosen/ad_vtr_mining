package application.advtr.sample.handler

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

//unkown feature index?
class AdxPIdChannelVTRHandler(val featureHandlerName: String = "adxPIdChannelVTRHandler") extends FeatureHandler{
  private val adxPIdChannelVTRFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_adxPId_channelVTR.conf"
  private var maxFeatureIndex: Int = 0
  private val numOfFields:Int = 2
  private var unSeenFeature: Feature = _
  private var adxPIdChannelVTRMap: Map[String,Double] = new HashMap[String,Double]()
  private val discreteDis = VTRSampleConfig.VTRDiscreteDis
  private val discreteLen = VTRSampleConfig.VTRDiscreteLen
  private var initFeatureInd: Int = 0

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(adxPIdChannelVTRFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfFields){
          if(!adxPIdChannelVTRMap.contains(contents(0).trim)){
            //val localIndex = 1
            adxPIdChannelVTRMap.+=(contents(0).trim -> contents(1).trim.toDouble)
          }
        }
      }
    }catch{
      case ex: IOException => ex.printStackTrace()
    }finally {
      initFeatureInd = initFeatureIndex
      maxFeatureIndex = initFeatureIndex + (if(discreteLen <= 1)1 else discreteLen)
      unSeenFeature = Feature(initFeatureInd + 1, "adxPIdChannelVTR", if(discreteLen <= 1)0.0 else 1.0)

      var index = 1
      var featureIndex = initFeatureInd + 1
      while(featureIndex <= maxFeatureIndex){
        System.out.println(s"feature:${featureIndex},[adxPIdChannelVTR]_[$index]")
        featureIndex += 1
        index += 1
      }
      if(source != null) {
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val vtrDataInstance = dataInstance.asInstanceOf[VTRDataInstance]
      val positionId = vtrDataInstance.adxPId
      val Channel = vtrDataInstance.channel
      if(positionId != null && Channel != null) {
        val adxPIdChannel = positionId + "_" + Channel
        if (adxPIdChannelVTRMap.contains(adxPIdChannel)) {
          val vtr = adxPIdChannelVTRMap(adxPIdChannel)
          //discreteLen<=1，那么不进行离散化
          if(discreteLen > 1) {
            var discreteIndex = (vtr / discreteDis).toInt
            discreteIndex = if (discreteIndex >= discreteLen - 1) (discreteLen - 1) else discreteIndex
            features.+=(Feature(initFeatureInd + discreteIndex + 1, "adxPIdChannelVTR", 1.0))
          }else{
            features.+=(Feature(initFeatureInd + 1, "adxPIdChannelVTR", vtr))
          }
        } else {
          features.+=(unSeenFeature)
        }
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}

