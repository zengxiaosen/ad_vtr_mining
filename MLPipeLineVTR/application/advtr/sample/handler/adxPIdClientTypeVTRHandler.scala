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
class AdxPIdClientTypeVTRHandler(val featureHandlerName: String = "adxPIdClientTypeVTRHandler") extends FeatureHandler{
  private val adxPIdClientTypeVTRFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_adxPId_clientTypeVTR.conf"
  private var maxFeatureIndex: Int = 0
  private val numOfFields:Int = 2
  private var unSeenFeature: Feature = _
  private var adxPIdClientTypeVTRMap: Map[String,Double] = new HashMap[String,Double]()
  private val discreteDis = VTRSampleConfig.VTRDiscreteDis
  private val discreteLen = VTRSampleConfig.VTRDiscreteLen
  private var initFeatureInd: Int = 0

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(adxPIdClientTypeVTRFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfFields){
          if(!adxPIdClientTypeVTRMap.contains(contents(0).trim)){
            //val localIndex = 1
            adxPIdClientTypeVTRMap.+=(contents(0).trim -> contents(1).trim.toDouble)
          }
        }
      }
    }catch{
      case ex: IOException => ex.printStackTrace()
    }finally {
      initFeatureInd = initFeatureIndex
      maxFeatureIndex = initFeatureIndex + (if(discreteLen <= 1)1 else discreteLen)
      unSeenFeature = Feature(initFeatureInd + 1, "adxPIdClientTypeVTR", if(discreteLen <= 1)0.0 else 1.0)

      var index = 1
      var featureIndex = initFeatureInd + 1
      while(featureIndex <= maxFeatureIndex){
        System.out.println(s"feature:${featureIndex},[adxPIdClientTypeVTR]_[$index]")
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
      val clientType = vtrDataInstance.clientType
      if(positionId != null && clientType != null) {
        val adxPIdClientType = positionId + "_" + clientType
        if (adxPIdClientTypeVTRMap.contains(adxPIdClientType)) {
          val vtr = adxPIdClientTypeVTRMap(adxPIdClientType)
          //discreteLen<=1，那么不进行离散化
          if(discreteLen > 1) {
            var discreteIndex = (vtr / discreteDis).toInt
            discreteIndex = if (discreteIndex >= discreteLen - 1) (discreteLen - 1) else discreteIndex
            features.+=(Feature(initFeatureInd + discreteIndex + 1, "adxPIdClientTypeVTR", 1.0))
          }else{
            features.+=(Feature(initFeatureInd + 1, "adxPIdClientTypeVTR", vtr))
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

