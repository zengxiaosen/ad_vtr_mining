package application.advtr.sample.handler

/**
 * Created by shishuyuan on 2015/11/14.
 */

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
 * Created by leilongyan on 2015/5/21.
 */
class AdxPIdSubChannelHandler(val featureHandlerName: String = "adxPIdSubChannelHandler") extends FeatureHandler{
  private val adxPIdSubChannelConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_adxPId_subChannel.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var adxPIdSubChannelFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(adxPIdSubChannelConf)
      val linesItr = source.getLines()
      for(line <- linesItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!adxPIdSubChannelFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            adxPIdSubChannelFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "adxPIdSubChannel", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + adxPIdSubChannelFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "adxPIdSubChannel", 1.0)
      for((propertyValue,feature) <- adxPIdSubChannelFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[adxPIdSubChannel]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    val features = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val VTRInstance = dataInstance.asInstanceOf[VTRDataInstance]
      val subChannel = VTRInstance.subChannel
      val adxPId= VTRInstance.adxPId
      val adxPIdSubChannel = adxPId+ "_" + subChannel
      if(adxPIdSubChannelFeatureMap.contains(adxPIdSubChannel)){
        features.+=(adxPIdSubChannelFeatureMap.get(adxPIdSubChannel).get)
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}

