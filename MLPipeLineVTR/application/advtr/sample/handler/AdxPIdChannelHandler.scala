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
class AdxPIdChannelHandler(val featureHandlerName: String = "adxPIdChannelHandler") extends FeatureHandler{
  private val adxPIdChannelConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_adxPId_channel.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var adxPIdChannelFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(adxPIdChannelConf)
      val linesItr = source.getLines()
      for(line <- linesItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!adxPIdChannelFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            adxPIdChannelFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "adxPIdChannel", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + adxPIdChannelFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "adxPIdChannel", 1.0)
      for((propertyValue,feature) <- adxPIdChannelFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[adxPIdChannel]_[none]")
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
      val Channel = VTRInstance.channel
      val adxPId= VTRInstance.adxPId
      val adxPIdChannel = adxPId+ "_" + Channel
      if(adxPIdChannelFeatureMap.contains(adxPIdChannel)){
        features.+=(adxPIdChannelFeatureMap.get(adxPIdChannel).get)
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}

