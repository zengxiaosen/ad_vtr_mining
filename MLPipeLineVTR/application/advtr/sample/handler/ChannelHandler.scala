package application.advtr.sample.handler

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
 * 一级频道
 * Created by leilongyan on 2015/4/1.
 */
class ChannelHandler(val featureHandlerName: String = "ChannelHandler") extends FeatureHandler{
  private val channelFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_channel.conf"
  private var maxFeatureIndex: Int = 0
  private val numOfFields:Int = 2
  private var unSeenFeature: Feature = _
  private var channelFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(channelFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfFields){
          if(!channelFeatureMap.contains(contents(0).trim)){
            val localIndex: Int = contents(1).trim.toInt
            channelFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "channel", 1.0))
          }
        }
      }
    }catch{
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + channelFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "channel", 1.0)
      for((propertyValue,feature) <- channelFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[channel]_[none]")
      if(source != null) {
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val channel = (dataInstance.asInstanceOf[VTRDataInstance]).channel
      if(channelFeatureMap.contains(channel)){
        features.+=(channelFeatureMap(channel))
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
