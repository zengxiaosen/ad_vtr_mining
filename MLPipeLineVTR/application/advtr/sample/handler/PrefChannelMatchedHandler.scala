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
 * Created by leilongyan on 2015/4/28.
 * DMP里用户频道偏好是否匹配广告所在频道
 */
class PrefChannelMatchedHandler(val featureHandlerName: String = "PrefChannelMatchedHandler") extends FeatureHandler{
  private val prefChannelMatchedFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_prefChannelMatched.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var prefChannelMatchedFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(prefChannelMatchedFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!prefChannelMatchedFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            prefChannelMatchedFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "prefChannelMatched", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + prefChannelMatchedFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "prefChannelMatched", 1.0)
      for((propertyValue,feature) <- prefChannelMatchedFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[prefChannelMatched]_[none]")

      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    val features = new ArrayBuffer[Feature](1)
    var count = 0
    if (dataInstance.isInstanceOf[VTRDataInstance]) {
      val vtrInstance = dataInstance.asInstanceOf[VTRDataInstance]
      val channel = vtrInstance.channel
      val subChannel = vtrInstance.subChannel
      val channelsPref = vtrInstance.userChannels
      if(channelsPref != null && channelsPref.length > 0){
        if(channel != null && channelsPref.contains(channel)){
          count += 1
        }
        if(subChannel != null && channelsPref.contains(subChannel)){
          count += 1
        }
      }
    }
    if(prefChannelMatchedFeaturesMap.contains(count.toString)){
      features.+=(prefChannelMatchedFeaturesMap.get(count.toString).get)
    }else{
      features.+=(unSeenFeature)
    }
    features
  }
}
