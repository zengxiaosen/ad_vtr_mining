package application.advtr.sample.handler

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
  * Created by Administrator on 2016/12/8.
  * 用户频道偏好特征，偏好频道是包含频道和子频道的
  */
class UserChannelPrefHandler(val featureHandlerName: String = "UserChannelPrefHandler") extends FeatureHandler{
  private val userChannelPrefFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_channel.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var userChannelPrefFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(userChannelPrefFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!userChannelPrefFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            userChannelPrefFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "userChannelPref", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + userChannelPrefFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "userChannelPref", 1.0)
      for((propertyValue,feature) <- userChannelPrefFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[userChannelPref]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      var channelSet: Set[String] = new HashSet[String]()
      val channelPref = dataInstance.asInstanceOf[VTRDataInstance].userChannels
      if(channelPref != null && channelPref.length != 0){
        val channelPrefs = channelPref.split(",")
        for(pref <- channelPrefs){
          channelSet.+=(pref)
        }
      }

      var isExist = false
      for(c <- channelSet){
        val isHave = userChannelPrefFeaturesMap.contains(c)
        if(isHave){
          features.+=(userChannelPrefFeaturesMap(c))
          isExist = true
        }
      }
      if(!isExist){
        features.+=(unSeenFeature)
      }
    }
    features
  }
}

