package application.advtr.sample.handler

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
  * Created by Administrator on 2016/12/8.
  * 用户广告行业偏好特征，行业偏好是用idea的行业子行业品类表示的
  * 看匹配多少个
  */
class UserAdPrefHandler(val featureHandlerName: String = "UserAdPrefHandler") extends FeatureHandler{
  private val userAdPrefFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_userAdIndustrys.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var userAdPrefFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(userAdPrefFeatureConf, "utf-8")
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!userAdPrefFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            userAdPrefFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "userAdPref", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + userAdPrefFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "userAdPref", 1.0)
      for((propertyValue,feature) <- userAdPrefFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[userAdPref]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    var count = 0
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      var adPrefSet: Set[String] = new HashSet[String]()
      val adsPref = dataInstance.asInstanceOf[VTRDataInstance].userAdIndustrys
      if(adsPref != null && adsPref.length != 0){
        val adsPrefs = adsPref.split(",")
        for(pref <- adsPrefs){
          adPrefSet.+=(pref)
        }
      }

      var isExist = false
      for(a <- adPrefSet){
        val isHave = userAdPrefFeaturesMap.contains(a)
        if(isHave){
          features.+=(userAdPrefFeaturesMap(a))
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
