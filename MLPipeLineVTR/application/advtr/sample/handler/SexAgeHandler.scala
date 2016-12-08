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
 * Created by leilongyan on 2015/5/25.
 * 性别与广告(素材)行业的组合特征
 */
class SexAgeHandler (val featureHandlerName: String = "SexAgeHandler") extends FeatureHandler{
  private val sexAgeFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_userSex_userAge.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var sexAgeFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(sexAgeFeatureConf, "utf-8")
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!sexAgeFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            sexAgeFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "sexAge", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + sexAgeFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "sexAge", 1.0)
      for((propertyValue,feature) <- sexAgeFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[sexAge]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]) {
      val vtrInstance = dataInstance.asInstanceOf[VTRDataInstance]
      val sex = vtrInstance.userSex
      val userAge = vtrInstance.userAge //子行业 去重
      val sexage = sex + "_" + userAge
      if (sexAgeFeaturesMap.contains(sexage)) {
        features.+=(sexAgeFeaturesMap.get(sexage).get)
      }
      else {
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
