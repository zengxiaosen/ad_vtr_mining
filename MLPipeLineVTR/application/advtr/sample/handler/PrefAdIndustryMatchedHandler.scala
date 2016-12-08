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
 * DMP里用户的广告偏好是否匹配广告的行业
 */
class PrefAdIndustryMatchedHandler(val featureHandlerName: String = "PrefAdIndustryMatchedHandler") extends FeatureHandler{
  private val prefAdMatchedFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_prefAdIndustryMatched.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var prefAdMatchedFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(prefAdMatchedFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!prefAdMatchedFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            prefAdMatchedFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "prefAdIndustryMatched", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + prefAdMatchedFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "prefAdIndustryMatched", 1.0)
      for((propertyValue,feature) <- prefAdMatchedFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[prefAdIndustryMatched]_[none]")

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
      val category = vtrInstance.adCategory
      val industry = vtrInstance.adIndustry
      val subIndustry = vtrInstance.adSubIndustry
      val adsPref = vtrInstance.userAdIndustrys
      if(adsPref != null && adsPref.length > 0){
        if(category != null && adsPref.contains(category)){
          count += 1
        }
        if(industry != null && adsPref.contains(industry)){
          count += 1
        }
        if(subIndustry != null && adsPref.contains(subIndustry)){
          count += 1
        }
      }
    }
    if(prefAdMatchedFeaturesMap.contains(count.toString)){
      features.+=(prefAdMatchedFeaturesMap.get(count.toString).get)
    }else{
      features.+=(unSeenFeature)
    }
    features
  }
}
