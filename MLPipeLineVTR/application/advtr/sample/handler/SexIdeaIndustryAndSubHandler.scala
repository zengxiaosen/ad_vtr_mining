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
 * Created by leilongyan on 2015/5/25.
 * 性别与广告(素材)行业的组合特征
 */
class SexIdeaIndustryAndSubHandler (val featureHandlerName: String = "SexIdeaIndustryAndSubHandler") extends FeatureHandler{
  private val sexIdeaIndustryAndSubFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_sexIdeaIndustryAndSub.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var sexIdeaIndustryAndSubFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(sexIdeaIndustryAndSubFeatureConf, "utf-8")
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!sexIdeaIndustryAndSubFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            sexIdeaIndustryAndSubFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "sexIdeaIndustryAndSub", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + sexIdeaIndustryAndSubFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "sexIdeaIndustryAndSub", 1.0)
      for((propertyValue,feature) <- sexIdeaIndustryAndSubFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[sexIdeaIndustryAndSub]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    var isExist = false;
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val vtrInstance = dataInstance.asInstanceOf[VTRDataInstance]
      val sex = vtrInstance.userSex

      var ideaCatIndAndSub: Set[String] = new HashSet[String]()
      ideaCatIndAndSub += vtrInstance.adCategory //品类
      ideaCatIndAndSub += vtrInstance.adIndustry //行业
      ideaCatIndAndSub += vtrInstance.adSubIndustry //子行业 去重

      if(sex != null && sex.length > 0 && ideaCatIndAndSub.size > 0){
        for(industry <- ideaCatIndAndSub){
          if(industry != null && industry.length > 0){
            val sexIndustry = sex + "_" + industry
            if(sexIdeaIndustryAndSubFeaturesMap.contains(sexIndustry)){
              features.+=(sexIdeaIndustryAndSubFeaturesMap.get(sexIndustry).get)
              isExist = true
            }
          }
        }
      }
    }
    if(!isExist){
      features.+=(unSeenFeature)
    }
    features
  }
}
