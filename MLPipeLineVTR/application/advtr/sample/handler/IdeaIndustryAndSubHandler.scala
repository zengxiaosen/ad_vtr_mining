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
 * Created by shishuyuan on 2015/12/15.
 */
class IdeaIndustryAndSubHandler(val featureHandlerName: String = "IdeaIndustryAndSubHandler") extends FeatureHandler{
  private val ideaIndustryAndSubFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_ideaIndustryAndSub.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var ideaIndustryAndSubFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(ideaIndustryAndSubFeatureConf, "utf-8")
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\t")
        if(contents.length == numOfField){
          if(!ideaIndustryAndSubFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            ideaIndustryAndSubFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "ideaIndustryAndSub", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + ideaIndustryAndSubFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "ideaIndustryAndSub", 1.0)
      for((propertyValue,feature) <- ideaIndustryAndSubFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[ideaIndustryAndSub]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    var isExist = false
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val VTRInstance = dataInstance.asInstanceOf[VTRDataInstance]
      var ideaCatIndAndSub: Set[String] = new HashSet[String]()
      ideaCatIndAndSub += VTRInstance.adCategory //品类
      ideaCatIndAndSub += VTRInstance.adIndustry //行业
      ideaCatIndAndSub += VTRInstance.adSubIndustry //子行业 去重

      for(c <- ideaCatIndAndSub){
        if(ideaIndustryAndSubFeaturesMap.contains(c)){
          features += ideaIndustryAndSubFeaturesMap(c)
          isExist = true
        }
      }
    }
    if(!isExist){
      features += unSeenFeature
    }
    features
  }
}

