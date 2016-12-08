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
class SubChannelAdSubIndustryHandler(val featureHandlerName: String = "subChannelAdSubIndustryHandler") extends FeatureHandler{
  private val subChannelAdSubIndustryConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_subChannel_adSubIndustry.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var subChannelAdSubIndustryFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(subChannelAdSubIndustryConf)
      val linesItr = source.getLines()
      for(line <- linesItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!subChannelAdSubIndustryFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            subChannelAdSubIndustryFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "subChannelAdSubIndustry", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + subChannelAdSubIndustryFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "subChannelAdSubIndustry", 1.0)
      for((propertyValue,feature) <- subChannelAdSubIndustryFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[subChannelAdSubIndustry]_[none]")
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
      val adSubIndustry = VTRInstance.adSubIndustry
      val subChannelAdSubIndustry = subChannel + "_" + adSubIndustry
      if(subChannelAdSubIndustryFeatureMap.contains(subChannelAdSubIndustry)){
        features.+=(subChannelAdSubIndustryFeatureMap.get(subChannelAdSubIndustry).get)
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}

