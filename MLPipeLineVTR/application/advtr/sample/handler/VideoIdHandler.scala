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
  * Created by Administrator on 2016/12/8.
  */
class VideoIdHandler(val featureHandlerName: String = "VideoIdHandler") extends FeatureHandler{
  private val VideoIdFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_videoId.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var VideoIdFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(VideoIdFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!VideoIdFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            VideoIdFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "VideoId", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + VideoIdFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "VideoId", 1.0)
      for((propertyValue,feature) <- VideoIdFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[VideoId]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val VideoId = (dataInstance.asInstanceOf[VTRDataInstance]).videoId
      if(VideoIdFeaturesMap.contains(VideoId)){
        features.+=(VideoIdFeaturesMap(VideoId))
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}

