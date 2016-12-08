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
class VideoIdIdeaIdHandler(val featureHandlerName: String = "videoIdIdeaIdHandler") extends FeatureHandler{
  private val videoIdIdeaIdConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_videoId_ideaId.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var videoIdIdeaIdFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(videoIdIdeaIdConf)
      val linesItr = source.getLines()
      for(line <- linesItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!videoIdIdeaIdFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            videoIdIdeaIdFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "videoIdIdeaId", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + videoIdIdeaIdFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "videoIdIdeaId", 1.0)
      for((propertyValue,feature) <- videoIdIdeaIdFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[videoIdIdeaId]_[none]")
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
      val ideaId = VTRInstance.ideaId
      val videoId= VTRInstance.videoId
      val videoIdIdeaId = videoId+ "_" + ideaId
      if(videoIdIdeaIdFeatureMap.contains(videoIdIdeaId)){
        features.+=(videoIdIdeaIdFeatureMap.get(videoIdIdeaId).get)
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}


