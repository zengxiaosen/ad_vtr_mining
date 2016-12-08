package application.advtr.sample.handler

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

class IdeaHandler(val featureHandlerName: String = "IdeaHandler") extends FeatureHandler{
  private val IdeaFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_ideaId.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var IdeaFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(IdeaFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!IdeaFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            IdeaFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "Idea", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + IdeaFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "Idea", 1.0)
      for((propertyValue,feature) <- IdeaFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[Idea]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val Idea = (dataInstance.asInstanceOf[VTRDataInstance]).ideaId
      if(IdeaFeaturesMap.contains(Idea)){
        features.+=(IdeaFeaturesMap(Idea))
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
