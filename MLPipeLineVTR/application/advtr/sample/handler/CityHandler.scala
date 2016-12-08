package application.advtr.sample.handler

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

class CityHandler(val featureHandlerName: String = "CityHandler") extends FeatureHandler{
  private val CityFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_cityId.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var CityFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(CityFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!CityFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            CityFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "City", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + CityFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "City", 1.0)
      for((propertyValue,feature) <- CityFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[City]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val City = (dataInstance.asInstanceOf[VTRDataInstance]).cityId
      if(CityFeaturesMap.contains(City)){
        features.+=(CityFeaturesMap(City))
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
